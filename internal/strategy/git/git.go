// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func init() {
	strategy.Register("git", New)
}

// Config for the Git strategy.
type Config struct {
	MirrorRoot       string        `hcl:"mirror-root" help:"Directory to store git mirrors." required:""`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
	BundleInterval   time.Duration `hcl:"bundle-interval,optional" help:"How often to generate bundles. 0 disables bundling." default:"0"`
	CloneDepth       int           `hcl:"clone-depth,optional" help:"Depth for shallow clones. 0 means full clone." default:"0"`
}

// cloneState represents the current state of a bare clone.
type cloneState int

const (
	stateEmpty   cloneState = iota // Clone doesn't exist yet
	stateCloning                   // Clone is in progress
	stateReady                     // Clone is ready to serve
)

// clone represents a bare clone of an upstream repository.
type clone struct {
	mu            sync.RWMutex
	state         cloneState
	path          string
	upstreamURL   string
	lastFetch     time.Time
	lastRefCheck  time.Time
	refCheckValid bool
	fetchSem      chan struct{} // Semaphore to coordinate fetch operations
}

// Strategy implements a protocol-aware Git caching proxy.
type Strategy struct {
	config     Config
	cache      cache.Cache
	clones     map[string]*clone
	clonesMu   sync.RWMutex
	httpClient *http.Client
	proxy      *httputil.ReverseProxy
	ctx        context.Context // Strategy lifecycle context
}

// New creates a new Git caching strategy.
func New(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
	logger := logging.FromContext(ctx)

	if config.MirrorRoot == "" {
		return nil, errors.New("mirror-root is required")
	}

	if config.FetchInterval == 0 {
		config.FetchInterval = 15 * time.Minute
	}

	if config.RefCheckInterval == 0 {
		config.RefCheckInterval = 10 * time.Second
	}

	if err := os.MkdirAll(config.MirrorRoot, 0o750); err != nil {
		return nil, errors.Wrap(err, "create mirror root directory")
	}

	s := &Strategy{
		config:     config,
		cache:      cache,
		clones:     make(map[string]*clone),
		httpClient: http.DefaultClient,
		ctx:        ctx,
	}

	// Scan for existing clones on disk and start bundle loops for them
	if err := s.discoverExistingClones(ctx); err != nil {
		logger.WarnContext(ctx, "Failed to discover existing clones",
			slog.String("error", err.Error()))
	}

	s.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = req.PathValue("host")
			req.URL.Path = "/" + req.PathValue("path")
			req.Host = req.URL.Host
		},
		Transport: s.httpClient.Transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logging.FromContext(r.Context()).ErrorContext(r.Context(), "Upstream request failed", slog.String("error", err.Error()))
			w.WriteHeader(http.StatusBadGateway)
		},
	}

	mux.Handle("GET /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))
	mux.Handle("POST /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))

	logger.InfoContext(ctx, "Git strategy initialized",
		"mirror_root", config.MirrorRoot,
		"fetch_interval", config.FetchInterval,
		"ref_check_interval", config.RefCheckInterval,
		"bundle_interval", config.BundleInterval)

	return s, nil
}

var _ strategy.Strategy = (*Strategy)(nil)

func (s *Strategy) String() string { return "git" }

// handleRequest routes Git HTTP requests based on operation type.
func (s *Strategy) handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	logger.DebugContext(ctx, "Git request",
		slog.String("method", r.Method),
		slog.String("host", host),
		slog.String("path", pathValue))

	// Check if this is a bundle request
	if strings.HasSuffix(pathValue, "/bundle") {
		s.handleBundleRequest(w, r, host, pathValue)
		return
	}

	// Determine the service type from query param or path
	service := r.URL.Query().Get("service")
	isReceivePack := service == "git-receive-pack" || strings.HasSuffix(pathValue, "/git-receive-pack")

	// Write operations always forward to upstream
	if isReceivePack {
		logger.DebugContext(ctx, "Forwarding write operation to upstream")
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	// Read operations: serve from local clone if ready, otherwise forward
	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath

	c := s.getOrCreateClone(ctx, upstreamURL)

	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()

	// Check if this is an info/refs request (ref discovery)
	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case stateReady:
		// For info/refs requests, ensure we have the latest refs from upstream
		if isInfoRefs {
			if err := s.ensureRefsUpToDate(ctx, c); err != nil {
				logger.WarnContext(ctx, "Failed to ensure refs up to date",
					slog.String("error", err.Error()))
				// Continue serving even if ref check fails
			}
		}
		// Also do background fetch if interval has passed
		s.maybeBackgroundFetch(ctx, c)
		s.serveFromBackend(w, r, c)

	case stateCloning:
		// Clone in progress, forward to upstream
		logger.DebugContext(ctx, "Clone in progress, forwarding to upstream")
		s.forwardToUpstream(w, r, host, pathValue)

	case stateEmpty:
		// Start cloning in background, forward this request to upstream
		logger.DebugContext(ctx, "Starting background clone, forwarding to upstream")
		go s.startClone(context.WithoutCancel(ctx), c)
		s.forwardToUpstream(w, r, host, pathValue)
	}
}

// ExtractRepoPath extracts the repository path from the request path,
// removing git-specific suffixes.
func ExtractRepoPath(pathValue string) string {
	repoPath := pathValue
	repoPath = strings.TrimSuffix(repoPath, "/info/refs")
	repoPath = strings.TrimSuffix(repoPath, "/git-upload-pack")
	repoPath = strings.TrimSuffix(repoPath, "/git-receive-pack")
	repoPath = strings.TrimSuffix(repoPath, ".git")
	return repoPath
}

// handleBundleRequest serves a git bundle from the cache.
func (s *Strategy) handleBundleRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	logger.DebugContext(ctx, "Bundle request",
		slog.String("host", host),
		slog.String("path", pathValue))

	// Remove /bundle suffix to get repo path
	pathValue = strings.TrimSuffix(pathValue, "/bundle")

	// Extract repo path and construct upstream URL
	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath

	// Generate cache key
	cacheKey := cache.NewKey(upstreamURL + ".bundle")

	// Open bundle from cache
	reader, headers, err := s.cache.Open(ctx, cacheKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.DebugContext(ctx, "Bundle not found in cache",
				slog.String("upstream", upstreamURL))
			http.NotFound(w, r)
			return
		}
		logger.ErrorContext(ctx, "Failed to open bundle from cache",
			slog.String("upstream", upstreamURL),
			slog.String("error", err.Error()))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	// Set headers
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Stream bundle to client
	_, err = io.Copy(w, reader)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to stream bundle",
			slog.String("upstream", upstreamURL),
			slog.String("error", err.Error()))
	}
}

// getOrCreateClone returns an existing clone or creates a new one in empty state.
func (s *Strategy) getOrCreateClone(ctx context.Context, upstreamURL string) *clone {
	s.clonesMu.RLock()
	c, exists := s.clones[upstreamURL]
	s.clonesMu.RUnlock()

	if exists {
		return c
	}

	s.clonesMu.Lock()
	defer s.clonesMu.Unlock()

	// Double-check after acquiring write lock
	if c, exists = s.clones[upstreamURL]; exists {
		return c
	}

	// Create new clone entry
	clonePath := s.clonePathForURL(upstreamURL)

	c = &clone{
		state:       stateEmpty,
		path:        clonePath,
		upstreamURL: upstreamURL,
		fetchSem:    make(chan struct{}, 1),
	}

	// Check if clone already exists on disk (from previous run)
	if _, err := os.Stat(clonePath); err == nil {
		c.state = stateReady
		logging.FromContext(ctx).DebugContext(ctx, "Found existing clone on disk",
			slog.String("path", clonePath))

		// Start bundle generation loop for existing clone
		if s.config.BundleInterval > 0 {
			go s.cloneBundleLoop(s.ctx, c)
		}
	}

	// Initialize semaphore as available
	c.fetchSem <- struct{}{}

	s.clones[upstreamURL] = c
	return c
}

// clonePathForURL returns the filesystem path for a clone given its upstream URL.
func (s *Strategy) clonePathForURL(upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		// Fallback to simple hash if URL parsing fails
		return filepath.Join(s.config.MirrorRoot, "unknown.git")
	}

	// Create path: {mirror_root}/{host}/{path}.git
	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(s.config.MirrorRoot, parsed.Host, repoPath+".git")
}

// discoverExistingClones scans the mirror root for existing clones and starts bundle loops.
func (s *Strategy) discoverExistingClones(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	// Walk the mirror root directory
	err := filepath.Walk(s.config.MirrorRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip non-directories
		if !info.IsDir() {
			return nil
		}

		// Check if this directory is a bare git repository by looking for HEAD file
		headPath := filepath.Join(path, "HEAD")
		if _, statErr := os.Stat(headPath); statErr != nil {
			// Skip if HEAD doesn't exist (not a git repo)
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			// Return other errors
			return errors.Wrap(statErr, "stat HEAD file")
		}

		// Extract upstream URL from path
		relPath, err := filepath.Rel(s.config.MirrorRoot, path)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get relative path",
				slog.String("path", path),
				slog.String("error", err.Error()))
			return nil
		}

		// Convert path to upstream URL: {host}/{path}.git -> https://{host}/{path}
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) < 2 {
			return nil
		}

		host := parts[0]
		repoPath := strings.TrimSuffix(strings.Join(parts[1:], "/"), ".git")
		upstreamURL := "https://" + host + "/" + repoPath

		// Create clone entry
		c := &clone{
			state:       stateReady,
			path:        path,
			upstreamURL: upstreamURL,
			fetchSem:    make(chan struct{}, 1),
		}
		c.fetchSem <- struct{}{}

		s.clonesMu.Lock()
		s.clones[upstreamURL] = c
		s.clonesMu.Unlock()

		logger.DebugContext(ctx, "Discovered existing clone",
			slog.String("path", path),
			slog.String("upstream", upstreamURL))

		// Start bundle generation loop
		if s.config.BundleInterval > 0 {
			go s.cloneBundleLoop(s.ctx, c)
		}

		return nil
	})

	if err != nil {
		return errors.Wrap(err, "walk mirror root")
	}

	return nil
}

// startClone initiates a git clone operation.
func (s *Strategy) startClone(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	c.mu.Lock()
	if c.state != stateEmpty {
		c.mu.Unlock()
		return
	}
	c.state = stateCloning
	c.mu.Unlock()

	logger.InfoContext(ctx, "Starting clone",
		slog.String("upstream", c.upstreamURL),
		slog.String("path", c.path))

	err := s.executeClone(ctx, c)

	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		logger.ErrorContext(ctx, "Clone failed",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		c.state = stateEmpty
		return
	}

	c.state = stateReady
	c.lastFetch = time.Now()
	logger.InfoContext(ctx, "Clone completed",
		slog.String("upstream", c.upstreamURL),
		slog.String("path", c.path))

	// Start bundle generation loop for new clone
	if s.config.BundleInterval > 0 {
		go s.cloneBundleLoop(context.WithoutCancel(ctx), c)
	}
}

// maybeBackgroundFetch triggers a background fetch if enough time has passed.
func (s *Strategy) maybeBackgroundFetch(ctx context.Context, c *clone) {
	c.mu.RLock()
	lastFetch := c.lastFetch
	c.mu.RUnlock()

	if time.Since(lastFetch) < s.config.FetchInterval {
		return
	}

	go s.backgroundFetch(context.WithoutCancel(ctx), c)
}

// backgroundFetch fetches updates from upstream.
func (s *Strategy) backgroundFetch(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	c.mu.Lock()
	// Double-check timing after acquiring lock
	if time.Since(c.lastFetch) < s.config.FetchInterval {
		c.mu.Unlock()
		return
	}
	c.lastFetch = time.Now() // Update immediately to prevent concurrent fetches
	c.mu.Unlock()

	logger.DebugContext(ctx, "Fetching updates",
		slog.String("upstream", c.upstreamURL),
		slog.String("path", c.path))

	if err := s.executeFetch(ctx, c); err != nil {
		logger.ErrorContext(ctx, "Fetch failed",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
	}
}
