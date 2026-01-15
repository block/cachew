// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"context"
	"log/slog"
	"net/http"
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
	MirrorRoot    string        `hcl:"mirror-root" help:"Directory to store git mirrors." required:""`
	FetchInterval time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
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
	mu          sync.RWMutex
	state       cloneState
	path        string
	upstreamURL string
	lastFetch   time.Time
}

// Strategy implements a protocol-aware Git caching proxy.
type Strategy struct {
	config     Config
	cache      cache.Cache
	clones     map[string]*clone
	clonesMu   sync.RWMutex
	httpClient *http.Client
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

	if err := os.MkdirAll(config.MirrorRoot, 0o750); err != nil {
		return nil, errors.Wrap(err, "create mirror root directory")
	}

	s := &Strategy{
		config:     config,
		cache:      cache,
		clones:     make(map[string]*clone),
		httpClient: http.DefaultClient,
	}

	mux.Handle("GET /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))
	mux.Handle("POST /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))

	logger.InfoContext(ctx, "Git strategy initialized",
		"mirror_root", config.MirrorRoot,
		"fetch_interval", config.FetchInterval)

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

	switch state {
	case stateReady:
		// Check if we need to fetch updates
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
	}

	// Check if clone already exists on disk (from previous run)
	if _, err := os.Stat(clonePath); err == nil {
		c.state = stateReady
		logging.FromContext(ctx).DebugContext(ctx, "Found existing clone on disk",
			slog.String("path", clonePath))
	}

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
