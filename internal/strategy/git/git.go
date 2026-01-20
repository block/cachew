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
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func init() {
	strategy.Register("git", New)
}

type Config struct {
	MirrorRoot       string        `hcl:"mirror-root" help:"Directory to store git clones." required:""`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
	BundleInterval   time.Duration `hcl:"bundle-interval,optional" help:"How often to generate bundles. 0 disables bundling." default:"0"`
	CloneDepth       int           `hcl:"clone-depth,optional" help:"Depth for shallow clones. 0 means full clone." default:"0"`
}

type cloneState int

const (
	stateEmpty cloneState = iota
	stateCloning
	stateReady
)

type clone struct {
	mu            sync.RWMutex
	state         cloneState
	path          string
	upstreamURL   string
	lastFetch     time.Time
	lastRefCheck  time.Time
	refCheckValid bool
	fetchSem      chan struct{}
}

type Strategy struct {
	config     Config
	cache      cache.Cache
	clones     map[string]*clone
	clonesMu   sync.RWMutex
	httpClient *http.Client
	proxy      *httputil.ReverseProxy
	ctx        context.Context
}

func New(ctx context.Context, _ jobscheduler.Scheduler, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
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

func (s *Strategy) handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	logger.DebugContext(ctx, "Git request",
		slog.String("method", r.Method),
		slog.String("host", host),
		slog.String("path", pathValue))

	if strings.HasSuffix(pathValue, "/bundle") {
		s.handleBundleRequest(w, r, host, pathValue)
		return
	}

	service := r.URL.Query().Get("service")
	isReceivePack := service == "git-receive-pack" || strings.HasSuffix(pathValue, "/git-receive-pack")

	if isReceivePack {
		logger.DebugContext(ctx, "Forwarding write operation to upstream")
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath

	c := s.getOrCreateClone(ctx, upstreamURL)

	c.mu.RLock()
	state := c.state
	c.mu.RUnlock()

	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case stateReady:
		if isInfoRefs {
			if err := s.ensureRefsUpToDate(ctx, c); err != nil {
				logger.WarnContext(ctx, "Failed to ensure refs up to date",
					slog.String("error", err.Error()))
			}
		}
		s.maybeBackgroundFetch(ctx, c)
		s.serveFromBackend(w, r, c)

	case stateCloning:
		logger.DebugContext(ctx, "Clone in progress, forwarding to upstream")
		s.forwardToUpstream(w, r, host, pathValue)

	case stateEmpty:
		logger.DebugContext(ctx, "Starting background clone, forwarding to upstream")
		go s.startClone(context.WithoutCancel(ctx), c)
		s.forwardToUpstream(w, r, host, pathValue)
	}
}

func ExtractRepoPath(pathValue string) string {
	repoPath := pathValue
	repoPath = strings.TrimSuffix(repoPath, "/info/refs")
	repoPath = strings.TrimSuffix(repoPath, "/git-upload-pack")
	repoPath = strings.TrimSuffix(repoPath, "/git-receive-pack")
	repoPath = strings.TrimSuffix(repoPath, ".git")
	return repoPath
}

func (s *Strategy) handleBundleRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	logger.DebugContext(ctx, "Bundle request",
		slog.String("host", host),
		slog.String("path", pathValue))

	pathValue = strings.TrimSuffix(pathValue, "/bundle")
	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath
	cacheKey := cache.NewKey(upstreamURL + ".bundle")

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

	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	_, err = io.Copy(w, reader)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to stream bundle",
			slog.String("upstream", upstreamURL),
			slog.String("error", err.Error()))
	}
}

func (s *Strategy) getOrCreateClone(ctx context.Context, upstreamURL string) *clone {
	s.clonesMu.RLock()
	c, exists := s.clones[upstreamURL]
	s.clonesMu.RUnlock()

	if exists {
		return c
	}

	s.clonesMu.Lock()
	defer s.clonesMu.Unlock()

	if c, exists = s.clones[upstreamURL]; exists {
		return c
	}

	clonePath := s.clonePathForURL(upstreamURL)

	c = &clone{
		state:       stateEmpty,
		path:        clonePath,
		upstreamURL: upstreamURL,
		fetchSem:    make(chan struct{}, 1),
	}

	gitDir := filepath.Join(clonePath, ".git")
	if _, err := os.Stat(gitDir); err == nil {
		c.state = stateReady
		logging.FromContext(ctx).DebugContext(ctx, "Found existing clone on disk",
			slog.String("path", clonePath))

		if s.config.BundleInterval > 0 {
			go s.cloneBundleLoop(s.ctx, c)
		}
	}

	c.fetchSem <- struct{}{}

	s.clones[upstreamURL] = c
	return c
}

func (s *Strategy) clonePathForURL(upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return filepath.Join(s.config.MirrorRoot, "unknown")
	}

	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(s.config.MirrorRoot, parsed.Host, repoPath)
}

func (s *Strategy) discoverExistingClones(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	err := filepath.Walk(s.config.MirrorRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		gitDir := filepath.Join(path, ".git")
		headPath := filepath.Join(path, ".git", "HEAD")
		if _, statErr := os.Stat(gitDir); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat .git directory")
		}
		if _, statErr := os.Stat(headPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat HEAD file")
		}

		relPath, err := filepath.Rel(s.config.MirrorRoot, path)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get relative path",
				slog.String("path", path),
				slog.String("error", err.Error()))
			return nil
		}

		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) < 2 {
			return nil
		}

		host := parts[0]
		repoPath := strings.Join(parts[1:], "/")
		upstreamURL := "https://" + host + "/" + repoPath

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

	if s.config.BundleInterval > 0 {
		go s.cloneBundleLoop(context.WithoutCancel(ctx), c)
	}
}

func (s *Strategy) maybeBackgroundFetch(ctx context.Context, c *clone) {
	c.mu.RLock()
	lastFetch := c.lastFetch
	c.mu.RUnlock()

	if time.Since(lastFetch) < s.config.FetchInterval {
		return
	}

	go s.backgroundFetch(context.WithoutCancel(ctx), c)
}

func (s *Strategy) backgroundFetch(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	c.mu.Lock()
	if time.Since(c.lastFetch) < s.config.FetchInterval {
		c.mu.Unlock()
		return
	}
	c.lastFetch = time.Now()
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
