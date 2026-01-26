// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func init() {
	strategy.Register("git", "Caches Git repositories, including bundle and tarball snapshots.", New)
}

type Config struct {
	MirrorRoot       string        `hcl:"mirror-root" help:"Directory to store git clones." required:""`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
	BundleInterval   time.Duration `hcl:"bundle-interval,optional" help:"How often to generate bundles. 0 disables bundling." default:"0"`
	CloneDepth       int           `hcl:"clone-depth,optional" help:"Depth for shallow clones. 0 means full clone." default:"0"`
}

type Strategy struct {
	config       Config
	cache        cache.Cache
	cloneManager *gitclone.Manager
	httpClient   *http.Client
	proxy        *httputil.ReverseProxy
	ctx          context.Context
	scheduler    jobscheduler.Scheduler
}

func New(ctx context.Context, config Config, scheduler jobscheduler.Scheduler, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
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

	cloneManager, err := gitclone.NewManager(ctx, gitclone.Config{
		RootDir:          config.MirrorRoot,
		FetchInterval:    config.FetchInterval,
		RefCheckInterval: config.RefCheckInterval,
		CloneDepth:       config.CloneDepth,
		GitConfig:        gitclone.DefaultGitTuningConfig(),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create clone manager")
	}

	s := &Strategy{
		config:       config,
		cache:        cache,
		cloneManager: cloneManager,
		httpClient:   http.DefaultClient,
		ctx:          ctx,
		scheduler:    scheduler.WithQueuePrefix("git"),
	}

	if err := s.cloneManager.DiscoverExisting(ctx); err != nil {
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

	repo, err := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone",
			slog.String("error", err.Error()))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	state := repo.State()
	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case gitclone.StateReady:
		if isInfoRefs {
			if err := s.ensureRefsUpToDate(ctx, repo); err != nil {
				logger.WarnContext(ctx, "Failed to ensure refs up to date",
					slog.String("error", err.Error()))
			}
		}
		s.maybeBackgroundFetch(repo)
		s.serveFromBackend(w, r, repo)

	case gitclone.StateCloning:
		logger.DebugContext(ctx, "Clone in progress, forwarding to upstream")
		s.forwardToUpstream(w, r, host, pathValue)

	case gitclone.StateEmpty:
		logger.DebugContext(ctx, "Starting background clone, forwarding to upstream")
		s.scheduler.Submit(repo.UpstreamURL(), "clone", func(ctx context.Context) error {
			s.startClone(ctx, repo)
			return nil
		})
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

func (s *Strategy) startClone(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)

	logger.InfoContext(ctx, "Starting clone",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("path", repo.Path()))

	gitcloneConfig := gitclone.Config{
		RootDir:          s.config.MirrorRoot,
		FetchInterval:    s.config.FetchInterval,
		RefCheckInterval: s.config.RefCheckInterval,
		CloneDepth:       s.config.CloneDepth,
		GitConfig:        gitclone.DefaultGitTuningConfig(),
	}

	err := repo.Clone(ctx, gitcloneConfig)

	if err != nil {
		logger.ErrorContext(ctx, "Clone failed",
			slog.String("upstream", repo.UpstreamURL()),
			slog.String("error", err.Error()))
		return
	}

	logger.InfoContext(ctx, "Clone completed",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("path", repo.Path()))

	if s.config.BundleInterval > 0 {
		s.scheduleBundleJobs(repo)
	}
}

func (s *Strategy) maybeBackgroundFetch(repo *gitclone.Repository) {
	if !repo.NeedsFetch(s.config.FetchInterval) {
		return
	}

	s.scheduler.Submit(repo.UpstreamURL(), "fetch", func(ctx context.Context) error {
		s.backgroundFetch(ctx, repo)
		return nil
	})
}

func (s *Strategy) backgroundFetch(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)

	if !repo.NeedsFetch(s.config.FetchInterval) {
		return
	}

	logger.DebugContext(ctx, "Fetching updates",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("path", repo.Path()))

	gitcloneConfig := gitclone.Config{
		RootDir:          s.config.MirrorRoot,
		FetchInterval:    s.config.FetchInterval,
		RefCheckInterval: s.config.RefCheckInterval,
		CloneDepth:       s.config.CloneDepth,
		GitConfig:        gitclone.DefaultGitTuningConfig(),
	}

	if err := repo.Fetch(ctx, gitcloneConfig); err != nil {
		logger.ErrorContext(ctx, "Fetch failed",
			slog.String("upstream", repo.UpstreamURL()),
			slog.String("error", err.Error()))
	}
}

func (s *Strategy) scheduleBundleJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "bundle-periodic", s.config.BundleInterval, func(ctx context.Context) error {
		s.generateAndUploadBundle(ctx, repo)
		return nil
	})
}
