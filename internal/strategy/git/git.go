// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func Register(r *strategy.Registry, scheduler jobscheduler.Scheduler, cloneManagerProvider gitclone.ManagerProvider, tokenManagerProvider githubapp.TokenManagerProvider) {
	strategy.Register(r, "git", "Caches Git repositories, including bundle and tarball snapshots.", func(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
		return New(ctx, config, scheduler, cache, mux, cloneManagerProvider, tokenManagerProvider)
	})
}

type Config struct {
	BundleInterval   time.Duration `hcl:"bundle-interval,optional" help:"How often to generate bundles. 0 disables bundling." default:"0"`
	SnapshotInterval time.Duration `hcl:"snapshot-interval,optional" help:"How often to generate tar.zstd snapshots. 0 disables snapshots." default:"0"`
}

type Strategy struct {
	config       Config
	cache        cache.Cache
	cloneManager *gitclone.Manager
	httpClient   *http.Client
	proxy        *httputil.ReverseProxy
	ctx          context.Context
	scheduler    jobscheduler.Scheduler
	spoolsMu     sync.Mutex
	spools       map[string]*RepoSpools
	tokenManager *githubapp.TokenManager
}

func New(
	ctx context.Context,
	config Config,
	scheduler jobscheduler.Scheduler,
	cache cache.Cache,
	mux strategy.Mux,
	cloneManagerProvider gitclone.ManagerProvider,
	tokenManagerProvider githubapp.TokenManagerProvider,
) (*Strategy, error) {
	logger := logging.FromContext(ctx)

	// Get GitHub App token manager if configured
	tokenManager, err := tokenManagerProvider()
	if err != nil {
		return nil, errors.Wrap(err, "create token manager")
	}
	if tokenManager != nil {
		logger.InfoContext(ctx, "Using GitHub App authentication for git strategy")
	} else {
		logger.WarnContext(ctx, "GitHub App not configured, using system git credentials")
	}

	cloneManager, err := cloneManagerProvider()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create clone manager")
	}
	if err := os.RemoveAll(filepath.Join(cloneManager.Config().MirrorRoot, ".spools")); err != nil {
		return nil, errors.Wrap(err, "clean up stale spools")
	}

	s := &Strategy{
		config:       config,
		cache:        cache,
		cloneManager: cloneManager,
		httpClient:   http.DefaultClient,
		ctx:          ctx,
		scheduler:    scheduler.WithQueuePrefix("git"),
		spools:       make(map[string]*RepoSpools),
		tokenManager: tokenManager,
	}

	existing, err := s.cloneManager.DiscoverExisting(ctx)
	if err != nil {
		logger.WarnContext(ctx, "Failed to discover existing clones",
			"error", err)
	}
	for _, repo := range existing {
		if s.config.BundleInterval > 0 {
			s.scheduleBundleJobs(repo)
		}
		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
	}

	s.proxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = req.PathValue("host")
			req.URL.Path = "/" + req.PathValue("path")
			req.Host = req.URL.Host

			// Inject GitHub App authentication for github.com requests
			if s.tokenManager != nil && req.URL.Host == "github.com" {
				// Extract org from path (e.g., /squareup/blox.git/...)
				parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
				if len(parts) >= 1 && parts[0] != "" {
					org := parts[0]
					token, err := s.tokenManager.GetTokenForOrg(req.Context(), org)
					if err == nil && token != "" {
						// Inject token as Basic auth with "x-access-token" username
						req.SetBasicAuth("x-access-token", token)
						logger.DebugContext(req.Context(), "Injecting GitHub App auth into upstream request",
							"org", org)
					}
				}
			}
		},
		Transport: s.httpClient.Transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logging.FromContext(r.Context()).ErrorContext(r.Context(), "Upstream request failed", "error", err)
			w.WriteHeader(http.StatusBadGateway)
		},
	}

	mux.Handle("GET /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))
	mux.Handle("POST /git/{host}/{path...}", http.HandlerFunc(s.handleRequest))

	logger.InfoContext(ctx, "Git strategy initialized",
		"bundle_interval", config.BundleInterval,
		"snapshot_interval", config.SnapshotInterval)

	return s, nil
}

var _ strategy.Strategy = (*Strategy)(nil)

// SetHTTPTransport overrides the HTTP transport used for upstream requests.
// This is intended for testing.
func (s *Strategy) SetHTTPTransport(t http.RoundTripper) {
	s.httpClient.Transport = t
	s.proxy.Transport = t
}

func (s *Strategy) String() string { return "git" }

func (s *Strategy) handleRequest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	logger.DebugContext(ctx, fmt.Sprintf("Git request: %s %s%s", r.Method, host, pathValue),
		"method", r.Method,
		"host", host,
		"path", pathValue)

	if strings.HasSuffix(pathValue, "/bundle") {
		s.handleBundleRequest(w, r, host, pathValue)
		return
	}

	if strings.HasSuffix(pathValue, "/snapshot") {
		s.handleSnapshotRequest(w, r, host, pathValue)
		return
	}

	service := r.URL.Query().Get("service")
	isReceivePack := service == "git-receive-pack" || strings.HasSuffix(pathValue, "/git-receive-pack")

	if isReceivePack {
		logger.DebugContext(ctx, fmt.Sprintf("Forwarding write operation to upstream: %s %s%s", r.Method, host, pathValue),
			slog.String("method", r.Method),
			slog.String("host", host),
			slog.String("path", pathValue))
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath

	repo, err := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Failed to get or create clone for %s: %v", upstreamURL, err),
			"upstream", upstreamURL,
			"error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	state := repo.State()
	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case gitclone.StateReady:
		if isInfoRefs {
			if err := s.ensureRefsUpToDate(ctx, repo); err != nil {
				logger.WarnContext(ctx, fmt.Sprintf("Failed to ensure refs up to date for %s: %v", repo.UpstreamURL(), err),
					"upstream", repo.UpstreamURL(),
					"error", err)
			}
		}
		s.maybeBackgroundFetch(repo)
		s.serveFromBackend(w, r, repo)

	case gitclone.StateCloning, gitclone.StateEmpty:
		if state == gitclone.StateEmpty {
			logger.DebugContext(ctx, fmt.Sprintf("Starting background clone for %s, forwarding to upstream", repo.UpstreamURL()),
				"upstream", repo.UpstreamURL())
			s.scheduler.Submit(repo.UpstreamURL(), "clone", func(ctx context.Context) error {
				s.startClone(ctx, repo)
				return nil
			})
		}
		s.serveWithSpool(w, r, host, pathValue, upstreamURL)
	}
}

// SpoolKeyForRequest returns the spool key for a request, or empty string if the
// request is not spoolable. For POST requests, the body is hashed to differentiate
// protocol v2 commands (e.g. ls-refs vs fetch) that share the same URL. The request
// body is buffered and replaced so it can still be read by the caller.
func SpoolKeyForRequest(pathValue string, r *http.Request) (string, error) {
	if !strings.HasSuffix(pathValue, "/git-upload-pack") {
		return "", nil
	}
	if r.Method != http.MethodPost || r.Body == nil {
		return "upload-pack", nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", errors.Wrap(err, "read request body for spool key")
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	h := sha256.Sum256(body)
	return "upload-pack-" + hex.EncodeToString(h[:8]), nil
}

func spoolDirForURL(mirrorRoot, upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return filepath.Join(mirrorRoot, ".spools", "unknown")
	}
	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(mirrorRoot, ".spools", parsed.Host, repoPath)
}

func (s *Strategy) getOrCreateRepoSpools(upstreamURL string) *RepoSpools {
	s.spoolsMu.Lock()
	defer s.spoolsMu.Unlock()
	rp, exists := s.spools[upstreamURL]
	if !exists {
		dir := spoolDirForURL(s.cloneManager.Config().MirrorRoot, upstreamURL)
		rp = NewRepoSpools(dir)
		s.spools[upstreamURL] = rp
	}
	return rp
}

func (s *Strategy) cleanupSpools(upstreamURL string) {
	s.spoolsMu.Lock()
	rp, exists := s.spools[upstreamURL]
	if exists {
		delete(s.spools, upstreamURL)
	}
	s.spoolsMu.Unlock()
	if rp != nil {
		if err := rp.Close(); err != nil {
			logging.FromContext(s.ctx).WarnContext(s.ctx, "Failed to clean up spools",
				"upstream", upstreamURL,
				"error", err)
		}
	}
}

func (s *Strategy) serveWithSpool(w http.ResponseWriter, r *http.Request, host, pathValue, upstreamURL string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	key, err := SpoolKeyForRequest(pathValue, r)
	if err != nil {
		logger.WarnContext(ctx, "Failed to compute spool key, forwarding to upstream",
			"error", err)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}
	if key == "" {
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	rp := s.getOrCreateRepoSpools(upstreamURL)
	spool, isWriter, err := rp.GetOrCreate(key)
	if err != nil {
		logger.WarnContext(ctx, "Failed to create spool, forwarding to upstream",
			"error", err)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	if isWriter {
		logger.DebugContext(ctx, "Spooling upstream response",
			"key", key,
			"upstream", upstreamURL)
		tw := NewSpoolTeeWriter(w, spool)
		s.forwardToUpstream(tw, r, host, pathValue)
		spool.MarkComplete()
		return
	}

	if spool.Failed() {
		logger.DebugContext(ctx, "Spool failed, forwarding to upstream",
			"key", key)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	logger.DebugContext(ctx, "Serving from spool",
		"key", key,
		"upstream", upstreamURL)
	if err := spool.ServeTo(w); err != nil {
		if errors.Is(err, ErrSpoolFailed) {
			logger.DebugContext(ctx, "Spool failed before response started, forwarding to upstream",
				"key", key)
			s.forwardToUpstream(w, r, host, pathValue)
			return
		}
		logger.WarnContext(ctx, fmt.Sprintf("Spool read failed mid-stream for key %s: %v", key, err),
			"key", key,
			"error", err)
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
	s.serveCachedArtifact(w, r, host, pathValue, "bundle")
}

func (s *Strategy) serveCachedArtifact(w http.ResponseWriter, r *http.Request, host, pathValue, artifact string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	logger.DebugContext(ctx, artifact+" request",
		"host", host,
		"path", pathValue)

	pathValue = strings.TrimSuffix(pathValue, "/"+artifact)
	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath
	cacheKey := cache.NewKey(upstreamURL + "." + artifact)

	reader, headers, err := s.cache.Open(ctx, cacheKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.DebugContext(ctx, artifact+" not found in cache",
				"upstream", upstreamURL)
			http.NotFound(w, r)
			return
		}
		logger.ErrorContext(ctx, fmt.Sprintf("Failed to open %s from cache for %s: %v", artifact, upstreamURL, err),
			"artifact", artifact,
			"upstream", upstreamURL,
			"error", err)
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
		logger.ErrorContext(ctx, fmt.Sprintf("Failed to stream %s for %s: %v", artifact, upstreamURL, err),
			"artifact", artifact,
			"upstream", upstreamURL,
			"error", err)
	}
}

func (s *Strategy) startClone(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)

	logger.InfoContext(ctx, "Starting clone",
		"upstream", repo.UpstreamURL(),
		"path", repo.Path())

	err := repo.Clone(ctx)

	// Clean up spools regardless of clone success or failure, so that subsequent
	// requests either serve from the local backend or go directly to upstream.
	s.cleanupSpools(repo.UpstreamURL())

	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Clone failed for %s: %v", repo.UpstreamURL(), err),
			"upstream", repo.UpstreamURL(),
			"error", err)
		return
	}

	logger.InfoContext(ctx, "Clone completed",
		"upstream", repo.UpstreamURL(),
		"path", repo.Path())

	if s.config.BundleInterval > 0 {
		s.scheduleBundleJobs(repo)
	}

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
}

func (s *Strategy) maybeBackgroundFetch(repo *gitclone.Repository) {
	if !repo.NeedsFetch(s.cloneManager.Config().FetchInterval) {
		return
	}

	s.scheduler.Submit(repo.UpstreamURL(), "fetch", func(ctx context.Context) error {
		s.backgroundFetch(ctx, repo)
		return nil
	})
}

func (s *Strategy) backgroundFetch(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)

	if !repo.NeedsFetch(s.cloneManager.Config().FetchInterval) {
		return
	}

	logger.DebugContext(ctx, "Fetching updates",
		"upstream", repo.UpstreamURL(),
		"path", repo.Path())

	if err := repo.Fetch(ctx); err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Fetch failed for %s: %v", repo.UpstreamURL(), err),
			"upstream", repo.UpstreamURL(),
			"error", err)
	}
}

func (s *Strategy) scheduleBundleJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "bundle-periodic", s.config.BundleInterval, func(ctx context.Context) error {
		return s.generateAndUploadBundle(ctx, repo)
	})
}
