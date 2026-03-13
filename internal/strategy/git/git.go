// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
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
	"github.com/block/cachew/internal/snapshot"
	"github.com/block/cachew/internal/strategy"
)

func Register(r *strategy.Registry, scheduler jobscheduler.Provider, cloneManagerProvider gitclone.ManagerProvider, tokenManagerProvider githubapp.TokenManagerProvider) {
	strategy.Register(r, "git", "Caches Git repositories, including tarball snapshots.", func(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
		return New(ctx, config, scheduler, cache, mux, cloneManagerProvider, tokenManagerProvider)
	})
}

type Config struct {
	SnapshotInterval       time.Duration `hcl:"snapshot-interval,optional" help:"How often to generate tar.zstd workstation snapshots. 0 disables snapshots." default:"0"`
	MirrorSnapshotInterval time.Duration `hcl:"mirror-snapshot-interval,optional" help:"How often to generate mirror snapshots for pod bootstrap. 0 uses snapshot-interval. Defaults to 6h." default:"6h"`
	RepackInterval         time.Duration `hcl:"repack-interval,optional" help:"How often to run full repack. 0 disables." default:"0"`
	// ServerURL is embedded as remote.origin.url in snapshots so git pull goes through cachew.
	ServerURL   string `hcl:"server-url,optional" help:"Base URL of this cachew instance, embedded in snapshot remote URLs." default:"${CACHEW_URL}"`
	ZstdThreads int    `hcl:"zstd-threads,optional" help:"Threads for zstd compression/decompression (0 = all CPU cores)." default:"0"`
}

type Strategy struct {
	config         Config
	cache          cache.Cache
	cloneManager   *gitclone.Manager
	httpClient     *http.Client
	proxy          *httputil.ReverseProxy
	ctx            context.Context
	scheduler      jobscheduler.Scheduler
	spoolsMu       sync.Mutex
	spools         map[string]*RepoSpools
	tokenManager   *githubapp.TokenManager
	snapshotMu     sync.Map // keyed by upstream URL, values are *sync.Mutex
	snapshotSpools sync.Map // keyed by upstream URL, values are *snapshotSpoolEntry
}

func New(
	ctx context.Context,
	config Config,
	schedulerProvider jobscheduler.Provider,
	cache cache.Cache,
	mux strategy.Mux,
	cloneManagerProvider gitclone.ManagerProvider,
	tokenManagerProvider githubapp.TokenManagerProvider,
) (*Strategy, error) {
	if _, err := exec.LookPath("git"); err != nil {
		return nil, errors.New("git is required but not found in PATH")
	}
	if config.SnapshotInterval > 0 {
		for _, bin := range []string{"tar", "zstd"} {
			if _, err := exec.LookPath(bin); err != nil {
				return nil, errors.Errorf("%s is required for snapshots (snapshot-interval > 0) but not found in PATH", bin)
			}
		}
	}

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
	for _, dir := range []string{".spools", ".snapshots", ".snapshot-spools"} {
		if err := os.RemoveAll(filepath.Join(cloneManager.Config().MirrorRoot, dir)); err != nil {
			return nil, errors.Wrapf(err, "clean up stale %s", dir)
		}
	}

	scheduler, err := schedulerProvider()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create scheduler")
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
	s.config.ServerURL = strings.TrimRight(config.ServerURL, "/")

	existing, err := s.cloneManager.DiscoverExisting(ctx)
	if err != nil {
		logger.WarnContext(ctx, "Failed to discover existing clones", "error", err)
	}
	for _, repo := range existing {
		logger.InfoContext(ctx, "Running startup fetch for existing repo", "upstream", repo.UpstreamURL())

		preRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get pre-fetch refs for existing repo", "upstream", repo.UpstreamURL(),
				"error", err)
		} else {
			logger.InfoContext(ctx, "Pre-fetch refs for existing repo", "upstream", repo.UpstreamURL(), "refs", preRefs)
		}

		start := time.Now()
		if err := repo.Fetch(ctx); err != nil {
			logger.ErrorContext(ctx, "Startup fetch failed for existing repo", "upstream", repo.UpstreamURL(), "error", err,
				"duration", time.Since(start))
		} else {
			logger.InfoContext(ctx, "Startup fetch completed for existing repo", "upstream", repo.UpstreamURL(),
				"duration", time.Since(start))
		}

		postRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get post-fetch refs for existing repo", "upstream", repo.UpstreamURL(),
				"error", err)
		} else {
			logger.InfoContext(ctx, "Post-fetch refs for existing repo", "upstream", repo.UpstreamURL(), "refs", postRefs)
		}

		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
		if s.config.RepackInterval > 0 {
			s.scheduleRepackJobs(repo)
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
						logger.DebugContext(req.Context(), "Injecting GitHub App auth into upstream request", "org", org)
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

	logger.InfoContext(ctx, "Git strategy initialized", "snapshot_interval", config.SnapshotInterval)

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

	logger.DebugContext(ctx, "Git request", "method", r.Method, "host", host, "path", pathValue)

	if strings.HasSuffix(pathValue, "/snapshot.tar.zst") {
		s.handleSnapshotRequest(w, r, host, pathValue)
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
		logger.ErrorContext(ctx, "Failed to get or create clone", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	state := repo.State()
	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case gitclone.StateReady:
		s.serveReadyRepo(w, r, repo, host, pathValue, isInfoRefs)

	case gitclone.StateCloning, gitclone.StateEmpty:
		if state == gitclone.StateEmpty {
			logger.DebugContext(ctx, "Starting background clone, forwarding to upstream")
			s.scheduler.Submit(repo.UpstreamURL(), "clone", func(ctx context.Context) error {
				s.startClone(ctx, repo)
				return nil
			})
		}
		s.serveWithSpool(w, r, host, pathValue, upstreamURL)
	}
}

func (s *Strategy) serveReadyRepo(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, host, pathValue string, isInfoRefs bool) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	if isInfoRefs {
		if err := s.ensureRefsUpToDate(ctx, repo); err != nil {
			logger.WarnContext(ctx, "Failed to check upstream refs", "error", err)
		}
	}
	s.maybeBackgroundFetch(repo)

	// Buffer the request body so it can be replayed if serveFromBackend
	// signals a fallback to upstream (e.g. on "not our ref").
	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody {
		var readErr error
		bodyBytes, readErr = io.ReadAll(r.Body)
		if readErr != nil {
			logger.ErrorContext(ctx, "Failed to read request body", "error", readErr)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		r.ContentLength = int64(len(bodyBytes))
		r.TransferEncoding = nil
	}

	if s.serveFromBackend(w, r, repo) {
		// The mirror is missing the requested object — most likely a commit
		// that was advertised before a concurrent force-push fetch orphaned
		// it. Fall back to upstream so the client is not left with an error.
		logger.InfoContext(ctx, "Falling back to upstream due to 'not our ref'", "path", pathValue)
		if bodyBytes != nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			r.ContentLength = int64(len(bodyBytes))
			r.TransferEncoding = nil
		}
		s.forwardToUpstream(w, r, host, pathValue)
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

func spoolDirForURL(mirrorRoot, upstreamURL string) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve spool directory")
	}
	return filepath.Join(mirrorRoot, ".spools", repoPath), nil
}

func (s *Strategy) getOrCreateRepoSpools(upstreamURL string) (*RepoSpools, error) {
	s.spoolsMu.Lock()
	defer s.spoolsMu.Unlock()
	rp, exists := s.spools[upstreamURL]
	if exists {
		return rp, nil
	}
	dir, err := spoolDirForURL(s.cloneManager.Config().MirrorRoot, upstreamURL)
	if err != nil {
		return nil, err
	}
	rp = NewRepoSpools(dir)
	s.spools[upstreamURL] = rp
	return rp, nil
}

func (s *Strategy) cleanupSpools(upstreamURL string) error {
	s.spoolsMu.Lock()
	rp, exists := s.spools[upstreamURL]
	if exists {
		delete(s.spools, upstreamURL)
	}
	s.spoolsMu.Unlock()
	if rp != nil {
		if err := rp.Close(); err != nil {
			return errors.Wrap(err, "clean up spools")
		}
	}
	return nil
}

func (s *Strategy) serveWithSpool(w http.ResponseWriter, r *http.Request, host, pathValue, upstreamURL string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	key, err := SpoolKeyForRequest(pathValue, r)
	if err != nil {
		logger.WarnContext(ctx, "Failed to compute spool key, forwarding to upstream", "error", err)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}
	if key == "" {
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	rp, err := s.getOrCreateRepoSpools(upstreamURL)
	if err != nil {
		logger.WarnContext(ctx, "Failed to resolve spool directory, forwarding to upstream", "error", err)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}
	spool, isWriter, err := rp.GetOrCreate(key)
	if err != nil {
		logger.WarnContext(ctx, "Failed to create spool, forwarding to upstream", "error", err)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	if isWriter {
		logger.DebugContext(ctx, "Spooling upstream response", "key", key, "upstream", upstreamURL)
		tw := NewSpoolTeeWriter(w, spool)
		s.forwardToUpstream(tw, r, host, pathValue)
		spool.MarkComplete()
		return
	}

	if spool.Failed() {
		logger.DebugContext(ctx, "Spool failed, forwarding to upstream", "key", key)
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	logger.DebugContext(ctx, "Serving from spool", "key", key, "upstream", upstreamURL)
	if err := spool.ServeTo(w); err != nil {
		if errors.Is(err, ErrSpoolFailed) {
			logger.DebugContext(ctx, "Spool failed before response started, forwarding to upstream", "key", key)
			s.forwardToUpstream(w, r, host, pathValue)
			return
		}
		logger.WarnContext(ctx, "Spool read failed mid-stream", "key", key, "error", err)
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

// ensureCloneReady blocks until the repository mirror is ready. If the mirror
// does not exist yet (StateEmpty), it triggers a clone synchronously. If another
// goroutine is already cloning (StateCloning), it polls until completion or the
// context is cancelled. Returns an error if the clone fails or the context is done.
func (s *Strategy) ensureCloneReady(ctx context.Context, repo *gitclone.Repository) error {
	if repo.State() == gitclone.StateEmpty {
		s.startClone(ctx, repo)
	}
	for repo.State() == gitclone.StateCloning {
		t := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Wrap(ctx.Err(), "cancelled waiting for clone")
		case <-t.C:
		}
	}
	if repo.State() != gitclone.StateReady {
		return errors.New("repository unavailable after clone attempt")
	}
	return nil
}

func (s *Strategy) startClone(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Attempting mirror snapshot restore", "upstream", upstream)

	if err := s.tryRestoreSnapshot(ctx, repo); err != nil {
		logger.InfoContext(ctx, "Mirror snapshot restore failed, falling back to clone", "upstream", upstream, "error", err)
	} else {
		// Mirror snapshot restored successfully. The bare mirror is immediately
		// servable — mark ready and let background fetch handle freshening.
		repo.MarkReady()

		if err := s.cleanupSpools(upstream); err != nil {
			logger.WarnContext(ctx, "Failed to clean up spools", "upstream", upstream, "error", err)
		}

		logger.InfoContext(ctx, "Mirror snapshot restored, serving immediately", "upstream", upstream)

		// Fetch synchronously so the mirror is fresh before we serve from it.
		// Mirror snapshots can be hours old; serving stale data defeats the
		// purpose of the cache.
		if err := s.backgroundFetch(ctx, repo); err != nil {
			logger.WarnContext(ctx, "Post-restore fetch failed, serving from snapshot", "upstream", upstream, "error", err)
		}

		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
		if s.config.RepackInterval > 0 {
			s.scheduleRepackJobs(repo)
		}
		return
	}

	logger.InfoContext(ctx, "Starting clone", "upstream", upstream, "path", repo.Path())

	err := repo.Clone(ctx)

	// Clean up spools regardless of clone success or failure, so that subsequent
	// requests either serve from the local backend or go directly to upstream.
	if cleanupErr := s.cleanupSpools(upstream); cleanupErr != nil {
		logger.WarnContext(ctx, "Failed to clean up spools", "upstream", upstream, "error", cleanupErr)
	}

	if err != nil {
		logger.ErrorContext(ctx, "Clone failed", "upstream", upstream, "error", err)
		return
	}

	logger.InfoContext(ctx, "Clone completed", "upstream", upstream, "path", repo.Path())

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
	if s.config.RepackInterval > 0 {
		s.scheduleRepackJobs(repo)
	}
}

// tryRestoreSnapshot attempts to restore a mirror from an S3 mirror snapshot.
// Mirror snapshots are bare repositories that can be extracted and used directly
// without any conversion. On failure the repo path is cleaned up so the caller
// can fall back to clone.
func (s *Strategy) tryRestoreSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	cacheKey := mirrorSnapshotCacheKey(repo.UpstreamURL())

	if err := os.MkdirAll(filepath.Dir(repo.Path()), 0o750); err != nil {
		return errors.Wrap(err, "create parent directory for restore")
	}

	logger := logging.FromContext(ctx)

	if err := snapshot.Restore(ctx, s.cache, cacheKey, repo.Path(), s.config.ZstdThreads); err != nil {
		_ = os.RemoveAll(repo.Path())
		return errors.Wrap(err, "restore mirror snapshot")
	}
	logger.InfoContext(ctx, "Mirror snapshot extracted", "upstream", repo.UpstreamURL(), "path", repo.Path())

	if err := repo.MarkRestored(ctx); err != nil {
		_ = os.RemoveAll(repo.Path())
		return errors.Wrap(err, "mark restored")
	}
	logger.InfoContext(ctx, "Repository marked as restored", "upstream", repo.UpstreamURL(), "state", repo.State())

	return nil
}

func (s *Strategy) maybeBackgroundFetch(repo *gitclone.Repository) {
	if !repo.NeedsFetch(s.cloneManager.Config().FetchInterval) {
		return
	}

	// Use a separate queue from snapshot/repack so fetches are not serialized
	// behind long-running jobs on the same upstream URL queue.
	s.scheduler.Submit(repo.UpstreamURL()+"/fetch", "fetch", func(ctx context.Context) error {
		return s.backgroundFetch(ctx, repo)
	})
}

func (s *Strategy) backgroundFetch(ctx context.Context, repo *gitclone.Repository) error {
	if !repo.NeedsFetch(s.cloneManager.Config().FetchInterval) {
		return nil
	}

	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Fetching updates", "upstream", repo.UpstreamURL(), "path", repo.Path())

	start := time.Now()
	if err := repo.Fetch(ctx); err != nil {
		return errors.Errorf("fetch failed in %s: %w", time.Since(start), err)
	}
	logger.InfoContext(ctx, "Fetch completed", "upstream", repo.UpstreamURL(), "duration", time.Since(start))
	return nil
}
