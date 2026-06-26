// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"maps"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/snapshot"
	"github.com/block/cachew/internal/strategy"
)

//nolint:gochecknoglobals // OTel tracer instances are package-scoped by convention
var tracer = otel.Tracer("github.com/block/cachew/internal/strategy/git")

func Register(r *strategy.Registry, scheduler jobscheduler.Provider, cloneManagerProvider gitclone.ManagerProvider, tokenManagerProvider githubapp.TokenManagerProvider) {
	strategy.Register(r, "git", "Caches Git repositories, including tarball snapshots.", func(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
		return New(ctx, config, scheduler, cache, mux, cloneManagerProvider, tokenManagerProvider)
	})
}

type Config struct {
	SnapshotInterval       time.Duration `hcl:"snapshot-interval,optional" help:"How often to generate tar.zstd workstation snapshots. 0 disables snapshots." default:"0"`
	MirrorSnapshotInterval time.Duration `hcl:"mirror-snapshot-interval,optional" help:"How often to generate mirror snapshots for pod bootstrap. 0 uses snapshot-interval. Defaults to 2h." default:"2h"`
	RepackInterval         time.Duration `hcl:"repack-interval,optional" help:"How often to run the geometric repack (consolidates packs, reuses deltas). 0 disables." default:"0"`
	FullRepackInterval     time.Duration `hcl:"full-repack-interval,optional" help:"How often to run the full repack (re-selects deltas across all objects, shrinking the mirror). Far more expensive than the geometric repack, so use a slow cadence. 0 disables." default:"0"`
	ZstdThreads            int           `hcl:"zstd-threads,optional" help:"Threads for zstd compression/decompression. 0 = all CPU cores; useful for short-lived CLI invocations but risky on a long-running server where multiple snapshot/restore operations can run concurrently." default:"4"`
	BundleCacheTTL         time.Duration `hcl:"bundle-cache-ttl,optional" help:"TTL of cached server-side git bundles." default:"2h"`
}

type Strategy struct {
	config              Config
	cache               cache.Cache
	cloneManager        *gitclone.Manager
	httpClient          *http.Client
	proxy               *httputil.ReverseProxy
	ctx                 context.Context
	scheduler           jobscheduler.Scheduler
	spoolsMu            sync.Mutex
	spools              map[string]*RepoSpools
	tokenManager        *githubapp.TokenManager
	snapshotMu          sync.Map // keyed by upstream URL, values are *sync.Mutex
	snapshotSpools      sync.Map // keyed by upstream URL, values are *snapshotSpoolEntry
	coldSnapshotMu      sync.Map // keyed by upstream URL, values are *coldSnapshotEntry
	deferredRestoreOnce sync.Map // keyed by upstream URL, ensures at most one deferred restore per repo
	metrics             *gitMetrics
	repoCounts          *RepoCounts
	ready               atomic.Bool
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
	if config.BundleCacheTTL == 0 {
		config.BundleCacheTTL = 2 * time.Hour
	}
	if config.SnapshotInterval > 0 {
		for _, bin := range []string{"tar", "pzstd"} {
			if _, err := exec.LookPath(bin); err != nil {
				return nil, errors.Errorf("%s is required for snapshots (snapshot-interval > 0) but not found in PATH", bin)
			}
		}
	}

	logger := logging.FromContext(ctx)

	if _, err := exec.LookPath("git-lfs"); err != nil {
		return nil, errors.New("git-lfs is required but not found in PATH")
	}

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

	m := newGitMetrics()

	s := &Strategy{
		config:       config,
		cache:        cache,
		cloneManager: cloneManager,
		httpClient:   http.DefaultClient,
		ctx:          ctx,
		scheduler:    scheduler.WithQueuePrefix("git"),
		spools:       make(map[string]*RepoSpools),
		tokenManager: tokenManager,
		metrics:      m,
	}
	// Run startup fetches in the background so the HTTP listener (and
	// /_liveness) come up immediately. /_readiness gates on Ready() so the
	// Service load balancer holds traffic until warming completes.
	go func() {
		warmCtx := context.WithoutCancel(ctx)
		if err := s.warmExistingRepos(warmCtx); err != nil {
			logger.WarnContext(warmCtx, "Failed to warm existing repos", "error", err)
		}
		s.ready.Store(true)
		logger.InfoContext(warmCtx, "Git strategy ready")
	}()

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
var _ strategy.Readier = (*Strategy)(nil)
var _ strategy.MetadataConsumer = (*Strategy)(nil)

// Ready reports whether startup warm-up has completed.
func (s *Strategy) Ready() bool {
	return s.ready.Load()
}

// SetMetadataStore enables the per-repo clone histogram and schedules its
// daily reaper. Called by config.Load after the metadata backend is built.
func (s *Strategy) SetMetadataStore(store *metadatadb.Store) {
	if store == nil {
		return
	}
	s.repoCounts = NewRepoCounts(store.Namespace("git"))
	logging.FromContext(s.ctx).InfoContext(s.ctx, "Per-repo clone histogram enabled",
		"retention_days", s.repoCounts.retentionDays)
	s.scheduler.SubmitPeriodicJob("repo-counts-reaper", "reap-repo-counts", defaultRepoCountsReapInterval, func(ctx context.Context) error {
		if deleted := s.repoCounts.Reap(); deleted > 0 {
			logging.FromContext(ctx).InfoContext(ctx, "Reaped stale repo clone counts", "deleted", deleted)
		}
		return nil
	})
}

func (s *Strategy) warmExistingRepos(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	existing, err := s.cloneManager.DiscoverExisting(ctx)
	if err != nil {
		return errors.Wrap(err, "discover existing clones")
	}
	for _, repo := range existing {
		logger.InfoContext(ctx, "Running startup fetch for existing repo", "upstream", repo.UpstreamURL())

		preRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			return errors.Wrapf(err, "get pre-fetch refs for %s", repo.UpstreamURL())
		}

		start := time.Now()
		if err := repo.FetchLenient(ctx, s.cloneManager.Config().CloneTimeout); err != nil {
			logger.ErrorContext(ctx, "Startup fetch failed for existing repo", "upstream", repo.UpstreamURL(), "error", err,
				"duration", time.Since(start))
			continue
		}
		logger.InfoContext(ctx, "Startup fetch completed for existing repo", "upstream", repo.UpstreamURL(),
			"duration", time.Since(start))

		postRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			return errors.Wrapf(err, "get post-fetch refs for %s", repo.UpstreamURL())
		}
		maps.DeleteFunc(postRefs, func(k, v string) bool { return preRefs[k] == v })
		logger.InfoContext(ctx, "Post-fetch changed refs for existing repo", "upstream", repo.UpstreamURL(), "refs", postRefs)

		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
		if s.repackEnabled() {
			s.scheduleRepackJobs(repo)
		}
	}
	return nil
}

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

	if strings.HasSuffix(pathValue, EnsureRefsPath) {
		s.handleEnsureRefs(w, r, host, pathValue)
		return
	}

	if strings.HasSuffix(pathValue, "/snapshot.tar.zst") {
		s.metrics.recordRequest(ctx, "snapshot")
		s.handleSnapshotRequest(w, r, host, pathValue)
		return
	}

	if strings.HasSuffix(pathValue, "/snapshot.bundle") {
		s.metrics.recordRequest(ctx, "bundle")
		s.handleBundleRequest(w, r, host, pathValue)
		return
	}

	if strings.HasSuffix(pathValue, "/lfs-snapshot.tar.zst") {
		s.metrics.recordRequest(ctx, "lfs-snapshot")
		s.handleLFSSnapshotRequest(w, r, host, pathValue)
		return
	}

	service := r.URL.Query().Get("service")
	isReceivePack := service == "git-receive-pack" || strings.HasSuffix(pathValue, "/git-receive-pack")

	if isReceivePack {
		s.metrics.recordRequest(ctx, "receive-pack")
		logger.DebugContext(ctx, "Forwarding write operation to upstream")
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	// Only handle known git smart protocol operations locally (info/refs
	// discovery and git-upload-pack negotiation). Everything else (LFS API
	// requests, unknown paths, etc.) is forwarded to upstream so it isn't
	// mistakenly treated as a clone/fetch.
	if isGitRequest(pathValue) {
		s.handleGitRequest(w, r, host, pathValue)
		return
	}

	s.metrics.recordRequest(ctx, "forward")
	logger.DebugContext(ctx, "Forwarding non-git request to upstream", "uri", pathValue)
	s.forwardToUpstream(w, r, host, pathValue)
}

func (s *Strategy) handleGitRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(pathValue)
	upstreamURL := "https://" + host + "/" + repoPath

	repo, err := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Increment after GetOrCreate so unvalidated URLs can't bloat the keyspace.
	if isClone, cerr := RequestIsClone(pathValue, r); cerr != nil {
		logger.WarnContext(ctx, "Failed to inspect upload-pack body for clone counting", "error", cerr)
	} else if isClone {
		s.repoCounts.IncrementClone(upstreamURL)
	}

	state := repo.State()
	isInfoRefs := strings.HasSuffix(pathValue, "/info/refs")

	switch state {
	case gitclone.StateReady:
		if err := s.serveReadyRepo(w, r, repo, host, pathValue, isInfoRefs); err != nil {
			logger.ErrorContext(ctx, "Failed to serve from local mirror", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}

	case gitclone.StateCloning, gitclone.StateEmpty:
		if state == gitclone.StateEmpty {
			logger.DebugContext(ctx, "Starting background clone, forwarding to upstream")
			s.scheduler.Submit(repo.UpstreamURL(), "clone", func(ctx context.Context) error {
				return s.startClone(ctx, repo)
			})
		}
		if err := s.serveWithSpool(w, r, host, pathValue, upstreamURL); err != nil {
			logger.WarnContext(ctx, "Spool failed, forwarding to upstream", "error", err)
			s.forwardToUpstream(w, r, host, pathValue)
		}
	}
}

func (s *Strategy) serveReadyRepo(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, host, pathValue string, isInfoRefs bool) error {
	ctx := r.Context()

	stale, _ := s.checkRefsStale(ctx, repo) //nolint:errcheck // best-effort; treat as non-stale on failure
	if isInfoRefs && stale {
		// Mirror is behind upstream. Forward to upstream so the client gets
		// fresh refs immediately, and kick off a background fetch so the
		// mirror catches up for subsequent requests.
		logging.FromContext(ctx).InfoContext(ctx, "Refs stale, forwarding to upstream and fetching in background", "upstream", repo.UpstreamURL())
		s.submitFetch(repo)
		s.forwardToUpstream(w, r, host, pathValue)
		return nil
	}

	// Buffer the request body so it can be replayed if serveFromBackend
	// signals a fallback to upstream (e.g. on "not our ref").
	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody {
		var readErr error
		bodyBytes, readErr = io.ReadAll(r.Body)
		if readErr != nil {
			return errors.Wrap(readErr, "read request body")
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		r.ContentLength = int64(len(bodyBytes))
		r.TransferEncoding = nil
	}

	if s.serveFromBackend(w, r, repo) {
		// The mirror is missing the requested object — most likely a commit
		// that was advertised before a concurrent force-push fetch orphaned
		// it. Fall back to upstream so the client is not left with an error.
		logging.FromContext(ctx).InfoContext(ctx, "Falling back to upstream due to 'not our ref'", "path", pathValue)
		if bodyBytes != nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			r.ContentLength = int64(len(bodyBytes))
			r.TransferEncoding = nil
		}
		s.forwardToUpstream(w, r, host, pathValue)
	}
	return nil
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

func (s *Strategy) serveWithSpool(w http.ResponseWriter, r *http.Request, host, pathValue, upstreamURL string) error {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	key, err := SpoolKeyForRequest(pathValue, r)
	if err != nil {
		return errors.Wrap(err, "compute spool key")
	}
	if key == "" {
		s.forwardToUpstream(w, r, host, pathValue)
		return nil
	}

	rp, err := s.getOrCreateRepoSpools(upstreamURL)
	if err != nil {
		return errors.Wrap(err, "resolve spool directory")
	}
	spool, isWriter, err := rp.GetOrCreate(key)
	if err != nil {
		return errors.Wrap(err, "create spool")
	}

	if isWriter {
		logger.DebugContext(ctx, "Spooling upstream response", "key", key, "upstream", upstreamURL)
		tw := NewSpoolTeeWriter(w, spool)
		s.forwardToUpstream(tw, r, host, pathValue)
		spool.MarkComplete()
		return nil
	}

	if spool.Failed() {
		logger.DebugContext(ctx, "Spool failed, forwarding to upstream", "key", key)
		s.forwardToUpstream(w, r, host, pathValue)
		return nil
	}

	logger.DebugContext(ctx, "Serving from spool", "key", key, "upstream", upstreamURL)
	if err := spool.ServeTo(w); err != nil {
		if errors.Is(err, ErrSpoolFailed) {
			logger.DebugContext(ctx, "Spool failed before response started, forwarding to upstream", "key", key)
			s.forwardToUpstream(w, r, host, pathValue)
			return nil
		}
		return errors.Wrapf(err, "spool read failed mid-stream for key %s", key)
	}
	return nil
}

// isGitRequest reports whether pathValue matches a git smart HTTP protocol
// endpoint (info/refs discovery or git-upload-pack negotiation).
func isGitRequest(pathValue string) bool {
	return strings.HasSuffix(pathValue, "/info/refs") ||
		strings.HasSuffix(pathValue, "/git-upload-pack")
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
		if err := s.startClone(ctx, repo); err != nil {
			return err
		}
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

func (s *Strategy) startClone(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	// Atomically claim the clone so only one goroutine performs the restore
	// or clone. Without this gate, concurrent snapshot requests each call
	// startClone and extract tarballs over the same directory, corrupting
	// packed-refs and other git metadata.
	if !repo.TryStartCloning() {
		return nil
	}

	ctx, span := tracer.Start(ctx, "git.start_clone",
		trace.WithAttributes(
			attribute.String("cachew.operation", "clone"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Attempting mirror snapshot restore", "upstream", upstream)

	if err := s.tryRestoreSnapshot(ctx, repo); err != nil {
		logger.InfoContext(ctx, "Mirror snapshot restore failed, falling back to clone", "upstream", upstream, "error", err)
	} else {
		logger.InfoContext(ctx, "Mirror snapshot restored, fetching to freshen", "upstream", upstream)

		// Fetch with a generous timeout and no low-speed check: mirror
		// snapshots can be hours old, so the delta may be very large and
		// GitHub's server-side pack computation can stall at near-zero
		// transfer for minutes (same as initial clone).
		//
		// State remains StateCloning until fetch succeeds so that
		// concurrent requests (via ensureCloneReady) block rather than
		// serving from a potentially empty or stale mirror.
		if err := repo.FetchLenient(ctx, s.cloneManager.Config().CloneTimeout); err != nil {
			logger.WarnContext(ctx, "Post-restore fetch failed, discarding snapshot and falling back to clone",
				"upstream", upstream, "error", err)
			// The restored snapshot may be corrupt or empty. Remove it and
			// fall through to a fresh clone so we don't re-upload bad data.
			repo.ResetToEmpty()
			if rmErr := os.RemoveAll(repo.Path()); rmErr != nil {
				return errors.Wrapf(rmErr, "remove corrupt mirror for %s", upstream)
			}
		} else {
			repo.MarkReady()

			if err := s.cleanupSpools(upstream); err != nil {
				return errors.Wrapf(err, "clean up spools for %s", upstream)
			}

			logger.InfoContext(ctx, "Post-restore fetch completed, serving", "upstream", upstream)

			if s.config.SnapshotInterval > 0 {
				s.scheduleSnapshotJobs(repo)
			}
			if s.repackEnabled() {
				s.scheduleRepackJobs(repo)
			}
			return nil
		}
	}

	logger.InfoContext(ctx, "Starting clone", "upstream", upstream, "path", repo.Path())

	cloneStart := time.Now()
	err := repo.Clone(ctx)

	// Clean up spools regardless of clone success or failure, so that subsequent
	// requests either serve from the local backend or go directly to upstream.
	if cleanupErr := s.cleanupSpools(upstream); cleanupErr != nil {
		return errors.Wrapf(cleanupErr, "clean up spools for %s", upstream)
	}

	if err != nil {
		s.metrics.recordOperation(ctx, "clone", "error", time.Since(cloneStart))
		repo.ResetToEmpty()
		return errors.Wrapf(err, "clone %s", upstream)
	}

	s.metrics.recordOperation(ctx, "clone", "success", time.Since(cloneStart))
	logger.InfoContext(ctx, "Clone completed", "upstream", upstream, "path", repo.Path())

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
	if s.repackEnabled() {
		s.scheduleRepackJobs(repo)
	}
	return nil
}

// tryRestoreSnapshot attempts to restore a mirror from an S3 mirror snapshot.
// Mirror snapshots are bare repositories that can be extracted and used directly
// without any conversion. The snapshot is extracted into a temporary directory
// and renamed into place only on success, so a failure can never delete an
// existing mirror directory.
func (s *Strategy) tryRestoreSnapshot(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	ctx, span := tracer.Start(ctx, "git.restore_snapshot",
		trace.WithAttributes(
			attribute.String("cachew.operation", "restore"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	cacheKey := mirrorSnapshotCacheKey(repo.UpstreamURL())

	parentDir := filepath.Dir(repo.Path())
	if err := os.MkdirAll(parentDir, 0o750); err != nil {
		return errors.Wrap(err, "create parent directory for restore")
	}

	tmpDir, err := os.MkdirTemp(parentDir, ".restore-*")
	if err != nil {
		return errors.Wrap(err, "create temp restore directory")
	}
	defer os.RemoveAll(tmpDir) //nolint:errcheck // best-effort cleanup on failure

	restoreDest := filepath.Join(tmpDir, "repo")

	logger := logging.FromContext(ctx)

	if err := snapshot.Restore(ctx, s.cache, cacheKey, restoreDest, s.config.ZstdThreads); err != nil {
		return errors.Wrap(err, "restore mirror snapshot")
	}
	logger.InfoContext(ctx, "Mirror snapshot extracted", "upstream", repo.UpstreamURL(), "path", restoreDest)

	if err := repo.ConfigureMirror(ctx, restoreDest); err != nil {
		return errors.Wrap(err, "configure restored mirror")
	}

	if err := os.Rename(restoreDest, repo.Path()); err != nil {
		return errors.Wrap(err, "move restored snapshot into place")
	}

	logger.InfoContext(ctx, "Repository restored from snapshot", "upstream", repo.UpstreamURL())
	return nil
}

// submitFetch schedules a fetch unconditionally. Use this when ls-remote has
// already confirmed the mirror is behind upstream.
func (s *Strategy) submitFetch(repo *gitclone.Repository) {
	// Use a separate queue from snapshot/repack so fetches are not serialized
	// behind long-running jobs on the same upstream URL queue.
	s.scheduler.Submit(repo.UpstreamURL()+"/fetch", "fetch", func(ctx context.Context) error {
		return s.doFetch(ctx, repo)
	})
}

func (s *Strategy) doFetch(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	ctx, span := tracer.Start(ctx, "git.fetch",
		trace.WithAttributes(
			attribute.String("cachew.operation", "fetch"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Fetching updates", "upstream", repo.UpstreamURL(), "path", repo.Path())

	start := time.Now()
	if err := repo.Fetch(ctx); err != nil {
		s.metrics.recordOperation(ctx, "fetch", "error", time.Since(start))
		return errors.Errorf("fetch failed: %w", err)
	}
	s.metrics.recordOperation(ctx, "fetch", "success", time.Since(start))
	logger.InfoContext(ctx, "Fetch completed", "upstream", repo.UpstreamURL(), "duration", time.Since(start))
	return nil
}
