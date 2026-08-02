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
	cachewhttputil "github.com/block/cachew/internal/httputil"
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
	SnapshotMaxAge         time.Duration `hcl:"snapshot-max-age,optional" help:"How long an unchanged snapshot (same HEAD commit) may be served before regeneration. Requires shared metadata; keep well below the cache max-ttl. 0 regenerates every interval." default:"24h"`
	MirrorSnapshotInterval time.Duration `hcl:"mirror-snapshot-interval,optional" help:"How often to generate mirror snapshots for pod bootstrap. 0 uses snapshot-interval. Defaults to 2h." default:"2h"`
	RepackInterval         time.Duration `hcl:"repack-interval,optional" help:"How often to run full repack. 0 disables." default:"0"`
	ZstdThreads            int           `hcl:"zstd-threads,optional" help:"Threads for zstd compression/decompression. 0 = all CPU cores; useful for short-lived CLI invocations but risky on a long-running server where multiple snapshot/restore operations can run concurrently." default:"4"`
	BundleCacheTTL         time.Duration `hcl:"bundle-cache-ttl,optional" help:"TTL of cached server-side git bundles." default:"2h"`

	SnapshotFilters         map[string]string `hcl:"snapshot-filters,optional" help:"Per-repository git partial-clone filter applied to workstation snapshots, keyed by host/org/repo (e.g. {\"github.com/org/repo\": \"blob:none\"}). Filtered snapshots contain full history metadata but only the blobs needed for the HEAD checkout; clients lazily fetch historical blobs through cachew on demand."`
	IncrementalPullthrough  bool              `hcl:"incremental-pullthrough,optional" help:"Serve partially-cached upload-pack requests by fetching only missing objects into the local mirror instead of proxying the full response from upstream. When enabled, upload-pack bodies are capped at 8 MiB (2× the parse limit); larger bodies receive 413." default:"true"`
	IncrementalFetchTimeout time.Duration     `hcl:"incremental-fetch-timeout,optional" help:"Upper bound on the synchronous request-path fetch performed by incremental pull-through. The coalescing and verified attempts share this budget, and the coalescing attempt is capped at half so a verified retry still has room. On timeout the request falls back to upstream. Each underlying git fetch also honors fetch-timeout, and the preceding ref-staleness check is bounded separately by ls-remote-timeout." default:"2m"`
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
	forcedFetches       sync.Map // keyed by upstream URL, ensures at most one cooldown-bypassing fetch is queued per repo
	metrics             *gitMetrics
	repoCounts          *RepoCounts
	snapshotCoord       *SnapshotCoordinator
	metadataWired       chan struct{} // closed by SetMetadataStore; gates warm-up
	wiredOnce           sync.Once
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
	if config.IncrementalFetchTimeout == 0 {
		config.IncrementalFetchTimeout = 2 * time.Minute
	}
	if config.IncrementalFetchTimeout < 0 {
		return nil, errors.Errorf("incremental-fetch-timeout must be positive, got %v", config.IncrementalFetchTimeout)
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
		config:        config,
		cache:         cache,
		cloneManager:  cloneManager,
		httpClient:    http.DefaultClient,
		ctx:           ctx,
		scheduler:     scheduler.WithQueuePrefix("git"),
		spools:        make(map[string]*RepoSpools),
		tokenManager:  tokenManager,
		metrics:       m,
		metadataWired: make(chan struct{}),
	}
	// Run startup fetches in the background so the HTTP listener (and
	// /_liveness) come up immediately. /_readiness gates on Ready() so the
	// Service load balancer holds traffic until warming completes.
	go func() {
		warmCtx := context.WithoutCancel(ctx)
		// Coordination only matters when warm-up will schedule snapshot
		// jobs; with snapshots disabled, warm immediately.
		if s.config.SnapshotInterval > 0 {
			// Wait for SetMetadataStore so warm-up never schedules snapshot
			// jobs before cross-replica coordination is installed.
			// config.Load wires every MetadataConsumer immediately after
			// construction.
			select {
			case <-s.metadataWired:
			case <-ctx.Done():
				return
			}
			// Sync shared coordination state before scheduling so the first
			// claim decisions don't run against an empty local view. Bounded
			// and fail-open (coordination is advisory) so a slow metadata
			// backend can't wedge readiness.
			primeCtx, cancel := context.WithTimeout(warmCtx, time.Minute)
			if err := s.snapshotCoord.Prime(primeCtx); err != nil {
				logger.WarnContext(warmCtx, "Failed to prime snapshot coordination state", "error", err)
			}
			cancel()
		}
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

	logger.InfoContext(ctx, "Git strategy initialized", "snapshot_interval", config.SnapshotInterval,
		"incremental_pullthrough", config.IncrementalPullthrough,
		"incremental_fetch_timeout", config.IncrementalFetchTimeout)

	return s, nil
}

var _ strategy.Strategy = (*Strategy)(nil)
var _ strategy.Readier = (*Strategy)(nil)
var _ strategy.MetadataConsumer = (*Strategy)(nil)

// Ready reports whether startup warm-up has completed.
func (s *Strategy) Ready() bool {
	return s.ready.Load()
}

// SetMetadataStore enables the per-repo clone histogram (and schedules its
// daily reaper) and cross-replica snapshot generation coordination. Called by
// config.Load after the metadata backend is built. It also releases the
// warm-up goroutine, which waits for wiring so warmed repos are scheduled
// with coordination in place.
func (s *Strategy) SetMetadataStore(store *metadatadb.Store) {
	defer s.wiredOnce.Do(func() { close(s.metadataWired) })
	if store == nil {
		return
	}
	s.snapshotCoord = NewSnapshotCoordinator(store.Namespace("git"))
	s.repoCounts = NewRepoCounts(store.Namespace("git"))
	logging.FromContext(s.ctx).InfoContext(s.ctx, "Per-repo clone histogram enabled",
		"retention_days", s.repoCounts.retentionDays)
	s.scheduler.SubmitPeriodicJob("repo-counts-reaper", "reap-repo-counts", defaultRepoCountsReapInterval, func(ctx context.Context) error {
		deleted, err := s.repoCounts.Reap()
		if err != nil {
			return err
		}
		if deleted > 0 {
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
		if s.config.RepackInterval > 0 {
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

	// Only incremental pull-through buffers and parses upload-pack bodies, so the
	// body cap belongs behind the same kill switch: with incremental disabled the
	// body streams straight through and a legitimately large one must not start
	// failing with 413.
	if s.config.IncrementalPullthrough && isUploadPackPost(r, pathValue) &&
		r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(w, r.Body, 2*uploadPackParseLimit)
	}

	// Increment after GetOrCreate so unvalidated URLs can't bloat the keyspace.
	if isClone, cerr := RequestIsClone(pathValue, r); cerr != nil {
		logger.WarnContext(ctx, "Failed to inspect upload-pack body for clone counting", "error", cerr)
	} else if isClone {
		if err := s.repoCounts.IncrementClone(upstreamURL); err != nil {
			logger.WarnContext(ctx, "Failed to increment repo clone count", "error", err)
		}
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
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				cachewhttputil.ErrorResponse(w, r, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			logger.WarnContext(ctx, "Spool failed, forwarding to upstream", "error", err)
			s.forwardToUpstream(w, r, host, pathValue)
		}
	}
}

func (s *Strategy) serveReadyRepo(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, host, pathValue string, isInfoRefs bool) error {
	ctx := r.Context()
	// Started at handler entry so the incremental duration metric covers the whole
	// latency this handler adds on the request path.
	incrementalStart := time.Now()

	if isInfoRefs && s.checkRefsStale(ctx, repo) {
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
			var maxErr *http.MaxBytesError
			if errors.As(readErr, &maxErr) {
				cachewhttputil.ErrorResponse(w, r, http.StatusRequestEntityTooLarge, "request body too large")
				return nil
			}
			return errors.Wrap(readErr, "read request body")
		}
		replayRequestBody(r, bodyBytes)
	}

	// incrementalParse has no side effects, so wants are computed even when the
	// feature is off. That lets the eligibility metric below cover
	// config-disabled traffic.
	wants, wantRefs := incrementalParse(r, pathValue, bodyBytes)
	// Use host/repoPath (matching sibling git metrics) rather than the full
	// upstream URL so the repository attribute joins across metrics.
	incrementalRepo := host + "/" + ExtractRepoPath(pathValue)
	if isUploadPackPost(r, pathValue) {
		s.metrics.recordIncrementalEligible(ctx, len(wants) > 0, incrementalRepo)
	}
	// Outcome metrics are recorded only once the serve outcome is known: a request
	// the incremental path cleared can still end as a full upstream passthrough on
	// "not our ref", which must not count as a local_hit or fetched success.
	// incrementalOutcome stays empty when incremental did not run.
	var (
		incrementalOutcome  string
		incrementalDuration time.Duration
	)
	recordIncremental := func(outcome string) {
		s.metrics.recordIncrementalServe(ctx, outcome, incrementalRepo)
		s.metrics.recordIncrementalFetchDuration(ctx, outcome, incrementalRepo, incrementalDuration)
	}

	// A want-ref body asks the mirror to resolve ref names locally. Mirrors do not
	// advertise ref-in-want, so their upload-pack cannot serve such a body at all.
	// Forwarding is also safer: local ref-name resolution can serve a stale tip and
	// regress the client's FETCH_HEAD, and nothing on this path proves the mirror's
	// refs are current.
	//
	// Not gated on IncrementalPullthrough: what the mirror advertises comes from
	// its on-disk config, not the flag.
	if wantRefs {
		logging.FromContext(ctx).InfoContext(ctx, "Forwarding want-ref request: mirror does not serve ref-in-want", "path", pathValue)
		s.forwardWithBody(w, r, host, pathValue, bodyBytes)
		return nil
	}

	if s.config.IncrementalPullthrough && len(wants) > 0 {
		outcome, herr := s.ensureWantsAvailable(ctx, repo, wants)
		// The incremental latency ends with the fetch, so capture the duration here
		// even though the outcome is recorded after the local serve.
		incrementalOutcome, incrementalDuration = outcome, time.Since(incrementalStart)
		if herr != nil || outcome == incrementalFallbackMissing {
			logging.FromContext(ctx).InfoContext(ctx, "Incremental pull-through falling back to upstream",
				"outcome", outcome, "error", herr, "path", pathValue)
			// A fetch failure (or a client disconnect before the verified retry)
			// leaves the mirror cold; kick a background catch-up so subsequent
			// requests do not each pay the synchronous timeout. The forced
			// variant is required because the failed attempt itself satisfies
			// NeedsFetch's cooldown.
			if outcome == incrementalFallbackFetchFailed || outcome == incrementalClientGone {
				s.submitFetchForce(repo)
			}
			recordIncremental(outcome)
			s.forwardWithBody(w, r, host, pathValue, bodyBytes)
			return nil
		}
	}

	if s.serveFromBackend(w, r, repo) {
		// The mirror is missing the requested object — most likely a commit
		// that was advertised before a concurrent force-push fetch orphaned
		// it. Fall back to upstream so the client is not left with an error.
		if incrementalOutcome != "" {
			incrementalOutcome = incrementalOutcomeAfterNotOurRef(incrementalOutcome)
		}
		logging.FromContext(ctx).InfoContext(ctx, "Falling back to upstream due to 'not our ref'", "path", pathValue)
		s.forwardWithBody(w, r, host, pathValue, bodyBytes)
	}
	if incrementalOutcome != "" {
		recordIncremental(incrementalOutcome)
	}
	return nil
}

// replayRequestBody replaces r.Body with a fresh reader over bodyBytes so the
// request can be re-served (local backend or upstream forward) after buffering
// consumed the body.
func replayRequestBody(r *http.Request, bodyBytes []byte) {
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.ContentLength = int64(len(bodyBytes))
	r.TransferEncoding = nil
}

// forwardWithBody replays bodyBytes onto r (when non-nil) and forwards to upstream.
func (s *Strategy) forwardWithBody(w http.ResponseWriter, r *http.Request, host, pathValue string, bodyBytes []byte) {
	if bodyBytes != nil {
		replayRequestBody(r, bodyBytes)
	}
	s.forwardToUpstream(w, r, host, pathValue)
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
			if s.config.RepackInterval > 0 {
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
		s.metrics.recordOperation(ctx, "clone", "error", "background", time.Since(cloneStart))
		repo.ResetToEmpty()
		return errors.Wrapf(err, "clone %s", upstream)
	}

	s.metrics.recordOperation(ctx, "clone", "success", "background", time.Since(cloneStart))
	logger.InfoContext(ctx, "Clone completed", "upstream", upstream, "path", repo.Path())

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
	if s.config.RepackInterval > 0 {
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

// submitFetch schedules a background fetch when the mirror may be behind
// upstream and a fetch cooldown has expired.
func (s *Strategy) submitFetch(repo *gitclone.Repository) {
	if !repo.NeedsFetch(s.cloneManager.Config().RefCheckInterval) {
		return
	}
	// Use a separate queue from snapshot/repack so fetches are not serialized
	// behind long-running jobs on the same upstream URL queue.
	s.scheduler.Submit(repo.UpstreamURL()+"/fetch", "fetch", func(ctx context.Context) error {
		return s.doFetch(ctx, repo)
	})
}

// submitFetchForce schedules a background fetch even when the fetch cooldown has
// not expired. Recovery paths need this: executeFetch stamps the attempt before
// the network call, so a fetch that has just failed already satisfies
// NeedsFetch's cooldown, and submitFetch would then do nothing just when the
// mirror needs to catch up.
//
// The scheduler neither dedupes queued jobs nor runs two jobs from one queue
// concurrently, so the fetch semaphore cannot collapse duplicates that are only
// queued. forcedFetches keeps at most one forced fetch pending per repository,
// bounding a burst of failing requests to a single catch-up.
//
// The job fetches verified rather than coalescing: a coalescing fetch that
// merely waits on a non-fetching semaphore holder, such as a snapshot tar,
// returns success without contacting upstream, which would clear forcedFetches
// and leave the mirror exactly as cold as the failure that queued the job.
func (s *Strategy) submitFetchForce(repo *gitclone.Repository) {
	key := repo.UpstreamURL()
	if _, pending := s.forcedFetches.LoadOrStore(key, struct{}{}); pending {
		return
	}
	s.scheduler.Submit(key+"/fetch", "fetch", func(ctx context.Context) error {
		defer s.forcedFetches.Delete(key)
		return s.doFetchVerified(ctx, repo, "background")
	})
}

// freshenMirror synchronously fetches the mirror unless it already fetched
// within the ref-check interval, bounding upstream load when fallback bundle
// requests repeat against the same repository.
func (s *Strategy) freshenMirror(ctx context.Context, repo *gitclone.Repository) error {
	if !repo.NeedsFetch(s.cloneManager.Config().RefCheckInterval) {
		return nil
	}
	return errors.WithStack(s.doFetch(ctx, repo))
}

func (s *Strategy) doFetch(ctx context.Context, repo *gitclone.Repository) error {
	return s.fetchMirror(ctx, repo, repo.Fetch, "background")
}

// doFetchReporting is doFetch plus a flag reporting whether this call ran a git
// fetch (true) or coalesced onto another semaphore holder (false), so the
// incremental path can skip a redundant verified fetch after a real one.
// Operation metrics and the success log are recorded only for a real fetch; a
// coalesce wait must not inflate the request-path fetch SLO, and that holds for
// failures too: a coalesce wait can end in error without ever contacting
// upstream, so only an attempted fetch records an error.
func (s *Strategy) doFetchReporting(ctx context.Context, repo *gitclone.Repository) (fetched bool, returnErr error) {
	ctx, span := tracer.Start(ctx, "git.fetch",
		trace.WithAttributes(
			attribute.String("cachew.operation", "fetch"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
			attribute.String("cachew.trigger", "incremental"),
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
	start := time.Now()
	attempted, err := repo.FetchCoalescing(ctx)
	if err != nil {
		if attempted {
			s.metrics.recordOperation(ctx, "fetch", "error", "incremental", time.Since(start))
		}
		return false, errors.Errorf("fetch failed: %w", err)
	}
	if !attempted {
		span.SetAttributes(attribute.Bool("cachew.coalesced", true))
		return false, nil
	}
	s.metrics.recordOperation(ctx, "fetch", "success", "incremental", time.Since(start))
	logger.InfoContext(ctx, "Fetch completed", "upstream", repo.UpstreamURL(), "duration", time.Since(start))
	return true, nil
}

// doFetchVerifiedIfAbsent waits for the fetch semaphore and runs a git fetch only
// when wants are still missing after the coalescing attempt. As with
// doFetchReporting, only a call that actually ran a fetch records an operation
// metric; giving up while waiting for the semaphore never contacted upstream.
func (s *Strategy) doFetchVerifiedIfAbsent(ctx context.Context, repo *gitclone.Repository, wants []string) (returnErr error) {
	ctx, span := tracer.Start(ctx, "git.fetch",
		trace.WithAttributes(
			attribute.String("cachew.operation", "fetch"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
			attribute.String("cachew.trigger", "incremental"),
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
	start := time.Now()
	attempted, err := repo.FetchVerifiedIfAbsent(ctx, wants)
	if err != nil {
		if attempted {
			s.metrics.recordOperation(ctx, "fetch", "error", "incremental", time.Since(start))
		}
		return errors.Errorf("fetch failed: %w", err)
	}
	if !attempted {
		span.SetAttributes(attribute.Bool("cachew.coalesced", true))
		return nil
	}
	s.metrics.recordOperation(ctx, "fetch", "success", "incremental", time.Since(start))
	logger.InfoContext(ctx, "Fetch completed", "upstream", repo.UpstreamURL(), "duration", time.Since(start))
	return nil
}

// doFetchVerified is doFetch minus fetch coalescing: nil guarantees this call
// ran a successful git fetch, so the caller can assert upstream state. trigger
// is "incremental" when invoked from the request path and "background" otherwise.
func (s *Strategy) doFetchVerified(ctx context.Context, repo *gitclone.Repository, trigger string) error {
	return s.fetchMirror(ctx, repo, repo.FetchVerified, trigger)
}

func (s *Strategy) fetchMirror(ctx context.Context, repo *gitclone.Repository, fetch func(context.Context) error, trigger string) (returnErr error) {
	ctx, span := tracer.Start(ctx, "git.fetch",
		trace.WithAttributes(
			attribute.String("cachew.operation", "fetch"),
			attribute.String("cachew.upstream", repo.UpstreamURL()),
			attribute.String("cachew.trigger", trigger),
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
	if err := fetch(ctx); err != nil {
		s.metrics.recordOperation(ctx, "fetch", "error", trigger, time.Since(start))
		return errors.Errorf("fetch failed: %w", err)
	}
	s.metrics.recordOperation(ctx, "fetch", "success", trigger, time.Since(start))
	logger.InfoContext(ctx, "Fetch completed", "upstream", repo.UpstreamURL(), "duration", time.Since(start))
	return nil
}
