// Package git implements a protocol-aware Git caching proxy strategy.
package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
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
	SnapshotInterval time.Duration `hcl:"snapshot-interval,optional" help:"How often to generate tar.zstd snapshots. 0 disables snapshots." default:"0"`
	RepackInterval   time.Duration `hcl:"repack-interval,optional" help:"How often to run full repack. 0 disables." default:"0"`
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
		logger.WarnContext(ctx, "Failed to discover existing clones",
			slog.String("error", err.Error()))
	}
	for _, repo := range existing {
		logger.InfoContext(ctx, "Running startup fetch for existing repo",
			slog.String("upstream", repo.UpstreamURL()))

		preRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get pre-fetch refs for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.String("error", err.Error()))
		} else {
			logger.InfoContext(ctx, "Pre-fetch refs for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.Any("refs", preRefs))
		}

		start := time.Now()
		if err := repo.Fetch(ctx); err != nil {
			logger.ErrorContext(ctx, "Startup fetch failed for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.String("error", err.Error()),
				slog.Duration("duration", time.Since(start)))
		} else {
			logger.InfoContext(ctx, "Startup fetch completed for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.Duration("duration", time.Since(start)))
		}

		postRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get post-fetch refs for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.String("error", err.Error()))
		} else {
			logger.InfoContext(ctx, "Post-fetch refs for existing repo",
				slog.String("upstream", repo.UpstreamURL()),
				slog.Any("refs", postRefs))
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
						logger.DebugContext(req.Context(), "Injecting GitHub App auth into upstream request",
							slog.String("org", org))
					}
				}
			}
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

	logger.DebugContext(ctx, "Git request",
		slog.String("method", r.Method),
		slog.String("host", host),
		slog.String("path", pathValue))

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
		logger.ErrorContext(ctx, "Failed to get or create clone",
			slog.String("error", err.Error()))
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
		s.ensureRefsUpToDate(ctx, repo)
	}
	s.maybeBackgroundFetch(repo)

	// Buffer the request body so it can be replayed if serveFromBackend
	// signals a fallback to upstream (e.g. on "not our ref").
	var bodyBytes []byte
	if r.Body != nil && r.Body != http.NoBody {
		var readErr error
		bodyBytes, readErr = io.ReadAll(r.Body)
		if readErr != nil {
			logger.ErrorContext(ctx, "Failed to read request body",
				slog.String("error", readErr.Error()))
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
		logger.InfoContext(ctx, "Falling back to upstream due to 'not our ref'",
			slog.String("path", pathValue))
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
				slog.String("upstream", upstreamURL),
				slog.String("error", err.Error()))
		}
	}
}

func (s *Strategy) serveWithSpool(w http.ResponseWriter, r *http.Request, host, pathValue, upstreamURL string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	key, err := SpoolKeyForRequest(pathValue, r)
	if err != nil {
		logger.WarnContext(ctx, "Failed to compute spool key, forwarding to upstream",
			slog.String("error", err.Error()))
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}
	if key == "" {
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	rp, err := s.getOrCreateRepoSpools(upstreamURL)
	if err != nil {
		logger.WarnContext(ctx, "Failed to resolve spool directory, forwarding to upstream",
			slog.String("error", err.Error()))
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}
	spool, isWriter, err := rp.GetOrCreate(key)
	if err != nil {
		logger.WarnContext(ctx, "Failed to create spool, forwarding to upstream",
			slog.String("error", err.Error()))
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	if isWriter {
		logger.DebugContext(ctx, "Spooling upstream response",
			slog.String("key", key),
			slog.String("upstream", upstreamURL))
		tw := NewSpoolTeeWriter(w, spool)
		s.forwardToUpstream(tw, r, host, pathValue)
		spool.MarkComplete()
		return
	}

	if spool.Failed() {
		logger.DebugContext(ctx, "Spool failed, forwarding to upstream",
			slog.String("key", key))
		s.forwardToUpstream(w, r, host, pathValue)
		return
	}

	logger.DebugContext(ctx, "Serving from spool",
		slog.String("key", key),
		slog.String("upstream", upstreamURL))
	if err := spool.ServeTo(w); err != nil {
		if errors.Is(err, ErrSpoolFailed) {
			logger.DebugContext(ctx, "Spool failed before response started, forwarding to upstream",
				slog.String("key", key))
			s.forwardToUpstream(w, r, host, pathValue)
			return
		}
		logger.WarnContext(ctx, "Spool read failed mid-stream",
			slog.String("key", key),
			slog.String("error", err.Error()))
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

	logger.InfoContext(ctx, "Attempting snapshot restore",
		slog.String("upstream", upstream))

	if err := s.tryRestoreSnapshot(ctx, repo); err != nil {
		logger.InfoContext(ctx, "Snapshot restore failed, falling back to clone",
			slog.String("upstream", upstream),
			slog.String("error", err.Error()))
	} else {
		s.cleanupSpools(upstream)

		logger.InfoContext(ctx, "Snapshot restored, running synchronous catch-up fetch",
			slog.String("upstream", upstream),
			slog.String("state", repo.State().String()))

		preRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get pre-fetch refs",
				slog.String("upstream", upstream),
				slog.String("error", err.Error()))
		} else {
			logger.InfoContext(ctx, "Pre-fetch refs",
				slog.String("upstream", upstream),
				slog.Any("refs", preRefs))
		}

		start := time.Now()
		if err := repo.Fetch(ctx); err != nil {
			logger.ErrorContext(ctx, "Catch-up fetch after snapshot restore failed",
				slog.String("upstream", upstream),
				slog.String("error", err.Error()),
				slog.Duration("duration", time.Since(start)))
		} else {
			logger.InfoContext(ctx, "Catch-up fetch after snapshot restore completed",
				slog.String("upstream", upstream),
				slog.Duration("duration", time.Since(start)))
		}

		postRefs, err := repo.GetLocalRefs(ctx)
		if err != nil {
			logger.WarnContext(ctx, "Failed to get post-fetch refs",
				slog.String("upstream", upstream),
				slog.String("error", err.Error()))
		} else {
			logger.InfoContext(ctx, "Post-fetch refs",
				slog.String("upstream", upstream),
				slog.Any("refs", postRefs))
		}

		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
		if s.config.RepackInterval > 0 {
			s.scheduleRepackJobs(repo)
		}
		return
	}

	logger.InfoContext(ctx, "Starting clone",
		slog.String("upstream", upstream),
		slog.String("path", repo.Path()))

	err := repo.Clone(ctx)

	// Clean up spools regardless of clone success or failure, so that subsequent
	// requests either serve from the local backend or go directly to upstream.
	s.cleanupSpools(upstream)

	if err != nil {
		logger.ErrorContext(ctx, "Clone failed",
			slog.String("upstream", upstream),
			slog.String("error", err.Error()))
		return
	}

	logger.InfoContext(ctx, "Clone completed",
		slog.String("upstream", upstream),
		slog.String("path", repo.Path()))

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
	if s.config.RepackInterval > 0 {
		s.scheduleRepackJobs(repo)
	}
}

// tryRestoreSnapshot attempts to restore a mirror from an S3 snapshot.
// On failure the repo path is cleaned up so the caller can fall back to clone.
//
// Snapshots are non-bare clones with remote.origin.url pointing to the cachew
// server (so end-user git pulls go through the proxy). When restoring into the
// mirror path we must fix the remote URL back to the real upstream and convert
// the repo to bare, otherwise backgroundFetch would loop back to cachew itself.
func (s *Strategy) tryRestoreSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	cacheKey := snapshotCacheKey(repo.UpstreamURL())

	if err := os.MkdirAll(filepath.Dir(repo.Path()), 0o750); err != nil {
		return errors.Wrap(err, "create parent directory for restore")
	}

	logger := logging.FromContext(ctx)

	if err := snapshot.Restore(ctx, s.cache, cacheKey, repo.Path(), s.config.ZstdThreads); err != nil {
		_ = os.RemoveAll(repo.Path())
		return errors.Wrap(err, "restore snapshot")
	}
	logger.InfoContext(ctx, "Snapshot archive extracted",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("path", repo.Path()))

	if err := convertSnapshotToMirror(ctx, repo.Path(), repo.UpstreamURL()); err != nil {
		_ = os.RemoveAll(repo.Path())
		return errors.Wrap(err, "convert snapshot to mirror")
	}
	logger.InfoContext(ctx, "Snapshot converted to bare mirror",
		slog.String("upstream", repo.UpstreamURL()))

	if err := repo.MarkRestored(ctx); err != nil {
		_ = os.RemoveAll(repo.Path())
		return errors.Wrap(err, "mark restored")
	}
	logger.InfoContext(ctx, "Repository marked as restored",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("state", repo.State().String()))

	return nil
}

// convertSnapshotToMirror converts a restored non-bare snapshot into a bare
// mirror suitable for serving upload-pack. It resets remote.origin.url to the
// real upstream and sets core.bare = true.
func convertSnapshotToMirror(ctx context.Context, repoPath, upstreamURL string) error {
	// The snapshot is a non-bare clone (.git subdir). Detect this by checking
	// for the .git directory and, if present, relocate it to a bare layout.
	dotGit := filepath.Join(repoPath, ".git")
	if info, err := os.Stat(dotGit); err == nil && info.IsDir() {
		if err := convertToBare(repoPath, dotGit); err != nil {
			return err
		}
	}

	// Reset the remote URL to the actual upstream.
	// #nosec G204 - repoPath and upstreamURL are controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "remote", "set-url", "origin", upstreamURL)
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "set remote URL: %s", string(output))
	}

	// Mark the repo as bare so git treats it as a mirror.
	// #nosec G204 - repoPath is controlled by us
	cmd = exec.CommandContext(ctx, "git", "-C", repoPath, "config", "core.bare", "true")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "set core.bare: %s", string(output))
	}

	return nil
}

// convertToBare moves the contents of .git/ up into repoPath, removing the
// working tree, so the directory becomes a bare repository.
func convertToBare(repoPath, dotGit string) error {
	entries, err := os.ReadDir(dotGit)
	if err != nil {
		return errors.Wrap(err, "read .git directory")
	}

	// Remove working tree files (everything except .git).
	topEntries, err := os.ReadDir(repoPath)
	if err != nil {
		return errors.Wrap(err, "read repo directory")
	}
	for _, e := range topEntries {
		if e.Name() == ".git" {
			continue
		}
		_ = os.RemoveAll(filepath.Join(repoPath, e.Name()))
	}

	// Move .git/* contents up to repoPath.
	for _, e := range entries {
		src := filepath.Join(dotGit, e.Name())
		dst := filepath.Join(repoPath, e.Name())
		if err := os.Rename(src, dst); err != nil {
			return errors.Wrapf(err, "move %s", e.Name())
		}
	}

	return errors.Wrap(os.Remove(dotGit), "remove .git directory")
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

	logger.InfoContext(ctx, "Fetching updates",
		slog.String("upstream", repo.UpstreamURL()),
		slog.String("path", repo.Path()))

	start := time.Now()
	if err := repo.Fetch(ctx); err != nil {
		logger.ErrorContext(ctx, "Fetch failed",
			slog.String("upstream", repo.UpstreamURL()),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		return
	}
	logger.InfoContext(ctx, "Fetch completed",
		slog.String("upstream", repo.UpstreamURL()),
		slog.Duration("duration", time.Since(start)))
}
