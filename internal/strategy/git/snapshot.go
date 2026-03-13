package git

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func snapshotDirForURL(mirrorRoot, upstreamURL string) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve snapshot directory")
	}
	return filepath.Join(mirrorRoot, ".snapshots", repoPath), nil
}

func snapshotCacheKey(upstreamURL string) cache.Key {
	return cache.NewKey(upstreamURL + ".snapshot")
}

func mirrorSnapshotCacheKey(upstreamURL string) cache.Key {
	return cache.NewKey(upstreamURL + ".mirror-snapshot")
}

// remoteURLForSnapshot returns the URL to embed as remote.origin.url in snapshots.
// When a server URL is configured, it returns the cachew URL for the repo so that
// git pull goes through cachew. Otherwise it falls back to the upstream URL.
func (s *Strategy) remoteURLForSnapshot(upstream string) string {
	if s.config.ServerURL == "" {
		return upstream
	}
	repoPath, err := gitclone.RepoPathFromURL(upstream)
	if err != nil {
		return upstream
	}
	return s.config.ServerURL + "/git/" + repoPath
}

// cloneForSnapshot clones the mirror into destDir under repo's read lock,
// then fixes the remote URL to point through cachew (or upstream).
func (s *Strategy) cloneForSnapshot(ctx context.Context, repo *gitclone.Repository, destDir string) error {
	if err := repo.WithReadLock(func() error {
		// #nosec G204 - repo.Path() and destDir are controlled by us
		cmd := exec.CommandContext(ctx, "git", "clone", repo.Path(), destDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "git clone for snapshot: %s", string(output))
		}

		// git clone from a local path sets remote.origin.url to that path; restore it.
		// #nosec G204 - remoteURL is derived from controlled inputs
		cmd = exec.CommandContext(ctx, "git", "-C", destDir, "remote", "set-url", "origin", s.remoteURLForSnapshot(repo.UpstreamURL()))
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "fix snapshot remote URL: %s", string(output))
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Snapshot generation started", "upstream", upstream)

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	mirrorRoot := s.cloneManager.Config().MirrorRoot
	snapshotDir, err := snapshotDirForURL(mirrorRoot, upstream)
	if err != nil {
		return err
	}

	// Clean any previous snapshot working directory.
	if err := os.RemoveAll(snapshotDir); err != nil { //nolint:gosec // snapshotDir is derived from controlled mirrorRoot + upstream URL
		return errors.Wrap(err, "remove previous snapshot dir")
	}
	if err := os.MkdirAll(filepath.Dir(snapshotDir), 0o750); err != nil { //nolint:gosec // snapshotDir is derived from controlled mirrorRoot + upstream URL
		return errors.Wrap(err, "create snapshot parent dir")
	}

	if err := s.cloneForSnapshot(ctx, repo, snapshotDir); err != nil {
		_ = os.RemoveAll(snapshotDir) //nolint:gosec // snapshotDir is derived from controlled mirrorRoot + upstream URL
		return err
	}

	cacheKey := snapshotCacheKey(upstream)
	excludePatterns := []string{"*.lock"}

	err = snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, 0, excludePatterns, s.config.ZstdThreads)

	// Always clean up the snapshot working directory.
	if rmErr := os.RemoveAll(snapshotDir); rmErr != nil { //nolint:gosec // snapshotDir is derived from controlled mirrorRoot + upstream URL
		logger.WarnContext(ctx, "Failed to clean up snapshot dir", "error", rmErr)
	}
	if err != nil {
		return errors.Wrap(err, "create snapshot")
	}

	logger.InfoContext(ctx, "Snapshot generation completed", "upstream", upstream)
	return nil
}

// generateAndUploadMirrorSnapshot creates a snapshot of the bare mirror
// directory itself (not a non-bare clone). The resulting tarball can be
// restored directly as a mirror without any conversion. This is used for
// pod-to-pod bootstrap: a new cachew pod restores the mirror snapshot and
// is immediately ready to serve, with background fetch handling freshening.
func (s *Strategy) generateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Mirror snapshot generation started", "upstream", upstream)

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := mirrorSnapshotCacheKey(upstream)
	excludePatterns := []string{"*.lock"}

	if err := repo.WithReadLock(func() error {
		return snapshot.Create(ctx, s.cache, cacheKey, repo.Path(), 0, excludePatterns, s.config.ZstdThreads)
	}); err != nil {
		return errors.Wrap(err, "create mirror snapshot")
	}

	logger.InfoContext(ctx, "Mirror snapshot generation completed", "upstream", upstream)
	return nil
}

func (s *Strategy) scheduleSnapshotJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "snapshot-periodic", s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadSnapshot(ctx, repo)
	})
	mirrorInterval := s.config.MirrorSnapshotInterval
	if mirrorInterval == 0 {
		mirrorInterval = s.config.SnapshotInterval
	}
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "mirror-snapshot-periodic", mirrorInterval, func(ctx context.Context) error {
		return s.generateAndUploadMirrorSnapshot(ctx, repo)
	})
}

func (s *Strategy) snapshotMutexFor(upstreamURL string) *sync.Mutex {
	mu, _ := s.snapshotMu.LoadOrStore(upstreamURL, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Strategy) handleSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.tar.zst"))
	upstreamURL := "https://" + host + "/" + repoPath

	// Ensure the local mirror is ready and up to date before considering any
	// cached snapshot, so we never serve stale data to workstations.
	repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if repoErr != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", repoErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if cloneErr := s.ensureCloneReady(ctx, repo); cloneErr != nil {
		logger.ErrorContext(ctx, "Clone unavailable for snapshot", "upstream", upstreamURL, "error", cloneErr)
		http.Error(w, "Repository unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := s.ensureRefsUpToDate(ctx, repo); err != nil {
		logger.WarnContext(ctx, "Failed to check upstream refs for snapshot", "upstream", upstreamURL, "error", err)
	}

	cacheKey := snapshotCacheKey(upstreamURL)

	reader, headers, err := s.cache.Open(ctx, cacheKey)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to open snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Only serve the cached snapshot if it was generated after the mirror's
	// last successful fetch. Otherwise regenerate from the fresh mirror.
	if reader != nil {
		stale := true
		if lastMod := headers.Get("Last-Modified"); lastMod != "" {
			if t, parseErr := time.Parse(http.TimeFormat, lastMod); parseErr == nil {
				stale = repo.LastFetch().After(t)
			}
		}
		if stale {
			logger.InfoContext(ctx, "Cached snapshot predates last fetch, regenerating", "upstream", upstreamURL)
			_ = reader.Close()
			reader = nil
		}
	}

	if reader == nil {
		s.serveSnapshotWithSpool(w, r, repo, upstreamURL)
		return
	}
	defer reader.Close()

	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if _, err = io.Copy(w, reader); err != nil {
		logger.ErrorContext(ctx, "Failed to stream snapshot", "upstream", upstreamURL, "error", err)
	}
}

// serveSnapshotWithSpool handles snapshot cache misses using the spool pattern.
// The first request for a given upstream URL becomes the writer: it clones the
// mirror, streams tar+zstd to both the HTTP client and a spool file, then
// triggers a background cache backfill. Concurrent requests for the same URL
// become readers that follow the spool, avoiding redundant clone+tar work.
func (s *Strategy) serveSnapshotWithSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	// Use LoadOrStore with a sentinel to atomically elect a single writer.
	// The first goroutine stores an empty snapshotSpoolEntry and becomes the
	// writer. Concurrent goroutines see the existing entry and wait for the
	// spool to be published via the ready channel.
	entry := &snapshotSpoolEntry{ready: make(chan struct{})}
	if existing, loaded := s.snapshotSpools.LoadOrStore(upstreamURL, entry); loaded {
		winner := existing.(*snapshotSpoolEntry)
		<-winner.ready
		if spool := winner.spool; spool != nil && !spool.Failed() {
			logger.DebugContext(ctx, "Serving snapshot from spool", "upstream", upstreamURL)
			if err := spool.ServeTo(w); err != nil {
				if errors.Is(err, ErrSpoolFailed) {
					logger.DebugContext(ctx, "Snapshot spool failed before headers, falling back to direct stream", "upstream", upstreamURL)
					s.streamSnapshotDirect(w, r, repo, upstreamURL)
					return
				}
				logger.WarnContext(ctx, "Snapshot spool read error", "upstream", upstreamURL, "error", err)
			}
			return
		}
		// Writer failed; fall through to generate independently.
		s.streamSnapshotDirect(w, r, repo, upstreamURL)
		return
	}

	s.writeSnapshotSpool(w, r, repo, upstreamURL, entry)
}

// streamSnapshotDirect streams a snapshot directly to the client without
// spooling. Used as a fallback when the spool writer failed.
func (s *Strategy) streamSnapshotDirect(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)
	mirrorRoot := s.cloneManager.Config().MirrorRoot

	snapshotDir, err := os.MkdirTemp(mirrorRoot, ".snapshot-stream-*")
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create temp snapshot dir", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer func() { _ = os.RemoveAll(snapshotDir) }()

	repoDir := filepath.Join(snapshotDir, "repo")
	if err := s.cloneForSnapshot(ctx, repo, repoDir); err != nil {
		logger.ErrorContext(ctx, "Failed to clone for snapshot streaming", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	excludePatterns := []string{"*.lock"}
	if err := snapshot.StreamTo(ctx, w, repoDir, excludePatterns, s.config.ZstdThreads); err != nil {
		logger.ErrorContext(ctx, "Failed to stream snapshot to client", "upstream", upstreamURL, "error", err)
	}
}

// prepareSnapshotSpool creates the spool and clones the mirror into a temp directory,
// publishing the spool to waiting readers via the entry's ready channel. On failure
// it signals readers and returns an error.
func (s *Strategy) prepareSnapshotSpool(ctx context.Context, repo *gitclone.Repository, upstreamURL string, entry *snapshotSpoolEntry) (spool *ResponseSpool, spoolDir, repoDir string, err error) {
	mirrorRoot := s.cloneManager.Config().MirrorRoot

	spoolDir, err = snapshotSpoolDirForURL(mirrorRoot, upstreamURL)
	if err != nil {
		close(entry.ready)
		s.snapshotSpools.Delete(upstreamURL)
		return nil, "", "", err
	}

	spool, err = NewResponseSpool(filepath.Join(spoolDir, "snapshot.spool"))
	if err != nil {
		close(entry.ready)
		s.snapshotSpools.Delete(upstreamURL)
		return nil, "", "", err
	}
	entry.spool = spool
	close(entry.ready)

	snapshotDir, err := os.MkdirTemp(mirrorRoot, ".snapshot-stream-*")
	if err != nil {
		err = errors.Wrap(err, "create temp snapshot dir")
		spool.MarkError(err)
		s.snapshotSpools.Delete(upstreamURL)
		return nil, "", "", err
	}

	repoDir = filepath.Join(snapshotDir, "repo")
	if err := s.cloneForSnapshot(ctx, repo, repoDir); err != nil {
		spool.MarkError(err)
		s.snapshotSpools.Delete(upstreamURL)
		_ = os.RemoveAll(snapshotDir)
		return nil, "", "", err
	}

	return spool, spoolDir, repoDir, nil
}

// writeSnapshotSpool is the writer path for snapshot spooling. It creates a
// spool, clones the mirror, streams the tar+zstd output through a SpoolTeeWriter,
// and triggers a background cache backfill.
func (s *Strategy) writeSnapshotSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL string, entry *snapshotSpoolEntry) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	spool, spoolDir, repoDir, err := s.prepareSnapshotSpool(ctx, repo, upstreamURL, entry)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to prepare snapshot spool", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	snapshotDir := filepath.Dir(repoDir)

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	tw := NewSpoolTeeWriter(w, spool)
	excludePatterns := []string{"*.lock"}
	if err := snapshot.StreamTo(ctx, tw, repoDir, excludePatterns, s.config.ZstdThreads); err != nil {
		logger.ErrorContext(ctx, "Failed to stream snapshot to client", "upstream", upstreamURL, "error", err)
		spool.MarkError(err)
	} else {
		spool.MarkComplete()
	}

	go func() {
		spool.WaitForReaders()
		s.snapshotSpools.Delete(upstreamURL)
		_ = os.RemoveAll(spoolDir)
		_ = os.RemoveAll(snapshotDir)
	}()

	go func() {
		mu := s.snapshotMutexFor(upstreamURL)
		if !mu.TryLock() {
			logger.InfoContext(ctx, "Skipping background cache upload, snapshot generation already in progress",
				"upstream", upstreamURL)
			return
		}
		mu.Unlock()
		bgCtx := context.WithoutCancel(ctx)
		if err := s.generateAndUploadSnapshot(bgCtx, repo); err != nil {
			logger.ErrorContext(bgCtx, "Background cache upload failed", "upstream", upstreamURL, "error", err)
		}
	}()

	if s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
}

// snapshotSpoolEntry holds a spool and a ready channel used to coordinate
// writer election. The first goroutine stores the entry via LoadOrStore and
// becomes the writer. It closes ready once the spool is created (or on
// failure with spool == nil) so waiting readers can proceed.
type snapshotSpoolEntry struct {
	spool *ResponseSpool
	ready chan struct{}
}

func snapshotSpoolDirForURL(mirrorRoot, upstreamURL string) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve snapshot spool directory")
	}
	return filepath.Join(mirrorRoot, ".snapshot-spools", repoPath), nil
}
