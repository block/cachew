package git

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/jobscheduler"
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

func bundleCacheKey(upstreamURL, baseCommit string) cache.Key {
	return cache.NewKey(upstreamURL + ".bundle." + baseCommit)
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
	start := time.Now()

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

	// Capture the snapshot's HEAD so we can later build a delta bundle between
	// the cached snapshot and the current mirror state.
	headSHA, err := revParse(ctx, snapshotDir, "HEAD")
	if err != nil {
		_ = os.RemoveAll(snapshotDir) //nolint:gosec
		return errors.Wrap(err, "rev-parse HEAD for snapshot")
	}
	extraHeaders := http.Header{}
	extraHeaders.Set("X-Cachew-Snapshot-Commit", headSHA)

	cacheKey := snapshotCacheKey(upstream)

	err = snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, 0, nil, s.config.ZstdThreads, extraHeaders)

	// Always clean up the snapshot working directory.
	if rmErr := os.RemoveAll(snapshotDir); rmErr != nil { //nolint:gosec // snapshotDir is derived from controlled mirrorRoot + upstream URL
		logger.WarnContext(ctx, "Failed to clean up snapshot dir", "error", rmErr)
	}
	if err != nil {
		s.metrics.recordOperation(ctx, "snapshot", "error", time.Since(start))
		return errors.Wrap(err, "create snapshot")
	}

	s.metrics.recordOperation(ctx, "snapshot", "success", time.Since(start))
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

	// Hold the fetch semaphore while tar-ing the bare mirror directory.
	// Without this, a concurrent git fetch can replace packed-refs mid-read,
	// causing tar to capture a truncated file.
	if err := repo.WithFetchExclusion(ctx, func() error {
		return repo.WithReadLock(func() error {
			return snapshot.Create(ctx, s.cache, cacheKey, repo.Path(), 0, excludePatterns, s.config.ZstdThreads)
		})
	}); err != nil {
		return errors.Wrap(err, "create mirror snapshot")
	}

	logger.InfoContext(ctx, "Mirror snapshot generation completed", "upstream", upstream)
	return nil
}

func (s *Strategy) scheduleSnapshotJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(jobscheduler.Job{Queue: repo.UpstreamURL(), ID: "snapshot-periodic", Cost: CostSnapshot, Run: func(ctx context.Context) error {
		return s.generateAndUploadSnapshot(ctx, repo)
	}}, s.config.SnapshotInterval)
	mirrorInterval := s.config.MirrorSnapshotInterval
	if mirrorInterval == 0 {
		mirrorInterval = s.config.SnapshotInterval
	}
	s.scheduler.SubmitPeriodicJob(jobscheduler.Job{Queue: repo.UpstreamURL(), ID: "mirror-snapshot-periodic", Cost: CostSnapshot, Run: func(ctx context.Context) error {
		return s.generateAndUploadMirrorSnapshot(ctx, repo)
	}}, mirrorInterval)
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

	repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if repoErr != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", repoErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	cacheKey := snapshotCacheKey(upstreamURL)

	// On cold start the local mirror may not be ready yet. Check the S3 cache
	// first so we can stream a cached snapshot to the client immediately while
	// the mirror restores in the background. This avoids blocking the client
	// behind the full S3-download → extract → git-fetch pipeline.
	if repo.State() != gitclone.StateReady {
		entry := &coldSnapshotEntry{done: make(chan struct{})}
		if existing, loaded := s.coldSnapshotMu.LoadOrStore(upstreamURL, entry); loaded {
			winner := existing.(*coldSnapshotEntry)
			<-winner.done
			reader, _, openErr := s.cache.Open(ctx, cacheKey)
			if openErr == nil && reader != nil {
				winner.serving.Add(1)
				defer func() {
					_ = reader.Close()
					winner.serving.Done()
				}()
				logger.InfoContext(ctx, "Serving locally cached snapshot after waiting for in-flight fill", "upstream", upstreamURL)
				w.Header().Set("Content-Type", "application/zstd")
				if _, err := io.Copy(w, reader); err != nil {
					logger.WarnContext(ctx, "Failed to stream locally cached snapshot", "upstream", upstreamURL, "error", err)
				}
				return
			}
		} else {
			defer func() {
				close(entry.done)
				s.coldSnapshotMu.Delete(upstreamURL)
			}()
			reader, _, openErr := s.cache.Open(ctx, cacheKey)
			if openErr == nil && reader != nil {
				logger.InfoContext(ctx, "Serving cached snapshot while mirror warms up", "upstream", upstreamURL)
				w.Header().Set("Content-Type", "application/zstd")
				if _, err := io.Copy(w, reader); err != nil {
					logger.WarnContext(ctx, "Failed to stream cached snapshot", "upstream", upstreamURL, "error", err)
				}
				_ = reader.Close()
				// Schedule a deferred mirror restore so the mirror eventually
				// becomes hot and cachew can generate fresh bundle deltas.
				// Without this, repos that only ever serve cached snapshots
				// would never restore their mirror.
				s.scheduleDeferredMirrorRestore(ctx, repo, entry)
				return
			}
			if reader != nil {
				_ = reader.Close()
			}
		}
	}

	// Either the mirror is already ready or no cached snapshot exists — fall
	// through to the original path which blocks until the mirror is available.
	if cloneErr := s.ensureCloneReady(ctx, repo); cloneErr != nil {
		logger.ErrorContext(ctx, "Clone unavailable for snapshot", "upstream", upstreamURL, "error", cloneErr)
		http.Error(w, "Repository unavailable", http.StatusServiceUnavailable)
		return
	}
	s.maybeBackgroundFetch(repo)

	reader, headers, err := s.cache.Open(ctx, cacheKey)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to open snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if reader == nil {
		if err := s.serveSnapshotWithSpool(w, r, repo, upstreamURL); err != nil {
			logger.ErrorContext(ctx, "Failed to serve snapshot via spool", "upstream", upstreamURL, "error", err)
		}
		return
	}
	defer reader.Close()

	if err := s.serveSnapshotWithBundle(ctx, w, reader, headers, repo, upstreamURL); err != nil {
		logger.ErrorContext(ctx, "Failed to serve snapshot", "upstream", upstreamURL, "error", err)
	}
}

func (s *Strategy) handleBundleRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.bundle"))
	upstreamURL := "https://" + host + "/" + repoPath

	base := r.URL.Query().Get("base")
	if base == "" {
		http.Error(w, "missing base query parameter", http.StatusBadRequest)
		return
	}

	bKey := bundleCacheKey(upstreamURL, base)

	// Try serving from cache first — works on any pod.
	if reader, _, err := s.cache.Open(ctx, bKey); err == nil && reader != nil {
		defer reader.Close()
		w.Header().Set("Content-Type", "application/x-git-bundle")
		if _, err := io.Copy(w, reader); err != nil {
			logger.WarnContext(ctx, "Failed to stream cached bundle", "upstream", upstreamURL, "error", err)
		}
		return
	}

	// Fallback: generate from local mirror.
	repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if repoErr != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", repoErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if cloneErr := s.ensureCloneReady(ctx, repo); cloneErr != nil {
		logger.ErrorContext(ctx, "Clone unavailable for bundle", "upstream", upstreamURL, "error", cloneErr)
		http.Error(w, "Repository unavailable", http.StatusServiceUnavailable)
		return
	}

	bundleData, err := s.createBundle(ctx, repo, base)
	if err != nil {
		logger.WarnContext(ctx, "Failed to create bundle", "upstream", upstreamURL, "base", base, "error", err)
		http.Error(w, "Bundle not available", http.StatusNotFound)
		return
	}

	// Cache for future requests from any pod.
	s.cacheBundleAsync(ctx, bKey, bundleData)

	w.Header().Set("Content-Type", "application/x-git-bundle")
	w.Header().Set("Content-Length", strconv.Itoa(len(bundleData)))
	if _, err := w.Write(bundleData); err != nil { //nolint:gosec // bundleData is a git bundle generated from a trusted local mirror
		logger.WarnContext(ctx, "Failed to write bundle response", "upstream", upstreamURL, "error", err)
	}
}

func (s *Strategy) serveSnapshotWithBundle(ctx context.Context, w http.ResponseWriter, reader io.ReadCloser, headers http.Header, repo *gitclone.Repository, upstreamURL string) error {
	snapshotCommit := headers.Get("X-Cachew-Snapshot-Commit")
	mirrorHead := s.getMirrorHead(ctx, repo)

	// Forward the snapshot commit to the client so it knows whether the
	// snapshot is fresh (no bundle URL = already at HEAD, skip freshen).
	if snapshotCommit != "" {
		w.Header().Set("X-Cachew-Snapshot-Commit", snapshotCommit)
	}

	if snapshotCommit != "" && mirrorHead != "" && snapshotCommit != mirrorHead {
		repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
		if err == nil {
			bundleURL := fmt.Sprintf("/git/%s/snapshot.bundle?base=%s", repoPath, snapshotCommit)
			w.Header().Set("X-Cachew-Bundle-Url", bundleURL)
		}

		// Proactively generate and cache the bundle so any pod can serve it.
		go func() {
			bgCtx := context.WithoutCancel(ctx)
			logger := logging.FromContext(bgCtx)
			bundleData, err := s.createBundle(bgCtx, repo, snapshotCommit)
			if err != nil {
				logger.WarnContext(bgCtx, "Failed to pre-generate bundle", "upstream", upstreamURL, "error", err)
				return
			}
			if err := s.cacheBundleSync(bgCtx, bundleCacheKey(upstreamURL, snapshotCommit), bundleData); err != nil {
				logger.WarnContext(bgCtx, "Failed to cache bundle", "upstream", upstreamURL, "error", err)
			}
		}()
	}

	w.Header().Set("Content-Type", "application/zstd")
	_, err := io.Copy(w, reader)
	return errors.Wrap(err, "stream snapshot")
}

const bundleCacheTTL = 2 * time.Hour

func (s *Strategy) cacheBundleAsync(ctx context.Context, key cache.Key, data []byte) {
	go func() {
		bgCtx := context.WithoutCancel(ctx)
		if err := s.cacheBundleSync(bgCtx, key, data); err != nil {
			logging.FromContext(bgCtx).WarnContext(bgCtx, "Failed to cache bundle", "error", err)
		}
	}()
}

func (s *Strategy) cacheBundleSync(ctx context.Context, key cache.Key, data []byte) error {
	headers := http.Header{"Content-Type": {"application/x-git-bundle"}}
	wc, err := s.cache.Create(ctx, key, headers, bundleCacheTTL)
	if err != nil {
		return errors.Wrap(err, "create cache entry")
	}
	if _, err := wc.Write(data); err != nil {
		_ = wc.Close()
		return errors.Wrap(err, "write bundle to cache")
	}
	return errors.Wrap(wc.Close(), "close bundle cache writer")
}

func revParse(ctx context.Context, repoDir, ref string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "rev-parse", ref) // #nosec G204 G702
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Wrapf(err, "git rev-parse %s", ref)
	}
	return strings.TrimSpace(string(output)), nil
}

func (s *Strategy) getMirrorHead(ctx context.Context, repo *gitclone.Repository) string {
	head, _ := revParse(ctx, repo.Path(), "HEAD") //nolint:errcheck // best-effort; empty string signals failure to callers
	return head
}

func (s *Strategy) createBundle(ctx context.Context, repo *gitclone.Repository, baseCommit string) ([]byte, error) {
	// No read lock needed: git bundle create reads objects through git's own
	// file-level locking, safe to run concurrently with fetches.
	headRef := "HEAD"
	if out, err := exec.CommandContext(ctx, "git", "-C", repo.Path(), "symbolic-ref", "HEAD").Output(); err == nil { //nolint:gosec // repo.Path() is controlled by us
		headRef = strings.TrimSpace(string(out))
	}

	tmpFile, err := os.CreateTemp("", "cachew-bundle-*.bundle")
	if err != nil {
		return nil, errors.Wrap(err, "create bundle temp file")
	}
	bundlePath := tmpFile.Name()
	defer os.Remove(bundlePath) //nolint:errcheck
	if err := tmpFile.Close(); err != nil {
		return nil, errors.Wrap(err, "close bundle temp file")
	}

	cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "bundle", "create", //nolint:gosec // baseCommit is a SHA string from rev-parse
		bundlePath, headRef, "^"+baseCommit)
	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, errors.Wrapf(err, "git bundle create: %s", string(output))
	}

	data, err := os.ReadFile(bundlePath) //nolint:gosec // bundlePath is a temp file we created
	if err != nil {
		return nil, errors.Wrap(err, "read bundle file")
	}
	return data, nil
}

// serveSnapshotWithSpool handles snapshot cache misses using the spool pattern.
// The first request for a given upstream URL becomes the writer: it clones the
// mirror, streams tar+zstd to both the HTTP client and a spool file, then
// triggers a background cache backfill. Concurrent requests for the same URL
// become readers that follow the spool, avoiding redundant clone+tar work.
func (s *Strategy) serveSnapshotWithSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL string) error {
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
					return s.streamSnapshotDirect(w, r, repo)
				}
				return errors.Wrap(err, "snapshot spool read")
			}
			return nil
		}
		// Writer failed; fall through to generate independently.
		return s.streamSnapshotDirect(w, r, repo)
	}

	return s.writeSnapshotSpool(w, r, repo, upstreamURL, entry)
}

// streamSnapshotDirect streams a snapshot directly to the client without
// spooling. Used as a fallback when the spool writer failed.
func (s *Strategy) streamSnapshotDirect(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository) error {
	ctx := r.Context()
	mirrorRoot := s.cloneManager.Config().MirrorRoot

	snapshotDir, err := os.MkdirTemp(mirrorRoot, ".snapshot-stream-*")
	if err != nil {
		return errors.Wrap(err, "create temp snapshot dir")
	}
	defer func() { _ = os.RemoveAll(snapshotDir) }()

	repoDir := filepath.Join(snapshotDir, "repo")
	if err := s.cloneForSnapshot(ctx, repo, repoDir); err != nil {
		return errors.Wrap(err, "clone for snapshot streaming")
	}

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	return errors.Wrap(snapshot.StreamTo(ctx, w, repoDir, nil, s.config.ZstdThreads), "stream snapshot to client")
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
func (s *Strategy) writeSnapshotSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL string, entry *snapshotSpoolEntry) error {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	spool, spoolDir, repoDir, err := s.prepareSnapshotSpool(ctx, repo, upstreamURL, entry)
	if err != nil {
		return errors.Wrap(err, "prepare snapshot spool")
	}
	snapshotDir := filepath.Dir(repoDir)

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	tw := NewSpoolTeeWriter(w, spool)
	streamErr := snapshot.StreamTo(ctx, tw, repoDir, nil, s.config.ZstdThreads)
	if streamErr != nil {
		spool.MarkError(streamErr)
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
	return errors.Wrap(streamErr, "stream snapshot to client")
}

// scheduleDeferredMirrorRestore schedules a one-shot background mirror restore
// for a repo that was served from a cached S3 snapshot on cold start. Without
// this, repos that only serve cached snapshots would never warm their mirror,
// preventing cachew from generating fresh bundle deltas.
//
// Submitted to the scheduler immediately after the first S3 snapshot stream
// completes. By this point the client snapshot is backfilled to local disk, so
// subsequent snapshot serves read from NVMe and don't compete for S3 bandwidth.
// The scheduler's concurrency limit naturally throttles the restore against
// other background work. Only one restore is scheduled per upstream URL.
func (s *Strategy) scheduleDeferredMirrorRestore(ctx context.Context, repo *gitclone.Repository, coldEntry *coldSnapshotEntry) {
	upstream := repo.UpstreamURL()
	if _, loaded := s.deferredRestoreOnce.LoadOrStore(upstream, true); loaded {
		return
	}

	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Scheduling deferred mirror restore", "upstream", upstream)

	s.scheduler.Submit(jobscheduler.Job{Queue: upstream, ID: "deferred-mirror-restore", Cost: CostClone, Clone: true, Run: func(ctx context.Context) error {
		logger := logging.FromContext(ctx)
		if repo.State() == gitclone.StateReady {
			logger.InfoContext(ctx, "Mirror already ready, skipping deferred restore", "upstream", upstream)
			return nil
		}
		if !repo.TryStartCloning() {
			logger.InfoContext(ctx, "Mirror restore already in progress, skipping", "upstream", upstream)
			return nil
		}
		// Wait for all in-flight cold snapshot serves to finish so the
		// restore's disk writes don't compete with local cache reads.
		coldEntry.serving.Wait()

		logger.InfoContext(ctx, "Starting deferred mirror restore", "upstream", upstream)

		if err := s.tryRestoreSnapshot(ctx, repo); err != nil {
			logger.WarnContext(ctx, "Deferred mirror snapshot restore failed", "upstream", upstream, "error", err)
			repo.ResetToEmpty()
			return nil
		}

		if err := repo.FetchLenient(ctx, gitclone.CloneTimeout); err != nil {
			logger.WarnContext(ctx, "Deferred mirror post-restore fetch failed", "upstream", upstream, "error", err)
			repo.ResetToEmpty()
			if rmErr := os.RemoveAll(repo.Path()); rmErr != nil {
				logger.WarnContext(ctx, "Failed to remove mirror after failed fetch", "upstream", upstream, "error", rmErr)
			}
			return nil
		}

		repo.MarkReady()
		logger.InfoContext(ctx, "Deferred mirror restore completed", "upstream", upstream)

		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobs(repo)
		}
		if s.config.RepackInterval > 0 {
			s.scheduleRepackJobs(repo)
		}
		return nil
	}})
}

// snapshotSpoolEntry holds a spool and a ready channel used to coordinate
// writer election. The first goroutine stores the entry via LoadOrStore and
// becomes the writer. It closes ready once the spool is created (or on
// failure with spool == nil) so waiting readers can proceed.
type snapshotSpoolEntry struct {
	spool *ResponseSpool
	ready chan struct{}
}

type coldSnapshotEntry struct {
	done    chan struct{}
	serving sync.WaitGroup // tracks all in-flight snapshot serves (winner + followers)
}

func snapshotSpoolDirForURL(mirrorRoot, upstreamURL string) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve snapshot spool directory")
	}
	return filepath.Join(mirrorRoot, ".snapshot-spools", repoPath), nil
}
