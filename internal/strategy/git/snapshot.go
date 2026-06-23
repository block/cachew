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
	"syscall"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

const lfsFetchTimeout = 25 * time.Minute

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

func lfsSnapshotCacheKey(upstreamURL string) cache.Key {
	return cache.NewKey(upstreamURL + ".lfs-snapshot")
}

// cloneForSnapshot clones the mirror into destDir under repo's read lock,
// then fixes the remote URL to point through cachew (or upstream).
func (s *Strategy) cloneForSnapshot(ctx context.Context, repo *gitclone.Repository, destDir string) error {
	if err := repo.WithReadLock(func() error {
		// #nosec G204 - repo.Path() and destDir are controlled by us
		cmd := exec.CommandContext(ctx, "git", "clone", repo.Path(), destDir)
		cmd.Env = append(os.Environ(), "GIT_LFS_SKIP_SMUDGE=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "git clone for snapshot: %s", string(output))
		}

		// git clone from a local path sets remote.origin.url to that path; restore
		// it to the upstream URL. Clients use insteadOf to route through cachew, so
		// embedding the cachew URL here would couple snapshots to a specific instance.
		// #nosec G204 - upstreamURL is derived from controlled inputs
		cmd = exec.CommandContext(ctx, "git", "-C", destDir, "remote", "set-url", "origin", repo.UpstreamURL())
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "fix snapshot remote URL: %s", string(output))
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *Strategy) withSnapshotClone(ctx context.Context, repo *gitclone.Repository, suffix string, fn func(workDir string) error) error {
	logger := logging.FromContext(ctx)
	mirrorRoot := s.cloneManager.Config().MirrorRoot
	workDir, err := snapshotDirForURL(mirrorRoot, repo.UpstreamURL())
	if err != nil {
		return err
	}
	workDir = filepath.Join(workDir, suffix)

	// Clean any previous snapshot working directory.
	if err := os.RemoveAll(workDir); err != nil {
		return errors.Wrap(err, "remove previous snapshot work dir")
	}
	if err := os.MkdirAll(filepath.Dir(workDir), 0o750); err != nil {
		return errors.Wrap(err, "create snapshot work dir parent")
	}

	if err := s.cloneForSnapshot(ctx, repo, workDir); err != nil {
		_ = os.RemoveAll(workDir)
		return err
	}

	// Always clean up the snapshot working directory.
	defer func() {
		if rmErr := os.RemoveAll(workDir); rmErr != nil {
			logger.WarnContext(ctx, "Failed to clean up snapshot work dir", "work_dir", workDir, "error", rmErr)
		}
	}()

	return fn(workDir)
}

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	upstream := repo.UpstreamURL()
	ctx, span := tracer.Start(ctx, "git.snapshot.generate",
		trace.WithAttributes(
			attribute.String("cachew.operation", "snapshot_generate"),
			attribute.String("cachew.upstream", upstream),
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

	logger.InfoContext(ctx, "Snapshot generation started", "upstream", upstream)

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := snapshotCacheKey(upstream)
	if err := s.withSnapshotClone(ctx, repo, "base", func(workDir string) error {
		// Capture the snapshot's HEAD so we can later build a delta bundle between
		// the cached snapshot and the current mirror state.
		headSHA, err := revParse(ctx, workDir, "HEAD")
		if err != nil {
			return errors.Wrap(err, "rev-parse HEAD for snapshot")
		}
		extraHeaders := http.Header{}
		extraHeaders.Set("X-Cachew-Snapshot-Commit", headSHA)

		return snapshot.Create(ctx, s.cache, cacheKey, workDir, 0, nil, s.config.ZstdThreads, extraHeaders)
	}); err != nil {
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
func (s *Strategy) generateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	upstream := repo.UpstreamURL()
	ctx, span := tracer.Start(ctx, "git.snapshot.generate_mirror",
		trace.WithAttributes(
			attribute.String("cachew.operation", "mirror_snapshot_generate"),
			attribute.String("cachew.upstream", upstream),
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
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "snapshot-periodic", s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadSnapshot(ctx, repo)
	})
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "lfs-snapshot-periodic", s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadLFSSnapshot(ctx, repo)
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

func (s *Strategy) handleSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) { //nolint:funlen
	start := time.Now()
	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.tar.zst"))
	upstreamURL := "https://" + host + "/" + repoPath
	repoName := host + "/" + repoPath

	ctx, span := tracer.Start(r.Context(), "git.snapshot.serve",
		trace.WithAttributes(
			attribute.String("cachew.operation", "snapshot_serve"),
			attribute.String("cachew.upstream", upstreamURL),
			attribute.String("cachew.repository", repoName),
		),
	)
	defer span.End()
	r = r.WithContext(ctx)
	logger := logging.FromContext(ctx)

	cacheKey := snapshotCacheKey(upstreamURL)

	// HEAD is answered from cache metadata alone so probes never read or
	// generate the snapshot body, and never warm up a mirror.
	if r.Method == http.MethodHead {
		s.serveSnapshotHead(ctx, w, r, cacheKey, repoName, start)
		return
	}

	repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if repoErr != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", repoErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// On cold start the local mirror may not be ready yet. Check the S3 cache
	// first so we can stream a cached snapshot to the client immediately while
	// the mirror restores in the background. This avoids blocking the client
	// behind the full S3-download → extract → git-fetch pipeline.
	if repo.State() != gitclone.StateReady {
		entry := &coldSnapshotEntry{done: make(chan struct{})}
		if existing, loaded := s.coldSnapshotMu.LoadOrStore(upstreamURL, entry); loaded {
			winner := existing.(*coldSnapshotEntry)
			<-winner.done
			reader, _, openErr := s.cache.Open(ctx, cacheKey, 0, -1)
			if openErr == nil && reader != nil {
				winner.serving.Add(1)
				defer func() {
					_ = reader.Close()
					winner.serving.Done()
				}()
				logger.InfoContext(ctx, "Serving locally cached snapshot after waiting for in-flight fill", "upstream", upstreamURL)
				w.Header().Set("Content-Type", "application/zstd")
				n, err := serveReaderFast(w, r, reader)
				s.metrics.recordSnapshotServe(ctx, "cold_cache", repoName, n, time.Since(start))
				span.SetAttributes(attribute.String("cachew.source", "cold_cache"), attribute.Int64("cachew.bytes", n))
				if err != nil {
					logger.WarnContext(ctx, "Failed to stream locally cached snapshot", "upstream", upstreamURL, "error", err)
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				return
			}
		} else {
			defer func() {
				close(entry.done)
				s.coldSnapshotMu.Delete(upstreamURL)
			}()
			reader, _, openErr := s.cache.Open(ctx, cacheKey, 0, -1)
			if openErr == nil && reader != nil {
				logger.InfoContext(ctx, "Serving cached snapshot while mirror warms up", "upstream", upstreamURL)
				w.Header().Set("Content-Type", "application/zstd")
				n, err := serveReaderFast(w, r, reader)
				s.metrics.recordSnapshotServe(ctx, "cold_cache", repoName, n, time.Since(start))
				span.SetAttributes(attribute.String("cachew.source", "cold_cache"), attribute.Int64("cachew.bytes", n))
				if err != nil {
					logger.WarnContext(ctx, "Failed to stream cached snapshot", "upstream", upstreamURL, "error", err)
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				_ = reader.Close()
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

	reader, headers, err := s.cache.Open(ctx, cacheKey, 0, -1)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to open snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	if reader == nil {
		if err := s.serveSnapshotWithSpool(w, r, repo, upstreamURL, repoName, start); err != nil {
			logger.ErrorContext(ctx, "Failed to serve snapshot via spool", "upstream", upstreamURL, "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return
	}
	defer reader.Close()

	if err := s.serveSnapshotWithBundle(ctx, w, r, reader, headers, repo, upstreamURL, repoName, start); err != nil {
		logger.ErrorContext(ctx, "Failed to serve snapshot", "upstream", upstreamURL, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// serveSnapshotHead answers a HEAD request from cache metadata alone via Stat,
// reporting the snapshot's validators (ETag, Content-Length) and freshness
// commit without reading or generating the body. An uncached snapshot yields
// 404 rather than triggering the expensive on-demand generation that GET does.
// If-None-Match / If-Match preconditions are honoured against the cached ETag.
func (s *Strategy) serveSnapshotHead(ctx context.Context, w http.ResponseWriter, r *http.Request, cacheKey cache.Key, repoName string, start time.Time) {
	headers, err := s.cache.Stat(ctx, cacheKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Snapshot not cached", http.StatusNotFound)
			return
		}
		logging.FromContext(ctx).ErrorContext(ctx, "Failed to stat snapshot", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	applySnapshotCacheHeaders(w, headers)
	if commit := headers.Get("X-Cachew-Snapshot-Commit"); commit != "" {
		w.Header().Set("X-Cachew-Snapshot-Commit", commit)
	}

	status := http.StatusOK
	if conditional := httputil.CheckConditionals(r, headers.Get(cache.ETagKey)); conditional != 0 {
		status = conditional
	}
	w.WriteHeader(status)

	s.metrics.recordSnapshotServe(ctx, "head", repoName, 0, time.Since(start))
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.source", "head"), attribute.Int64("cachew.bytes", 0))
	}
}

func (s *Strategy) streamSnapshotArtifact(_ context.Context, w http.ResponseWriter, r *http.Request, reader io.ReadCloser, headers http.Header) error {
	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if _, err := serveReaderFast(w, r, reader); err != nil {
		return errors.Wrap(err, "streaming artifact")
	}
	return nil
}

// serveReaderFast serves the content from reader using the most efficient method
// available. When reader is an *os.File, it uses http.ServeContent which enables
// sendfile(2) zero-copy I/O and automatic Content-Length/Range support. For other
// reader types it falls back to io.Copy. Returns bytes served for metrics.
func serveReaderFast(w http.ResponseWriter, r *http.Request, reader io.Reader) (int64, error) {
	if f, ok := reader.(*os.File); ok {
		info, err := f.Stat()
		if err != nil {
			return 0, errors.Wrap(err, "stat file for serving")
		}
		// http.ServeContent handles Content-Length, Range requests, and uses
		// sendfile(2) for zero-copy transfer from file to socket.
		http.ServeContent(w, r, "", time.Time{}, f)
		return info.Size(), nil
	}
	n, err := io.Copy(w, reader)
	return n, errors.Wrap(err, "copy to response")
}

func (s *Strategy) handleBundleRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) { //nolint:funlen
	start := time.Now()
	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.bundle"))
	upstreamURL := "https://" + host + "/" + repoPath
	repoName := host + "/" + repoPath

	ctx, span := tracer.Start(r.Context(), "git.bundle.serve",
		trace.WithAttributes(
			attribute.String("cachew.operation", "bundle_serve"),
			attribute.String("cachew.upstream", upstreamURL),
			attribute.String("cachew.repository", repoName),
		),
	)
	defer span.End()
	logger := logging.FromContext(ctx)

	base := r.URL.Query().Get("base")
	if base == "" {
		http.Error(w, "missing base query parameter", http.StatusBadRequest)
		span.SetAttributes(attribute.String("cachew.source", "bad_request"))
		return
	}
	span.SetAttributes(attribute.String("cachew.base_commit", base))

	bKey := bundleCacheKey(upstreamURL, base)

	// Source and bytes are recorded by the deferred metric call.
	source := "miss"
	var bytes int64
	defer func() {
		span.SetAttributes(attribute.String("cachew.source", source), attribute.Int64("cachew.bytes", bytes))
		s.metrics.recordBundleServe(ctx, source, repoName, bytes, time.Since(start))
	}()

	// Try serving from cache first — works on any pod.
	if reader, _, err := s.cache.Open(ctx, bKey, 0, -1); err == nil && reader != nil {
		defer reader.Close()
		w.Header().Set("Content-Type", "application/x-git-bundle")
		n, err := io.Copy(w, reader)
		bytes = n
		source = "cache"
		if err != nil {
			logger.WarnContext(ctx, "Failed to stream cached bundle", "upstream", upstreamURL, "error", err)
			span.RecordError(err)
		}
		return
	}

	// Fallback: generate from local mirror.
	repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if repoErr != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", repoErr)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		span.RecordError(repoErr)
		span.SetStatus(codes.Error, repoErr.Error())
		return
	}
	if cloneErr := s.ensureCloneReady(ctx, repo); cloneErr != nil {
		logger.ErrorContext(ctx, "Clone unavailable for bundle", "upstream", upstreamURL, "error", cloneErr)
		http.Error(w, "Repository unavailable", http.StatusServiceUnavailable)
		span.RecordError(cloneErr)
		span.SetStatus(codes.Error, cloneErr.Error())
		return
	}

	bundleFile, err := s.createBundle(ctx, repo, base)
	if err != nil {
		logger.WarnContext(ctx, "Failed to create bundle", "upstream", upstreamURL, "base", base, "error", err)
		http.Error(w, "Bundle not available", http.StatusNotFound)
		span.RecordError(err)
		return
	}
	defer bundleFile.Close()

	w.Header().Set("Content-Type", "application/x-git-bundle")

	// Stream to client and cache simultaneously so the bundle never has to be
	// buffered in memory. If creating the cache writer fails we still serve
	// the client.
	wc, cacheErr := s.cache.Create(ctx, bKey, http.Header{"Content-Type": {"application/x-git-bundle"}}, s.config.BundleCacheTTL)
	if cacheErr != nil {
		logger.WarnContext(ctx, "Failed to create bundle cache writer", "upstream", upstreamURL, "error", cacheErr)
		n, err := io.Copy(w, bundleFile)
		bytes = n
		source = "generated"
		if err != nil {
			logger.WarnContext(ctx, "Failed to stream bundle", "upstream", upstreamURL, "error", err)
			span.RecordError(err)
		}
		return
	}
	n, copyErr := io.Copy(io.MultiWriter(w, wc), bundleFile)
	bytes = n
	source = "generated"
	if copyErr != nil {
		logger.WarnContext(ctx, "Failed to stream bundle", "upstream", upstreamURL, "error", copyErr)
		span.RecordError(copyErr)
		if abortErr := wc.Abort(copyErr); abortErr != nil {
			logger.WarnContext(ctx, "Failed to abort bundle cache writer", "upstream", upstreamURL, "error", abortErr)
		}
		return
	}
	if err := wc.Close(); err != nil {
		logger.WarnContext(ctx, "Failed to close bundle cache writer", "upstream", upstreamURL, "error", err)
	}
}

func (s *Strategy) serveSnapshotWithBundle(ctx context.Context, w http.ResponseWriter, r *http.Request, reader io.ReadCloser, headers http.Header, repo *gitclone.Repository, upstreamURL, repoName string, start time.Time) error {
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
			bundleFile, err := s.createBundle(bgCtx, repo, snapshotCommit)
			if err != nil {
				logger.WarnContext(bgCtx, "Failed to pre-generate bundle", "upstream", upstreamURL, "error", err)
				return
			}
			defer bundleFile.Close()
			if err := s.cacheBundle(bgCtx, bundleCacheKey(upstreamURL, snapshotCommit), bundleFile); err != nil {
				logger.WarnContext(bgCtx, "Failed to cache bundle", "upstream", upstreamURL, "error", err)
			}
		}()
	}

	applySnapshotCacheHeaders(w, headers)

	// Honour conditional GETs against the advertised ETag. ServeContent does this
	// natively for *os.File readers, but cache backends returning non-file readers
	// (S3, memory, remote) fall through to io.Copy, so revalidate explicitly to
	// avoid streaming the full snapshot when the client already has it.
	var n int64
	var err error
	if status := httputil.CheckConditionals(r, headers.Get(cache.ETagKey)); status != 0 {
		w.WriteHeader(status)
	} else {
		n, err = serveReaderFast(w, r, reader)
	}
	s.metrics.recordSnapshotServe(ctx, "cache", repoName, n, time.Since(start))
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.source", "cache"), attribute.Int64("cachew.bytes", n))
	}
	return errors.Wrap(err, "stream snapshot")
}

// applySnapshotCacheHeaders forwards the cached snapshot's validators so clients
// can revalidate (ETag) and size the transfer (Content-Length). Content-Type is
// fixed for snapshots regardless of what the cache backend recorded.
func applySnapshotCacheHeaders(w http.ResponseWriter, headers http.Header) {
	w.Header().Set("Content-Type", "application/zstd")
	if etag := headers.Get(cache.ETagKey); etag != "" {
		w.Header().Set(cache.ETagKey, etag)
	}
	if contentLength := headers.Get("Content-Length"); contentLength != "" {
		w.Header().Set("Content-Length", contentLength)
	}
}

// cacheBundle streams r into the cache under key. Used by the bundle
// pre-generation path; handleBundleRequest caches inline via io.MultiWriter.
func (s *Strategy) cacheBundle(ctx context.Context, key cache.Key, r io.Reader) error {
	headers := http.Header{"Content-Type": {"application/x-git-bundle"}}
	wc, err := s.cache.Create(ctx, key, headers, s.config.BundleCacheTTL)
	if err != nil {
		return errors.Wrap(err, "create cache entry")
	}
	if _, err := io.Copy(wc, r); err != nil {
		return errors.Join(errors.Wrap(err, "write bundle to cache"), wc.Abort(err))
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

// createBundle generates a git bundle for the commits between baseCommit and
// the mirror's HEAD, writing it to a temp file. It returns an open *os.File
// to that temp file; the file has already been removed from the filesystem,
// so the open file descriptor is what keeps the data alive. The caller must
// Close() the returned file.
func (s *Strategy) createBundle(ctx context.Context, repo *gitclone.Repository, baseCommit string) (*os.File, error) {
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
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(bundlePath) //nolint:gosec // bundlePath is from os.CreateTemp
		return nil, errors.Wrap(err, "close bundle temp file")
	}

	cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "bundle", "create", //nolint:gosec // baseCommit is a SHA string from rev-parse
		bundlePath, headRef, "^"+baseCommit)
	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(bundlePath) //nolint:gosec // bundlePath is from os.CreateTemp
		return nil, errors.Wrapf(err, "git bundle create: %s", string(output))
	}

	f, err := os.Open(bundlePath) //nolint:gosec // bundlePath is from os.CreateTemp
	if err != nil {
		_ = os.Remove(bundlePath) //nolint:gosec // bundlePath is from os.CreateTemp
		return nil, errors.Wrap(err, "open bundle file")
	}
	// Unlink immediately; the open fd keeps the data alive until f.Close().
	if err := os.Remove(bundlePath); err != nil { //nolint:gosec // bundlePath is from os.CreateTemp
		_ = f.Close()
		return nil, errors.Wrap(err, "remove bundle temp file")
	}
	return f, nil
}

// serveSnapshotWithSpool handles snapshot cache misses using the spool pattern.
// The first request for a given upstream URL becomes the writer: it clones the
// mirror, streams tar+zstd to both the HTTP client and a spool file, then
// triggers a background cache backfill. Concurrent requests for the same URL
// become readers that follow the spool, avoiding redundant clone+tar work.
func (s *Strategy) serveSnapshotWithSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL, repoName string, start time.Time) error {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	// Use LoadOrStore with a sentinel to atomically elect a single writer.
	// The first goroutine stores an empty snapshotSpoolEntry and becomes the
	// writer. Concurrent goroutines see the existing entry and wait for the
	// spool to be published via the ready channel.
	entry := &snapshotSpoolEntry{ready: make(chan struct{})}
	if existing, loaded := s.snapshotSpools.LoadOrStore(upstreamURL, entry); loaded {
		winner := existing.(*snapshotSpoolEntry)
		waitStart := time.Now()
		<-winner.ready
		wait := time.Since(waitStart)
		if spool := winner.spool; spool != nil && !spool.Failed() {
			logger.DebugContext(ctx, "Serving snapshot from spool", "upstream", upstreamURL, "wait", wait)
			if err := spool.ServeTo(w); err != nil {
				if errors.Is(err, ErrSpoolFailed) {
					logger.DebugContext(ctx, "Snapshot spool failed before headers, falling back to direct stream", "upstream", upstreamURL)
					s.metrics.recordSpoolFollowerWait(ctx, repoName, "writer_failed", wait)
					return s.streamSnapshotDirect(w, r, repo)
				}
				s.metrics.recordSpoolFollowerWait(ctx, repoName, "read_error", wait)
				return errors.Wrap(err, "snapshot spool read")
			}
			s.metrics.recordSpoolFollowerWait(ctx, repoName, "served", wait)
			s.metrics.recordSnapshotServe(ctx, "spool", repoName, spool.Written(), time.Since(start))
			if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
				span.SetAttributes(attribute.String("cachew.source", "spool"), attribute.Int64("cachew.bytes", spool.Written()),
					attribute.Float64("cachew.spool_wait_seconds", wait.Seconds()))
			}
			return nil
		}
		// Writer failed; fall through to generate independently.
		s.metrics.recordSpoolFollowerWait(ctx, repoName, "writer_failed", wait)
		return s.streamSnapshotDirect(w, r, repo)
	}

	err := s.writeSnapshotSpool(w, r, repo, upstreamURL, repoName, entry)
	if err == nil {
		s.metrics.recordSnapshotServe(ctx, "generated", repoName, entry.spool.Written(), time.Since(start))
		if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
			span.SetAttributes(attribute.String("cachew.source", "generated"), attribute.Int64("cachew.bytes", entry.spool.Written()))
		}
	}
	return err
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
func (s *Strategy) writeSnapshotSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL, repoName string, entry *snapshotSpoolEntry) error {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	writerStart := time.Now()
	spool, spoolDir, repoDir, err := s.prepareSnapshotSpool(ctx, repo, upstreamURL, entry)
	if err != nil {
		s.metrics.recordSpoolWriter(ctx, repoName, "prepare_error", time.Since(writerStart))
		return errors.Wrap(err, "prepare snapshot spool")
	}
	snapshotDir := filepath.Dir(repoDir)

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	tw := NewSpoolTeeWriter(w, spool)
	streamErr := snapshot.StreamTo(ctx, tw, repoDir, nil, s.config.ZstdThreads)
	if streamErr != nil {
		spool.MarkError(streamErr)
		s.metrics.recordSpoolWriter(ctx, repoName, "error", time.Since(writerStart))
	} else {
		spool.MarkComplete()
		s.metrics.recordSpoolWriter(ctx, repoName, "success", time.Since(writerStart))
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

	s.scheduler.Submit(upstream, "deferred-mirror-restore", func(ctx context.Context) error {
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

		if err := repo.FetchLenient(ctx, s.cloneManager.Config().CloneTimeout); err != nil {
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
	})
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

// generateAndUploadLFSSnapshot fetches only the LFS objects needed to check out
// the repository's default branch (HEAD) and archives them as a separate tar.zst
// served at /git/{repo}/lfs-snapshot.tar.zst.
//
// Only objects referenced by the current HEAD tree are included — historical
// versions of LFS-tracked files are excluded to keep the archive small.
//
// The archive stores paths relative to .git/ (e.g. ./lfs/objects/xx/yy/sha256) so that
// the client can extract it directly into the repo's .git/ directory.
func (s *Strategy) generateAndUploadLFSSnapshot(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	upstream := repo.UpstreamURL()
	ctx, span := tracer.Start(ctx, "git.snapshot.generate_lfs",
		trace.WithAttributes(
			attribute.String("cachew.operation", "lfs_snapshot_generate"),
			attribute.String("cachew.upstream", upstream),
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

	// Check if any .gitattributes file at HEAD declares filter=lfs. This searches
	// the root and all nested .gitattributes, avoiding false negatives for repos
	// that only configure LFS in subdirectories.
	discoverStart := time.Now()
	repoPath := repo.Path()
	grepCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "grep", "-q", "filter=lfs", "HEAD", "--", "*.gitattributes") //nolint:gosec
	if err := grepCmd.Run(); err != nil {
		// git grep exits 1 for "no match" (legitimate "no LFS in this repo");
		// any other non-zero exit (invalid HEAD, repo corruption, command
		// failure) is a real error that should propagate so we don't silently
		// skip LFS snapshot generation for repos that actually use LFS.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			s.metrics.recordLFSPhase(ctx, upstream, "discover", "skipped", time.Since(discoverStart))
			logger.DebugContext(ctx, "No LFS filter in any .gitattributes, skipping LFS snapshot", "upstream", upstream)
			return nil
		}
		s.metrics.recordLFSPhase(ctx, upstream, "discover", "error", time.Since(discoverStart))
		return errors.Wrap(err, "git grep for LFS filter")
	}
	s.metrics.recordLFSPhase(ctx, upstream, "discover", "success", time.Since(discoverStart))

	start := time.Now()
	logger.InfoContext(ctx, "LFS snapshot generation started", "upstream", upstream)

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := lfsSnapshotCacheKey(upstream)
	excludePatterns := []string{"*.lock"}
	cloneStart := time.Now()
	cloneRecorded := false
	if err := s.withSnapshotClone(ctx, repo, "lfs", func(workDir string) error {
		s.metrics.recordLFSPhase(ctx, upstream, "clone", "success", time.Since(cloneStart))
		cloneRecorded = true

		// Set up LFS in the snapshot clone. cloneForSnapshot already restores
		// remote.origin.url to the upstream URL, so LFS will fetch from GitHub.
		// #nosec G204
		if output, err := exec.CommandContext(ctx, "git", "-C", workDir,
			"lfs", "install", "--local").CombinedOutput(); err != nil {
			logger.WarnContext(ctx, "git lfs install --local failed (non-fatal)", "upstream", upstream, "error", err,
				"output", string(output))
		}

		// Fetch only the LFS objects referenced by HEAD (the default branch).
		// Timeout must stay below githubapp.RefreshBuffer (30m) so the baked-in
		// token can't expire mid-fetch and trigger a retry storm.
		fetchStart := time.Now()
		fetchCtx, cancel := context.WithTimeout(ctx, lfsFetchTimeout)
		fetchCmd, err := repo.GitCommand(fetchCtx, "-C", workDir, "lfs", "fetch", "origin", "HEAD")
		if err != nil {
			cancel()
			s.metrics.recordLFSPhase(ctx, upstream, "fetch", "error", time.Since(fetchStart))
			return errors.Wrap(err, "create git lfs fetch command")
		}
		// git-lfs spawns transfer helpers that inherit our pipes; without
		// killing the whole group, CombinedOutput stays blocked after the
		// timeout fires.
		fetchCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		fetchCmd.Cancel = func() error {
			return syscall.Kill(-fetchCmd.Process.Pid, syscall.SIGKILL)
		}
		output, fetchErr := fetchCmd.CombinedOutput()
		cancel()
		if fetchErr != nil {
			s.metrics.recordLFSPhase(ctx, upstream, "fetch", "error", time.Since(fetchStart))
			return errors.Wrapf(fetchErr, "git lfs fetch: %s", string(output))
		}
		s.metrics.recordLFSPhase(ctx, upstream, "fetch", "success", time.Since(fetchStart))

		lfsDir := filepath.Join(workDir, ".git", "lfs")
		if _, err := os.Stat(lfsDir); os.IsNotExist(err) {
			logger.InfoContext(ctx, "No LFS objects in repository, skipping LFS snapshot", "upstream", upstream)
			return nil
		}
		// Record .git/lfs size as a proxy for "bytes fetched". Best-effort:
		// surface 0 on error so we don't fail the snapshot for a stat walk.
		if size, walkErr := dirSizeBytes(lfsDir); walkErr == nil {
			s.metrics.recordLFSPhaseBytes(ctx, upstream, "fetch", size)
		} else {
			logger.DebugContext(ctx, "Failed to size .git/lfs after fetch", "upstream", upstream, "error", walkErr)
		}

		gitDir := filepath.Join(workDir, ".git")
		archiveStart := time.Now()
		if err := snapshot.CreatePaths(ctx, s.cache, cacheKey, gitDir, "lfs", []string{"lfs"}, 0, excludePatterns, s.config.ZstdThreads); err != nil {
			s.metrics.recordLFSPhase(ctx, upstream, "archive_upload", "error", time.Since(archiveStart))
			return err //nolint:wrapcheck // wrapped by caller
		}
		s.metrics.recordLFSPhase(ctx, upstream, "archive_upload", "success", time.Since(archiveStart))
		return nil
	}); err != nil {
		if !cloneRecorded {
			s.metrics.recordLFSPhase(ctx, upstream, "clone", "error", time.Since(cloneStart))
		}
		s.metrics.recordOperation(ctx, "lfs-snapshot", "error", time.Since(start))
		return errors.Wrap(err, "create LFS snapshot")
	}

	s.metrics.recordOperation(ctx, "lfs-snapshot", "success", time.Since(start))
	logger.InfoContext(ctx, "LFS snapshot generation completed", "upstream", upstream)
	return nil
}

// dirSizeBytes returns the total size in bytes of regular files under root.
// Per-entry stat or walk errors are deliberately swallowed so a transient
// failure (e.g. a file removed mid-walk during snapshot prep) doesn't fail
// the surrounding snapshot operation; the returned sum is best-effort.
func dirSizeBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort: skip unreadable entries
		}
		// Stat first so we don't drop files on filesystems where DirEntry.Type()
		// reports "unknown" (e.g. some NFS/FUSE setups) and IsRegular() returns false.
		info, infoErr := d.Info()
		if infoErr != nil {
			return nil //nolint:nilerr // best-effort: skip un-stat-able entries
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total, errors.WithStack(err)
}

func (s *Strategy) handleLFSSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/lfs-snapshot.tar.zst"))
	upstreamURL := "https://" + host + "/" + repoPath
	cacheKey := lfsSnapshotCacheKey(upstreamURL)

	// Try cache first so we can serve even when the mirror isn't ready (cold start).
	reader, headers, err := s.cache.Open(ctx, cacheKey, 0, -1)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to open LFS snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if reader != nil {
		defer reader.Close()
		logger.DebugContext(ctx, "Serving cached LFS snapshot", "upstream", upstreamURL)
		if err := s.streamSnapshotArtifact(ctx, w, r, reader, headers); err != nil {
			logger.ErrorContext(ctx, "Failed to stream LFS snapshot", "upstream", upstreamURL, "error", err)
		}
		return
	}

	// Cache miss — return 404 immediately rather than blocking on mirror
	// restore + on-demand generation. Kick off a background mirror warm so
	// the periodic LFS snapshot job can fire once the mirror is ready.
	logger.InfoContext(ctx, "LFS snapshot cache miss, triggering background warm", "upstream", upstreamURL)
	if repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL); repoErr == nil && repo.State() != gitclone.StateReady {
		s.scheduler.Submit(upstreamURL, "lfs-mirror-warm", func(ctx context.Context) error {
			if err := s.startClone(ctx, repo); err != nil {
				logger.WarnContext(ctx, "Background mirror warm for LFS failed", "upstream", upstreamURL, "error", err)
			}
			return nil
		})
	}
	http.Error(w, "LFS snapshot not found", http.StatusNotFound)
}
