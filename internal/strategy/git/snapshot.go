package git

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/block/cachew/internal/spool"
)

const lfsFetchTimeout = 25 * time.Minute

const bundleBuildTimeout = 5 * time.Minute

// bundleTipHeader records the mirror HEAD a cached bundle was built up to.
const bundleTipHeader = "X-Cachew-Bundle-Tip"

type bundleBuild struct {
	done          chan struct{}
	verifiedEmpty bool
	file          *os.File
	size          int64
	refs          atomic.Int64
}

func newBundleBuild() *bundleBuild {
	build := &bundleBuild{done: make(chan struct{})}
	build.refs.Store(1)
	return build
}

func (b *bundleBuild) retain() bool {
	for refs := b.refs.Load(); refs > 0; refs = b.refs.Load() {
		if b.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
	return false
}

func (b *bundleBuild) release() {
	if b.refs.Add(-1) == 0 && b.file != nil {
		_ = b.file.Close()
	}
}

func (b *bundleBuild) reader() *io.SectionReader {
	return io.NewSectionReader(b.file, 0, b.size)
}

func (s *Strategy) finishBundleBuild(key cache.Key, build *bundleBuild) {
	if build.file == nil {
		s.bundleBuilds.Delete(key)
		close(build.done)
	}
	build.release()
}

func (s *Strategy) publishBundle(ctx context.Context, key cache.Key, build *bundleBuild, file *os.File, tip string) error {
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return errors.Wrap(err, "stat generated bundle")
	}
	build.file, build.size = file, info.Size()
	build.retain()
	close(build.done)
	deadline, _ := ctx.Deadline()
	go func() {
		defer build.release()
		defer s.bundleBuilds.Delete(key)
		publishCtx, cancel := context.WithDeadline(s.ctx, deadline)
		defer cancel()
		if err := s.cacheBundle(publishCtx, key, tip, build.reader()); err != nil {
			logging.FromContext(ctx).WarnContext(publishCtx, "Failed to cache bundle", "key", key, "error", err)
		}
	}()
	return nil
}

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

// commitSHARe matches a full SHA-1 or SHA-256 commit hash, so untrusted bundle bases are never passed to git.
var commitSHARe = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

var errEmptyBundle = errors.New("bundle contains no commits")

func lfsSnapshotCacheKey(upstreamURL string) cache.Key {
	return cache.NewKey(upstreamURL + ".lfs-snapshot")
}

// cloneForSnapshot clones the mirror into destDir under repo's read lock, then resets the remote URL.
//
// A non-empty filter (e.g. blob:none) produces a partial clone with full history but only the HEAD checkout's blobs.
func (s *Strategy) cloneForSnapshot(ctx context.Context, repo *gitclone.Repository, destDir, filter string) error {
	if err := repo.WithReadLock(func() error {
		args := []string{"-c", "core.hooksPath=" + os.DevNull, "clone", "--template="}
		if filter != "" {
			// Local (hardlink) clones copy the whole object store and silently ignore --filter.
			//
			// --no-local forces the transport path so the filter applies and unreachable refs/pull objects are omitted.
			args = append(args, "--no-local", "--filter="+filter)
		}
		args = append(args, repo.Path(), destDir)
		// #nosec G204 - repo.Path() and destDir are controlled by us
		cmd := exec.CommandContext(ctx, "git", args...)
		// Repository files choose checkout filters, while inherited templates can add hooks that leak into snapshots.
		//
		// LC_ALL=C pins git's messages to English so the filter fallback warning below is detectable in any locale.
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull,
			"GIT_CONFIG_COUNT=0", "GIT_CONFIG_PARAMETERS=", "GIT_TEMPLATE_DIR=", "GIT_LFS_SKIP_SMUDGE=1", "LC_ALL=C")
		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "git clone for snapshot: %s", string(output))
		}

		// Fail rather than cache a full-size artifact when the mirror ignores the filter.
		//
		// Git then falls back to a full clone with only a warning, yet still writes promisor and partialclonefilter
		// config as if the filter applied. Only the warning is a reliable signal.
		if filter != "" && strings.Contains(string(output), "filtering not recognized by server") {
			return errors.Errorf("snapshot filter %q was ignored by the mirror (uploadpack.allowFilter unset?)", filter)
		}

		// Restore remote.origin.url from the local mirror path to the upstream URL.
		//
		// Clients route through cachew with insteadOf, so a cachew URL here would couple snapshots to one instance.
		// #nosec G204 - upstreamURL is derived from controlled inputs
		cmd = exec.CommandContext(ctx, "git", "-C", destDir, "remote", "set-url", "origin", repo.UpstreamURL())
		if output, err = cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "fix snapshot remote URL: %s", string(output))
		}
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (s *Strategy) snapshotFilterFor(upstreamURL string) string {
	if len(s.config.SnapshotFilters) == 0 {
		return ""
	}
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return ""
	}
	return s.config.SnapshotFilters[repoPath]
}

func (s *Strategy) withSnapshotClone(ctx context.Context, repo *gitclone.Repository, suffix, filter string, fn func(workDir string) error) error {
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

	if err := s.cloneForSnapshot(ctx, repo, workDir, filter); err != nil {
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

func snapshotExcludePatterns(gitDir string) []string {
	return []string{gitDir + "/*.lock", gitDir + "/objects/pack/tmp_*", gitDir + "/objects/pack/.tmp-*"}
}

// checkRepackHealthy refuses snapshots of a mirror with a running or failed repack.
//
// A failed repack may leave duplicate objects, which snapshots would copy. The error releases
// the coordination claim so a healthy replica can publish instead.
func checkRepackHealthy(repo *gitclone.Repository) error { //nolint:privatedoc // explains why
	if repo.RepackPending() {
		return errors.Errorf("mirror for %s has an unfinished repack and may contain duplicate packs", repo.UpstreamURL())
	}
	return nil
}

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) (commit string, returnErr error) {
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

	filter := s.snapshotFilterFor(upstream)
	if filter != "" {
		span.SetAttributes(attribute.String("cachew.snapshot.filter", filter))
	}
	logger.InfoContext(ctx, "Snapshot generation started", "upstream", upstream, "filter", filter)
	if err := checkRepackHealthy(repo); err != nil {
		return "", err
	}

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := snapshotCacheKey(upstream)
	if head := s.getMirrorHead(ctx, repo); s.snapshotUnchanged(ctx, snapshotJobBase, cacheKey, upstream, head) {
		s.metrics.recordOperation(ctx, "snapshot", "unchanged", time.Since(start))
		logger.InfoContext(ctx, "Snapshot unchanged, skipping generation", "upstream", upstream, "commit", head)
		return "", nil
	}
	if err := s.withSnapshotClone(ctx, repo, "base", filter, func(workDir string) error {
		// The snapshot's HEAD is the base for later delta bundles against the current mirror.
		headSHA, err := mirrorHead(ctx, workDir)
		if err != nil {
			return errors.Wrap(err, "rev-parse HEAD for snapshot")
		}
		commit = headSHA
		extraHeaders := http.Header{}
		extraHeaders.Set("X-Cachew-Snapshot-Commit", headSHA)

		return snapshot.Create(ctx, s.cache, cacheKey, workDir, 0, snapshotExcludePatterns("./.git"), s.config.ZstdThreads, extraHeaders)
	}); err != nil {
		return "", errors.Wrap(err, "create snapshot")
	}

	s.metrics.recordOperation(ctx, "snapshot", "success", time.Since(start))
	logger.InfoContext(ctx, "Snapshot generation completed", "upstream", upstream)
	return commit, nil
}

// generateAndUploadMirrorSnapshot snapshots the bare mirror itself, so it restores as a mirror without conversion.
//
// This bootstraps new pods: they restore it and serve immediately while a background fetch freshens it.
func (s *Strategy) generateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) (commit string, returnErr error) {
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
	if err := checkRepackHealthy(repo); err != nil {
		return "", err
	}

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	cacheKey := mirrorSnapshotCacheKey(upstream)
	excludePatterns := snapshotExcludePatterns(".")

	// Exclude fetches while reading HEAD and tar-ing, or a fetch can replace packed-refs mid-read and truncate it.
	//
	// The unchanged check runs under the same exclusion so a fetch cannot advance the mirror between check and tar.
	var skipped bool
	if err := repo.WithFetchExclusion(ctx, func() error {
		return repo.WithReadLock(func() error {
			// HEAD is a proxy for the whole mirror, so a skip may miss branch updates for up to snapshot-max-age.
			//
			// Restored pods fetch immediately, which covers that drift.
			commit = s.getMirrorHead(ctx, repo)
			if s.snapshotUnchanged(ctx, snapshotJobMirror, cacheKey, upstream, commit) {
				skipped = true
				return nil
			}
			return snapshot.Create(ctx, s.cache, cacheKey, repo.Path(), 0, excludePatterns, s.config.ZstdThreads)
		})
	}); err != nil {
		return "", errors.Wrap(err, "create mirror snapshot")
	}
	if skipped {
		logger.InfoContext(ctx, "Mirror snapshot unchanged, skipping generation", "upstream", upstream, "commit", commit)
		return "", nil
	}

	logger.InfoContext(ctx, "Mirror snapshot generation completed", "upstream", upstream)
	return commit, nil
}

// Coordinated snapshot job names, shared by scheduling and each generator's unchanged-skip check.
const (
	snapshotJobBase   = "snapshot"
	snapshotJobLFS    = "lfs-snapshot"
	snapshotJobMirror = "mirror-snapshot"
)

const coldPreparationSpread = 30 * time.Second

const coldPreparationQueueLimit = 128

const snapshotClaimCleanupTimeout = 30 * time.Second

// snapshotUnchanged reports whether generation can be skipped.
//
// The last generation must have captured the same commit recently, and the entry must still exist
// in the authoritative tier: a local copy can outlive a lost shared object and mask the loss.
func (s *Strategy) snapshotUnchanged(ctx context.Context, job string, key cache.Key, upstream, head string) bool { //nolint:privatedoc // explains why
	if !s.snapshotCoord.Load().Unchanged(job, upstream, head, s.config.SnapshotMaxAge) {
		return false
	}
	_, err := cache.StatAuthoritative(ctx, s.cache, key)
	return err == nil
}

// snapshotStartupSpread staggers each replica's first coordinated snapshot run after registration.
//
// A pod with no recorded last run starts periodic jobs at once, so on deploy every replica would otherwise decide
// within the same metadata sync window, before peers' claims propagate.
const snapshotStartupSpread = 5 * time.Minute

func (s *Strategy) scheduleSnapshotJobs(repo *gitclone.Repository) {
	if _, preparing := s.mirrorPreparations.Load(repo.UpstreamURL()); preparing {
		return
	}
	s.scheduleSnapshotJobsAfter(repo, 0)
}

func (s *Strategy) scheduleSnapshotJobsAfter(repo *gitclone.Repository, baseDelay time.Duration) {
	upstream := repo.UpstreamURL()
	if _, loaded := s.snapshotJobsScheduled.LoadOrStore(upstream, true); loaded {
		return
	}
	submit := func(job string, interval time.Duration, generate func(ctx context.Context) (string, error)) {
		run := s.coordinatedSnapshotJob(job, repo, interval, generate)
		delay, interval := s.snapshotSchedule(interval)
		if job == snapshotJobBase {
			delay = max(delay, baseDelay)
		}
		if delay == 0 {
			s.scheduler.SubmitPeriodicJob(upstream, job+"-periodic", interval, run)
			return
		}
		// A timer firing during shutdown is harmless: SubmitPeriodicJob is then a no-op.
		time.AfterFunc(delay, func() {
			s.scheduler.SubmitPeriodicJob(upstream, job+"-periodic", interval, run)
		})
	}
	submit(snapshotJobBase, s.config.SnapshotInterval, func(ctx context.Context) (string, error) {
		if err := s.doFetch(ctx, repo); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Pre-snapshot fetch failed", "upstream", upstream, "error", err)
		}
		return s.generateAndUploadSnapshot(ctx, repo)
	})
	submit(snapshotJobLFS, s.config.SnapshotInterval, func(ctx context.Context) (string, error) {
		return s.generateAndUploadLFSSnapshot(ctx, repo)
	})
	mirrorInterval := s.config.MirrorSnapshotInterval
	if mirrorInterval == 0 {
		mirrorInterval = s.config.SnapshotInterval
	}
	submit(snapshotJobMirror, mirrorInterval, func(ctx context.Context) (string, error) {
		return s.generateAndUploadMirrorSnapshot(ctx, repo)
	})
}

func startupSpreadDelay() time.Duration {
	return rand.N(snapshotStartupSpread) //nolint:gosec // scheduling jitter needs no cryptographic randomness
}

// snapshotSchedule applies spread and jitter only when a coordinator is configured.
//
// They only help when replicas coordinate through shared metadata. Without a coordinator,
// registration stays immediate at the exact configured interval.
func (s *Strategy) snapshotSchedule(interval time.Duration) (delay, jittered time.Duration) { //nolint:privatedoc // explains why
	if s.snapshotCoord.Load() == nil {
		return 0, interval
	}
	return startupSpreadDelay(), jitterInterval(interval)
}

// coordinatedSnapshotJob skips generation when another replica generated the artifact recently or is generating it.
//
// Coordination fails open so a broken metadata store never stops generation. An empty commit from generate means
// nothing was uploaded, which must not count as a completion or the max-age bound drifts from the last upload.
func (s *Strategy) coordinatedSnapshotJob(job string, repo *gitclone.Repository, interval time.Duration, generate func(ctx context.Context) (string, error)) func(ctx context.Context) error {
	upstream := repo.UpstreamURL()
	return func(ctx context.Context) error {
		logger := logging.FromContext(ctx)
		coord := s.snapshotCoord.Load()
		claimID, claimed, err := coord.ClaimWithTTL(job, upstream, interval, snapshotClaimTTL)
		if err != nil {
			logger.WarnContext(ctx, "Snapshot coordination claim failed, generating anyway", "job", job, "upstream", upstream, "error", err)
			claimID = ""
		} else if !claimed {
			logger.DebugContext(ctx, "Skipping snapshot generation, fresh or in progress on another replica", "job", job, "upstream", upstream)
			return nil
		}
		commit, err := generate(ctx)
		if err != nil {
			return errors.Join(err, s.failSnapshotClaim(ctx, job, upstream, claimID))
		}
		if commit == "" {
			// Record a skip, not a completion, so CompletedAt keeps tracking the last actual upload.
			//
			// The skip's CheckedAt still keeps peers from re-fetching an unchanged repo every interval.
			if err := coord.SkipClaim(ctx, job, upstream, claimID); err != nil {
				logger.WarnContext(ctx, "Failed to record snapshot skip", "job", job, "upstream", upstream, "error", err)
				if releaseErr := s.failSnapshotClaim(ctx, job, upstream, claimID); releaseErr != nil {
					logger.WarnContext(ctx, "Failed to release snapshot claim after skip error", "job", job,
						"upstream", upstream, "error", releaseErr)
				}
			}
			return nil
		}
		if err := coord.CompleteClaim(ctx, job, upstream, claimID, commit); err != nil {
			logger.WarnContext(ctx, "Failed to record snapshot completion", "job", job, "upstream", upstream, "error", err)
			if releaseErr := s.failSnapshotClaim(ctx, job, upstream, claimID); releaseErr != nil {
				logger.WarnContext(ctx, "Failed to release snapshot claim after completion error", "job", job,
					"upstream", upstream, "error", releaseErr)
			}
		}
		return nil
	}
}

func (s *Strategy) failSnapshotClaim(ctx context.Context, job, upstream, claimID string) error {
	if claimID == "" {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotClaimCleanupTimeout)
	defer cancel()
	return s.snapshotCoord.Load().Fail(cleanupCtx, job, upstream, claimID)
}

// jitterInterval spreads replicas' periodic snapshot schedules apart.
//
// Coordination claims sync asynchronously, so they must propagate before a peer decides whether to
// generate. Without jitter, replicas deployed together tick in lockstep and race in the sync window.
func jitterInterval(interval time.Duration) time.Duration { //nolint:privatedoc // explains why
	if interval <= 0 {
		return interval
	}
	return interval + rand.N(interval/8) //nolint:gosec // scheduling jitter needs no cryptographic randomness
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

	// HEAD probes use cache metadata alone, so they never read or generate the body or warm up a mirror.
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

	// On cold start, stream a cached snapshot while the mirror restores in the background.
	//
	// This avoids blocking the client behind the full S3-download, extract and git-fetch pipeline.
	if repo.State() != gitclone.StateReady {
		entry := &coldSnapshotEntry{done: make(chan struct{})}
		if existing, loaded := s.coldSnapshotMu.LoadOrStore(upstreamURL, entry); loaded {
			winner := existing.(*coldSnapshotEntry)
			<-winner.done
			reader, headers, openErr := s.cache.Open(ctx, cacheKey, httputil.ConditionalOptions(r)...)
			if !errors.Is(openErr, os.ErrNotExist) {
				winner.serving.Add(1)
				served, serveErr := s.serveOpenedSnapshot(ctx, w, reader, headers, openErr, repoName, "cold_cache", start)
				winner.serving.Done()
				if serveErr != nil {
					logger.WarnContext(ctx, "Failed to serve locally cached snapshot after waiting for in-flight fill", "upstream", upstreamURL, "error", serveErr)
					span.RecordError(serveErr)
				}
				if served {
					logger.InfoContext(ctx, "Served locally cached snapshot after waiting for in-flight fill", "upstream", upstreamURL)
					return
				}
			}
		} else {
			defer func() {
				close(entry.done)
				s.coldSnapshotMu.Delete(upstreamURL)
			}()
			reader, headers, openErr := s.cache.Open(ctx, cacheKey, httputil.ConditionalOptions(r)...)
			if !errors.Is(openErr, os.ErrNotExist) {
				served, serveErr := s.serveOpenedSnapshot(ctx, w, reader, headers, openErr, repoName, "cold_cache", start)
				if serveErr != nil {
					logger.WarnContext(ctx, "Failed to serve cached snapshot while mirror warms up", "upstream", upstreamURL, "error", serveErr)
					span.RecordError(serveErr)
				}
				if served {
					logger.InfoContext(ctx, "Served cached snapshot while mirror warms up", "upstream", upstreamURL)
					s.scheduleDeferredMirrorRestore(ctx, repo, entry)
					return
				}
			}
		}
	}

	if repo.State() != gitclone.StateReady {
		s.scheduleColdSnapshotPreparation(ctx, repo)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Snapshot not cached", http.StatusNotFound)
		return
	}

	// Forward all conditional and range headers: the cache resolves If-Match and If-None-Match before Range.
	//
	// So 304/412 take precedence over a satisfied range or a 416 (RFC 7232 §3, RFC 7233 §3.1).
	reader, headers, err := s.cache.Open(ctx, cacheKey, httputil.ConditionalOptions(r)...)
	switch {
	case err == nil,
		errors.Is(err, cache.ErrNotModified),
		errors.Is(err, cache.ErrPreconditionFailed),
		errors.Is(err, cache.ErrRangeNotSatisfiable):
		if serveErr := s.serveSnapshotWithBundle(ctx, w, r, reader, headers, err, repo, upstreamURL, repoName, start); serveErr != nil {
			logger.ErrorContext(ctx, "Failed to serve snapshot", "upstream", upstreamURL, "error", serveErr)
			span.RecordError(serveErr)
			span.SetStatus(codes.Error, serveErr.Error())
		}
	case errors.Is(err, os.ErrNotExist):
		_, preparing := s.mirrorPreparations.Load(upstreamURL)
		if preparing || checkRepackHealthy(repo) != nil {
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "Snapshot not cached", http.StatusNotFound)
			return
		}
		if spoolErr := s.serveSnapshotWithSpool(w, r, repo, upstreamURL, repoName, start); spoolErr != nil {
			logger.ErrorContext(ctx, "Failed to serve snapshot via spool", "upstream", upstreamURL, "error", spoolErr)
			span.RecordError(spoolErr)
			span.SetStatus(codes.Error, spoolErr.Error())
			if ctx.Err() == nil {
				panic(http.ErrAbortHandler)
			}
		}
	default:
		logger.ErrorContext(ctx, "Failed to open snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// serveSnapshotHead answers a HEAD request from cache metadata, honouring preconditions against the cached ETag.
//
// An uncached snapshot yields 404 rather than the expensive on-demand generation that GET does.
func (s *Strategy) serveSnapshotHead(ctx context.Context, w http.ResponseWriter, r *http.Request, cacheKey cache.Key, repoName string, start time.Time) {
	headers, err := s.cache.Stat(ctx, cacheKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			w.Header().Set("Cache-Control", "no-store")
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
	if !commitSHARe.MatchString(base) {
		http.Error(w, "base query parameter must be a full commit SHA", http.StatusBadRequest)
		span.SetAttributes(attribute.String("cachew.source", "bad_request"))
		return
	}
	span.SetAttributes(attribute.String("cachew.base_commit", base))
	// The client's ensure-refs may have reached another replica; tip is the HEAD it saw there.
	want := r.URL.Query().Get("tip")
	if !commitSHARe.MatchString(want) {
		want = ""
	}

	bKey := bundleCacheKey(upstreamURL, base)

	// Source and bytes are recorded by the deferred metric call.
	source := "miss"
	var bytes int64
	defer func() {
		span.SetAttributes(attribute.String("cachew.source", source), attribute.Int64("cachew.bytes", bytes))
		s.metrics.recordBundleServe(ctx, source, repoName, bytes, time.Since(start))
	}()

	serveGenerated := func(build *bundleBuild) {
		w.Header().Set("Content-Type", "application/x-git-bundle")
		n, err := io.Copy(w, build.reader())
		bytes, source = n, "generated"
		if err != nil {
			logger.WarnContext(ctx, "Failed to stream bundle", "upstream", upstreamURL, "error", err)
			span.RecordError(err)
		}
	}
	build := newBundleBuild()
	releaseBuild := func() {}
	for acquired := false; ; {
		reader, headers, openErr := s.cache.Open(ctx, bKey, httputil.ConditionalOptions(r)...)
		// Resumed downloads must keep reading the entry they started on, so only plain GETs re-check the tip.
		if openErr == nil && r.Header.Get("Range") == "" {
			stale, staleErr := s.cachedBundleStale(ctx, s.cloneManager.Get(upstreamURL), headers, want)
			if staleErr != nil {
				logger.WarnContext(ctx, "Failed to freshen mirror for bundle tip", "upstream", upstreamURL, "tip", want, "error", staleErr)
			}
			if stale {
				_ = reader.Close()
				openErr = os.ErrNotExist
			}
		}
		switch {
		case openErr == nil,
			errors.Is(openErr, cache.ErrNotModified),
			errors.Is(openErr, cache.ErrPreconditionFailed),
			errors.Is(openErr, cache.ErrRangeNotSatisfiable):
			releaseBuild()
			decorate := func(rw http.ResponseWriter, _ http.Header) {
				rw.Header().Set("Content-Type", "application/x-git-bundle")
			}
			_, n, serveErr := httputil.ServeCacheHit(w, headers, reader, openErr, httputil.WithResponseDecorator(decorate))
			bytes = n
			source = "cache"
			if serveErr != nil {
				logger.WarnContext(ctx, "Failed to stream cached bundle", "upstream", upstreamURL, "error", serveErr)
				span.RecordError(serveErr)
			}
			return
		}
		if acquired {
			break
		}
		if existing, loaded := s.bundleBuilds.LoadOrStore(bKey, build); loaded {
			pending := existing.(*bundleBuild)
			if !pending.retain() {
				continue
			}
			waitCtx, cancel := context.WithDeadline(ctx, start.Add(bundleBuildTimeout))
			select {
			case <-pending.done:
			case <-waitCtx.Done():
			}
			err := waitCtx.Err()
			cancel()
			if err != nil {
				pending.release()
				http.Error(w, "Bundle not available", http.StatusServiceUnavailable)
				span.RecordError(err)
				return
			}
			if pending.verifiedEmpty {
				pending.release()
				source = "up_to_date"
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if pending.file != nil {
				defer pending.release()
				serveGenerated(pending)
				return
			}
			pending.release()
		} else {
			acquired = true
			releaseBuild = sync.OnceFunc(func() {
				s.finishBundleBuild(bKey, build)
			})
			defer releaseBuild()
		}
	}

	ctx, cancel := context.WithDeadline(ctx, start.Add(bundleBuildTimeout))
	defer cancel()

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

	if !repo.HasCommit(ctx, base) {
		freshenErr := s.freshenMirror(ctx, repo)
		if freshenErr != nil {
			logger.WarnContext(ctx, "Failed to freshen mirror for bundle", "upstream", upstreamURL, "error", freshenErr)
			span.RecordError(freshenErr)
		}
		if !repo.HasCommit(ctx, base) {
			if freshenErr != nil {
				source = "miss_stale"
			} else {
				source = "miss_bad_base"
			}
			logger.WarnContext(ctx, "Bundle base not in mirror after freshen", "upstream", upstreamURL, "base", base)
			http.Error(w, "Bundle not available", http.StatusNotFound)
			return
		}
	}

	if err := s.freshenForBundleTip(ctx, repo, want); err != nil {
		logger.WarnContext(ctx, "Failed to freshen mirror for bundle tip", "upstream", upstreamURL, "tip", want, "error", err)
	}
	bundleFile, tip, err := s.createBundle(ctx, repo, base)
	if errors.Is(err, errEmptyBundle) {
		if freshenErr := s.doFetchVerified(ctx, repo); freshenErr != nil {
			source = "miss_stale"
			logger.WarnContext(ctx, "Failed to verify mirror for empty bundle", "upstream", upstreamURL, "error", freshenErr)
			http.Error(w, "Bundle not available", http.StatusNotFound)
			span.RecordError(freshenErr)
			return
		}
		if !repo.HasCommit(ctx, base) {
			source = "miss_bad_base"
			logger.WarnContext(ctx, "Bundle base not in mirror after verified fetch", "upstream", upstreamURL, "base", base)
			http.Error(w, "Bundle not available", http.StatusNotFound)
			return
		}
		bundleFile, tip, err = s.createBundle(ctx, repo, base)
		if errors.Is(err, errEmptyBundle) {
			build.verifiedEmpty = true
			releaseBuild()
			source = "up_to_date"
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	if err != nil {
		logger.WarnContext(ctx, "Failed to create bundle", "upstream", upstreamURL, "base", base, "error", err)
		http.Error(w, "Bundle not available", http.StatusNotFound)
		span.RecordError(err)
		return
	}
	if err := s.publishBundle(ctx, bKey, build, bundleFile, tip); err != nil {
		logger.WarnContext(ctx, "Failed to publish bundle", "upstream", upstreamURL, "error", err)
		http.Error(w, "Bundle not available", http.StatusServiceUnavailable)
		span.RecordError(err)
		return
	}
	serveGenerated(build)
}

func (s *Strategy) serveSnapshotWithBundle(ctx context.Context, w http.ResponseWriter, _ *http.Request, reader io.ReadCloser, headers http.Header, openErr error, repo *gitclone.Repository, upstreamURL, repoName string, start time.Time) error {
	// The commit comes from the stored headers; only the delta-bundle URL is computed per request.
	//
	// Both go on 200 and 206 responses so client.ParallelGet learns from its first chunk whether to apply a bundle.
	var snapshotCommit, bundleURL string
	if openErr == nil {
		snapshotCommit, bundleURL = s.snapshotMetadata(ctx, headers, repo, upstreamURL)
	}

	decorate := func(rw http.ResponseWriter, _ http.Header) {
		// Content-Type is fixed for snapshots regardless of what the backend recorded.
		rw.Header().Set("Content-Type", "application/zstd")
		if bundleURL != "" {
			rw.Header().Set("X-Cachew-Bundle-Url", bundleURL)
		}
	}

	// Only whole-snapshot downloads pregenerate a bundle; ranged chunks are served as-is.
	if openErr == nil && bundleURL != "" && headers.Get("Content-Range") == "" {
		s.pregenerateBundle(ctx, repo, upstreamURL, snapshotCommit)
	}

	handled, n, err := httputil.ServeCacheHit(w, headers, reader, openErr, httputil.WithResponseDecorator(decorate))
	if !handled {
		return errors.Wrap(openErr, "serve snapshot")
	}

	source := snapshotServeSource("cache", headers)
	s.metrics.recordSnapshotServe(ctx, source, repoName, n, time.Since(start))
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.source", source), attribute.Int64("cachew.bytes", n))
	}
	return errors.Wrap(err, "serve snapshot")
}

// serveOpenedSnapshot writes an already-opened cached snapshot to w, honouring Range and conditional requests.
//
// It is the cold-start path, with no mirror and so no bundle negotiation. Callers must not pass an
// os.ErrNotExist miss. served=false means an unexpected cache error; reader is always closed.
func (s *Strategy) serveOpenedSnapshot(ctx context.Context, w http.ResponseWriter, reader io.ReadCloser, headers http.Header, openErr error, repoName, source string, start time.Time) (served bool, err error) { //nolint:privatedoc // documents a non-obvious contract
	decorate := func(rw http.ResponseWriter, _ http.Header) {
		rw.Header().Set("Content-Type", "application/zstd")
	}
	handled, n, serveErr := httputil.ServeCacheHit(w, headers, reader, openErr, httputil.WithResponseDecorator(decorate))
	if !handled {
		if reader != nil {
			serveErr = errors.Join(serveErr, reader.Close())
		}
		return false, errors.Wrap(errors.Join(openErr, serveErr), "open cached snapshot")
	}
	source = snapshotServeSource(source, headers)
	s.metrics.recordSnapshotServe(ctx, source, repoName, n, time.Since(start))
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.source", source), attribute.Int64("cachew.bytes", n))
	}
	return true, errors.Wrap(serveErr, "serve cached snapshot")
}

// snapshotServeSource appends "_range" to the metric source label when a byte range was satisfied.
//
// Full and partial serves are then distinguishable.
func snapshotServeSource(base string, headers http.Header) string { //nolint:privatedoc // explains why
	if headers.Get("Content-Range") != "" {
		return base + "_range"
	}
	return base
}

// pregenerateBundle builds and caches the delta bundle in the background so any pod can serve it without regenerating.
func (s *Strategy) pregenerateBundle(ctx context.Context, repo *gitclone.Repository, upstreamURL, snapshotCommit string) {
	bKey := bundleCacheKey(upstreamURL, snapshotCommit)
	build := newBundleBuild()
	if _, loaded := s.bundleBuilds.LoadOrStore(bKey, build); loaded {
		return
	}
	go func() {
		defer s.finishBundleBuild(bKey, build)
		bgCtx, cancel := context.WithTimeout(s.ctx, bundleBuildTimeout)
		defer cancel()
		bgCtx = logging.ContextWithLogger(bgCtx, logging.FromContext(ctx))
		logger := logging.FromContext(bgCtx)
		if reader, headers, err := s.cache.Open(bgCtx, bKey); err == nil {
			_ = reader.Close()
			if stale, err := s.cachedBundleStale(bgCtx, repo, headers, ""); err == nil && !stale {
				return
			}
		}
		bundleFile, tip, err := s.createBundle(bgCtx, repo, snapshotCommit)
		if err != nil {
			if errors.Is(err, errEmptyBundle) {
				return
			}
			logger.WarnContext(bgCtx, "Failed to pre-generate bundle", "upstream", upstreamURL, "error", err)
			return
		}
		if err := s.publishBundle(bgCtx, bKey, build, bundleFile, tip); err != nil {
			logger.WarnContext(bgCtx, "Failed to publish bundle", "upstream", upstreamURL, "error", err)
		}
	}()
}

// snapshotMetadata reports the snapshot's commit and, when it trails HEAD, the delta-bundle URL.
//
// The URL is empty when the snapshot is at HEAD. It does not mutate the response, so the full and
// ranged serve paths can advertise the same freshen metadata.
func (s *Strategy) snapshotMetadata(ctx context.Context, headers http.Header, repo *gitclone.Repository, upstreamURL string) (snapshotCommit, bundleURL string) { //nolint:privatedoc // explains why
	snapshotCommit = headers.Get("X-Cachew-Snapshot-Commit")
	if snapshotCommit == "" {
		return "", ""
	}
	mirrorHead := s.getMirrorHead(ctx, repo)
	if mirrorHead == "" || snapshotCommit == mirrorHead {
		return snapshotCommit, ""
	}
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return snapshotCommit, ""
	}
	return snapshotCommit, fmt.Sprintf("/git/%s/snapshot.bundle?base=%s", repoPath, snapshotCommit)
}

// applySnapshotCacheHeaders forwards the cached snapshot's validators.
//
// Clients revalidate with the ETag and size the transfer with Content-Length. Content-Type is
// fixed for snapshots regardless of what the cache backend recorded.
func applySnapshotCacheHeaders(w http.ResponseWriter, headers http.Header) { //nolint:privatedoc // explains why
	w.Header().Set("Content-Type", "application/zstd")
	if etag := headers.Get(cache.ETagKey); etag != "" {
		w.Header().Set(cache.ETagKey, etag)
	}
	if contentLength := headers.Get("Content-Length"); contentLength != "" {
		w.Header().Set("Content-Length", contentLength)
	}
}

func (s *Strategy) cacheBundle(ctx context.Context, key cache.Key, tip string, r io.Reader) error {
	headers := http.Header{"Content-Type": {"application/x-git-bundle"}}
	if tip != "" {
		headers.Set(bundleTipHeader, tip)
	}
	wc, err := s.cache.Create(ctx, key, headers, s.config.BundleCacheTTL)
	if err != nil {
		return errors.Wrap(err, "create cache entry")
	}
	if _, err := io.Copy(wc, r); err != nil {
		return errors.Join(errors.Wrap(err, "write bundle to cache"), wc.Abort(err))
	}
	return errors.Wrap(wc.Close(), "close bundle cache writer")
}

func mirrorHead(ctx context.Context, repoDir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "rev-parse", "HEAD") // #nosec G204 G702
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Wrap(err, "git rev-parse HEAD")
	}
	return strings.TrimSpace(string(output)), nil
}

func (s *Strategy) getMirrorHead(ctx context.Context, repo *gitclone.Repository) string {
	head, _ := mirrorHead(ctx, repo.Path()) //nolint:errcheck // best-effort; empty string signals failure to callers
	return head
}

// cachedBundleStale reports whether this replica's mirror HEAD has moved past the tip a cached bundle was built to.
//
// Bundles are keyed by base alone, so this stops clones getting the first one built after a snapshot. A tip this
// mirror lacks came from a newer replica and is kept; a want the bundle lacks freshens this mirror first.
func (s *Strategy) cachedBundleStale(ctx context.Context, repo *gitclone.Repository, headers http.Header, want string) (bool, error) { //nolint:privatedoc // explains why
	if repo == nil || repo.State() != gitclone.StateReady {
		return false, nil
	}
	tip := headers.Get(bundleTipHeader)
	validTip := commitSHARe.MatchString(tip)
	if want != "" && validTip && (tip == want || repo.HasAncestor(ctx, want, tip)) {
		return false, nil
	}
	freshenErr := s.freshenForBundleTip(ctx, repo, want)
	head := s.getMirrorHead(ctx, repo)
	if head == "" || head == tip {
		return false, freshenErr
	}
	if tip == "" {
		return true, freshenErr
	}
	return validTip && repo.HasAncestor(ctx, tip, head), freshenErr
}

// freshenForBundleTip fetches upstream when the client saw a tip this replica's mirror lacks.
//
// freshenMirror's ref-check gate allows one fetch per interval, so a bogus or unreachable tip cannot repeat it.
func (s *Strategy) freshenForBundleTip(ctx context.Context, repo *gitclone.Repository, want string) error { //nolint:privatedoc // explains why
	if want == "" || repo.HasCommit(ctx, want) {
		return nil
	}
	if err := s.freshenMirror(ctx, repo); err != nil {
		return errors.Wrap(err, "freshen mirror for bundle tip")
	}
	return nil
}

// createBundle writes a git bundle of the commits between baseCommit and the mirror's HEAD to a temp file.
//
// It also returns that HEAD. The returned file is already removed from the filesystem, so the open
// descriptor keeps the data alive. The caller must Close it.
func (s *Strategy) createBundle(ctx context.Context, repo *gitclone.Repository, baseCommit string) (*os.File, string, error) { //nolint:privatedoc // documents a non-obvious contract
	ctx, cancel := context.WithTimeout(ctx, bundleBuildTimeout)
	defer cancel()
	var bundleFile *os.File
	var head string
	err := repo.WithFetchExclusion(ctx, func() error {
		var err error
		head, err = mirrorHead(ctx, repo.Path())
		if err != nil {
			return err
		}
		if head == baseCommit {
			return errEmptyBundle
		}
		bundleFile, err = createBundleFile(ctx, repo, baseCommit)
		return err
	})
	return bundleFile, head, errors.WithStack(err)
}

func createBundleFile(ctx context.Context, repo *gitclone.Repository, baseCommit string) (*os.File, error) {
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

	cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "-c", "pack.useBitmaps=false", "bundle", "create", //nolint:gosec // baseCommit is a SHA string from rev-parse
		bundlePath, headRef, "^"+baseCommit)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
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

// serveSnapshotWithSpool generates one snapshot per upstream URL into the cache, streamed to all concurrent requests.
//
// An error means a possibly truncated response.
func (s *Strategy) serveSnapshotWithSpool(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository, upstreamURL, repoName string, start time.Time) error {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	result, err := s.snapshotSpools.Serve(w, r, upstreamURL, func(ctx context.Context, sw *spool.Writer) error {
		return s.fillSnapshot(ctx, sw, repo, repoName)
	})
	if result.Filled && s.config.SnapshotInterval > 0 {
		s.scheduleSnapshotJobs(repo)
	}
	if !result.Filled {
		s.metrics.recordSpoolFollowerWait(ctx, repoName, spoolFollowerOutcome(err), result.Wait)
	}
	if errors.Is(err, spool.ErrNotShared) {
		logger.DebugContext(ctx, "Snapshot spool not shareable, falling back to direct stream", "upstream", upstreamURL)
		return s.streamSnapshotDirect(w, r, repo)
	}
	if err != nil {
		return errors.Wrap(err, "snapshot spool read")
	}
	if result.Status != http.StatusOK {
		return nil
	}
	source := "spool"
	if result.Filled {
		source = "generated"
	}
	s.metrics.recordSnapshotServe(ctx, source, repoName, result.Bytes, time.Since(start))
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.source", source), attribute.Int64("cachew.bytes", result.Bytes))
		if !result.Filled {
			span.SetAttributes(attribute.Float64("cachew.spool_wait_seconds", result.Wait.Seconds()))
		}
	}
	return nil
}

func spoolFollowerOutcome(err error) string {
	switch {
	case err == nil:
		return "served"
	case errors.Is(err, spool.ErrNotShared):
		return "writer_failed"
	default:
		return "read_error"
	}
}

// streamSnapshotDirect streams a snapshot to the client without spooling, as a fallback when the spool writer failed.
func (s *Strategy) streamSnapshotDirect(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository) error {
	ctx := r.Context()
	mirrorRoot := s.cloneManager.Config().MirrorRoot

	snapshotDir, err := os.MkdirTemp(mirrorRoot, ".snapshot-stream-*")
	if err != nil {
		return errors.Wrap(err, "create temp snapshot dir")
	}
	defer func() { _ = os.RemoveAll(snapshotDir) }()

	repoDir := filepath.Join(snapshotDir, "repo")
	if err := s.cloneForSnapshot(ctx, repo, repoDir, s.snapshotFilterFor(repo.UpstreamURL())); err != nil {
		return errors.Wrap(err, "clone for snapshot streaming")
	}

	w.Header().Set("Content-Type", "application/zstd")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(repoDir)+".tar.zst"))

	return errors.Wrap(snapshot.StreamTo(ctx, w, repoDir, snapshotExcludePatterns("./.git"), s.config.ZstdThreads), "stream snapshot to client")
}

// fillSnapshot streams a snapshot of the mirror to sw while caching it, so it is generated once for both.
func (s *Strategy) fillSnapshot(ctx context.Context, sw *spool.Writer, repo *gitclone.Repository, repoName string) error {
	logger := logging.FromContext(ctx)
	start := time.Now()
	upstreamURL := repo.UpstreamURL()

	// Holding the snapshot mutex from clone to commit stops an older clone overwriting a newer snapshot.
	//
	// If another publisher holds it, stream uncached.
	mu := s.snapshotMutexFor(upstreamURL)
	publish := mu.TryLock()
	if publish {
		defer mu.Unlock()
	}

	snapshotDir, err := os.MkdirTemp(s.cloneManager.Config().MirrorRoot, ".snapshot-stream-*")
	if err != nil {
		s.failSnapshotFill(ctx, sw, repoName, start, errors.Wrap(err, "create temp snapshot dir"))
		return nil
	}
	defer func() {
		if err := os.RemoveAll(snapshotDir); err != nil {
			logger.WarnContext(ctx, "Failed to clean up snapshot stream dir", "dir", snapshotDir, "error", err)
		}
	}()
	repoDir := filepath.Join(snapshotDir, "repo")
	if err := s.cloneForSnapshot(ctx, repo, repoDir, s.snapshotFilterFor(upstreamURL)); err != nil {
		s.failSnapshotFill(ctx, sw, repoName, start, errors.Wrap(err, "clone for snapshot"))
		return nil
	}
	commit, err := mirrorHead(ctx, repoDir)
	if err != nil {
		s.failSnapshotFill(ctx, sw, repoName, start, errors.Wrap(err, "rev-parse HEAD for snapshot"))
		return nil
	}

	extraHeaders := http.Header{}
	extraHeaders.Set("X-Cachew-Snapshot-Commit", commit)
	headers := snapshot.Headers(filepath.Base(repoDir), extraHeaders)
	maps.Copy(sw.Header(), headers)
	sw.WriteHeader(http.StatusOK)
	if !publish {
		logger.InfoContext(ctx, "Snapshot generation in progress, streaming snapshot without caching it", "upstream", upstreamURL)
	} else if cw, err := s.cache.Create(ctx, snapshotCacheKey(upstreamURL), headers, 0); err != nil {
		logger.WarnContext(ctx, "Failed to create snapshot cache entry", "upstream", upstreamURL, "error", err)
	} else if err := sw.Store(cw); err != nil {
		s.metrics.recordSpoolWriter(ctx, repoName, "error", time.Since(start))
		return errors.Wrap(err, "store snapshot")
	}
	if err := snapshot.StreamTo(ctx, sw, repoDir, snapshotExcludePatterns("./.git"), s.config.ZstdThreads); err != nil {
		s.metrics.recordSpoolWriter(ctx, repoName, "error", time.Since(start))
		return errors.Wrap(err, "stream snapshot")
	}
	if err := sw.Commit(); err != nil {
		logger.WarnContext(ctx, "Failed to cache snapshot", "upstream", upstreamURL, "error", err)
	}
	s.metrics.recordSpoolWriter(ctx, repoName, "success", time.Since(start))
	return nil
}

// failSnapshotFill answers with a 500, which the spool never shares.
//
// Waiting requests then fall back to generating their own snapshot.
func (s *Strategy) failSnapshotFill(ctx context.Context, sw *spool.Writer, repoName string, start time.Time, err error) { //nolint:privatedoc // explains why
	logging.FromContext(ctx).ErrorContext(ctx, "Failed to prepare snapshot", "error", err)
	s.metrics.recordSpoolWriter(ctx, repoName, "prepare_error", time.Since(start))
	http.Error(sw, "Internal server error", http.StatusInternalServerError)
}

// scheduleDeferredMirrorRestore schedules one background mirror restore per upstream after a cold-start cached serve.
//
// Otherwise such repos never warm their mirror and cannot generate bundle deltas. By now the snapshot is backfilled
// to local disk, so later serves don't compete with the restore for S3 bandwidth.
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
		// Wait for in-flight cold serves so the restore's disk writes don't compete with local cache reads.
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

func (s *Strategy) scheduleColdSnapshotPreparation(ctx context.Context, repo *gitclone.Repository) {
	upstream := repo.UpstreamURL()
	if _, loaded := s.mirrorPreparations.LoadOrStore(upstream, true); loaded {
		return
	}

	logging.FromContext(ctx).InfoContext(ctx, "Scheduling cold snapshot preparation", "upstream", upstream)
	accepted := s.scheduler.TrySubmit(upstream, "cold-snapshot-clone", coldPreparationQueueLimit, func(ctx context.Context) error {
		defer s.mirrorPreparations.Delete(upstream)
		ctx, cancel := context.WithTimeout(ctx, s.coldPreparationTimeout())
		defer cancel()
		return s.prepareColdSnapshot(ctx, repo)
	})
	if !accepted {
		s.mirrorPreparations.Delete(upstream)
		logging.FromContext(ctx).WarnContext(ctx, "Cold snapshot preparation queue is full", "upstream", upstream)
	}
}

func (s *Strategy) coldPreparationTimeout() time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	cloneTimeout := s.cloneManager.Config().CloneTimeout
	if cloneTimeout > (maxDuration-snapshotClaimTTL)/3 {
		return maxDuration
	}
	return 3*cloneTimeout + snapshotClaimTTL
}

func (s *Strategy) coldSnapshotPublished(ctx context.Context, upstream string) (bool, error) {
	_, err := cache.StatAuthoritative(ctx, s.cache, snapshotCacheKey(upstream))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, errors.Wrap(err, "check shared snapshot")
}

func (s *Strategy) prepareColdSnapshot(ctx context.Context, repo *gitclone.Repository) (returnErr error) {
	upstream := repo.UpstreamURL()
	defer func() {
		if repo.State() == gitclone.StateReady && s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobsAfter(repo, s.config.SnapshotInterval)
		}
	}()
	if published, err := s.coldSnapshotPublished(ctx, upstream); err != nil {
		logging.FromContext(ctx).WarnContext(ctx, "Shared snapshot recheck failed, preparing anyway",
			"upstream", upstream, "error", err)
	} else if published {
		return nil
	}
	if repo.State() == gitclone.StateReady && s.config.SnapshotInterval == 0 {
		return nil
	}

	claimID, claimed := "", true
	if coord := s.snapshotCoord.Load(); s.config.SnapshotInterval > 0 && coord != nil {
		timer := time.NewTimer(s.coldPreparationDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Wrap(ctx.Err(), "wait to coordinate cold snapshot preparation")
		case <-timer.C:
		}
		primeErr := coord.Prime(ctx)
		if primeErr != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Cold preparation coordination refresh failed, preparing anyway",
				"upstream", upstream, "error", primeErr)
		}
		if published, err := s.coldSnapshotPublished(ctx, upstream); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Shared snapshot recheck failed, preparing anyway",
				"upstream", upstream, "error", err)
		} else if published {
			return nil
		}
		if primeErr == nil {
			var err error
			claimID, claimed, err = coord.ClaimWithTTL(snapshotJobBase, upstream, 0,
				s.coldPreparationTimeout())
			if err != nil {
				logging.FromContext(ctx).WarnContext(ctx, "Cold preparation coordination claim failed, preparing anyway",
					"upstream", upstream, "error", err)
				claimed = true
			}
		}
	}
	if !claimed {
		return nil
	}
	if claimID != "" {
		defer func() {
			if returnErr != nil {
				returnErr = errors.Join(returnErr, s.failSnapshotClaim(ctx, snapshotJobBase, upstream, claimID))
			}
		}()
	}

	if err := s.ensureCloneReady(ctx, repo); err != nil {
		return err
	}
	if s.config.RepackInterval > 0 {
		s.scheduleRepackJobs(repo)
	}
	publishedCommit := ""
	if s.config.SnapshotInterval > 0 {
		commit, err := s.generateAndUploadSnapshot(ctx, repo)
		if err != nil {
			return errors.Wrap(err, "publish prepared snapshot")
		}
		publishedCommit = commit
	}
	if claimID != "" {
		var err error
		if publishedCommit == "" {
			err = s.snapshotCoord.Load().SkipClaim(ctx, snapshotJobBase, upstream, claimID)
		} else {
			err = s.snapshotCoord.Load().CompleteClaim(ctx, snapshotJobBase, upstream, claimID, publishedCommit)
		}
		if err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Failed to record cold snapshot preparation result",
				"upstream", upstream, "error", err)
			if releaseErr := s.failSnapshotClaim(ctx, snapshotJobBase, upstream, claimID); releaseErr != nil {
				logging.FromContext(ctx).WarnContext(ctx, "Failed to release cold snapshot claim after recording error",
					"upstream", upstream, "error", releaseErr)
			}
		}
	}
	return nil
}

type coldSnapshotEntry struct {
	done    chan struct{}
	serving sync.WaitGroup // tracks all in-flight snapshot serves (winner + followers)
}

// generateAndUploadLFSSnapshot archives the LFS objects for HEAD, served at /git/{repo}/lfs-snapshot.tar.zst.
//
// Historical LFS versions are excluded to keep the archive small. Paths are relative to .git/
// (e.g. ./lfs/objects/xx/yy/sha256) so clients extract it directly into .git/.
func (s *Strategy) generateAndUploadLFSSnapshot(ctx context.Context, repo *gitclone.Repository) (commit string, returnErr error) {
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

	// LFS objects derive from HEAD, so an unchanged HEAD means unchanged content.
	//
	// Repos without an LFS entry fail the existence check and re-run the cheap discovery grep below.
	head := s.getMirrorHead(ctx, repo)
	if s.snapshotUnchanged(ctx, snapshotJobLFS, lfsSnapshotCacheKey(upstream), upstream, head) {
		logger.InfoContext(ctx, "LFS snapshot unchanged, skipping generation", "upstream", upstream, "commit", head)
		return "", nil
	}

	// Search nested .gitattributes too, since some repos only configure LFS in subdirectories.
	discoverStart := time.Now()
	repoPath := repo.Path()
	grepCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "grep", "-q", "filter=lfs", "HEAD", "--", "*.gitattributes") //nolint:gosec
	if err := grepCmd.Run(); err != nil {
		// Exit 1 means no match; any other failure propagates so repos that use LFS are never silently skipped.
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok && exitErr.ExitCode() == 1 {
			s.metrics.recordLFSPhase(ctx, upstream, "discover", "skipped", time.Since(discoverStart))
			logger.DebugContext(ctx, "No LFS filter in any .gitattributes, skipping LFS snapshot", "upstream", upstream)
			return head, nil
		}
		s.metrics.recordLFSPhase(ctx, upstream, "discover", "error", time.Since(discoverStart))
		return "", errors.Wrap(err, "git grep for LFS filter")
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
	// The LFS clone stays unfiltered so lazy partial-clone fetches never hit upstream without credentials.
	//
	// The remote is reset to upstream, which bypasses the credential injection that repo.GitCommand provides.
	if err := s.withSnapshotClone(ctx, repo, "lfs", "", func(workDir string) error {
		s.metrics.recordLFSPhase(ctx, upstream, "clone", "success", time.Since(cloneStart))
		cloneRecorded = true

		// Record the clone's HEAD so the coordination record matches the archive.
		//
		// A concurrent fetch can advance the mirror between the unchanged check and this clone.
		headSHA, err := mirrorHead(ctx, workDir)
		if err != nil {
			return errors.Wrap(err, "rev-parse HEAD for LFS snapshot")
		}
		commit = headSHA
		if err := replaceRepositoryLFSConfig(workDir); err != nil {
			return err
		}

		// cloneForSnapshot already restored remote.origin.url to upstream, so LFS fetches from GitHub.
		//
		// #nosec G204
		if output, err := exec.CommandContext(ctx, "git", "-C", workDir,
			"lfs", "install", "--local").CombinedOutput(); err != nil {
			logger.WarnContext(ctx, "git lfs install --local failed (non-fatal)", "upstream", upstream, "error", err,
				"output", string(output))
		}

		// Fetch only the LFS objects referenced by HEAD.
		//
		// The timeout must stay below githubapp.RefreshBuffer (30m) so the token can't expire mid-fetch.
		fetchStart := time.Now()
		fetchCtx, cancel := context.WithTimeout(ctx, lfsFetchTimeout)
		lfsURL, err := lfsEndpointForUpstream(upstream)
		if err != nil {
			cancel()
			s.metrics.recordLFSPhase(ctx, upstream, "fetch", "error", time.Since(fetchStart))
			return errors.Wrap(err, "derive LFS endpoint")
		}
		fetchCmd, err := repo.GitCommand(fetchCtx, "-c", "lfs.url="+lfsURL, "-c", "lfs.basictransfersonly=true",
			"-c", "lfs.standalonetransferagent=", "-C", workDir, "lfs", "fetch", "origin", "HEAD")
		if err != nil {
			cancel()
			s.metrics.recordLFSPhase(ctx, upstream, "fetch", "error", time.Since(fetchStart))
			return errors.Wrap(err, "create git lfs fetch command")
		}
		// Kill the whole group: git-lfs helpers inherit our pipes and keep CombinedOutput blocked after the timeout.
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
		// The .git/lfs size is a best-effort proxy for bytes fetched; a failed walk must not fail the snapshot.
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
		return "", errors.Wrap(err, "create LFS snapshot")
	}

	s.metrics.recordOperation(ctx, "lfs-snapshot", "success", time.Since(start))
	logger.InfoContext(ctx, "LFS snapshot generation completed", "upstream", upstream)
	return commit, nil
}

func lfsEndpointForUpstream(upstream string) (string, error) {
	parsed, err := url.Parse(upstream)
	if err != nil {
		return "", errors.Wrap(err, "parse upstream URL")
	}
	if parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return "", errors.Errorf("unsupported upstream URL %q", upstream)
	}
	parsed.Path = strings.TrimSuffix(strings.TrimSuffix(parsed.Path, "/"), ".git") + ".git/info/lfs"
	parsed.RawPath = ""
	return parsed.String(), nil
}

func replaceRepositoryLFSConfig(workDir string) error {
	lfsConfigPath := filepath.Join(workDir, ".lfsconfig")
	if err := os.RemoveAll(lfsConfigPath); err != nil {
		return errors.Wrap(err, "remove repository LFS configuration")
	}
	return errors.Wrap(os.WriteFile(lfsConfigPath, nil, 0o600), "replace repository LFS configuration")
}

func dirSizeBytes(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort: skip unreadable entries
		}
		// Stat first: on some NFS/FUSE setups DirEntry.Type() is unknown, so IsRegular() would drop files.
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

	// Try the cache first so cold starts can serve before the mirror is ready.
	//
	// Range and validators go to the cache so an interrupted download can resume pinned to its ETag.
	reader, headers, err := s.cache.Open(ctx, cacheKey, httputil.ConditionalOptions(r)...)
	handled, _, serveErr := httputil.ServeCacheHit(w, headers, reader, err)
	if serveErr != nil {
		logger.ErrorContext(ctx, "Failed to stream LFS snapshot", "upstream", upstreamURL, "error", serveErr)
	}
	if handled {
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to open LFS snapshot from cache", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Return 404 rather than block on mirror restore and on-demand generation.
	//
	// A background mirror warm lets the periodic LFS snapshot job fire once the mirror is ready.
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
