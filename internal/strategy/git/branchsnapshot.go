package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// Branch snapshots let clients request a working-tree snapshot of a branch
// other than the default with ?ref=<branch>. They are built on the first
// request, refreshed on the snapshot interval while clients keep requesting
// them, and dropped once idle. Tracking is per replica; generation is
// deduplicated across replicas by the snapshot coordinator.

const maxBranchNameLength = 255

var branchNameRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]*$`)

// validBranchName accepts a conservative subset of git branch names so the
// value is safe to pass to git and to embed in cache keys and paths.
func validBranchName(ref string) bool {
	if len(ref) > maxBranchNameLength || !branchNameRe.MatchString(ref) {
		return false
	}
	if strings.Contains(ref, "..") || strings.Contains(ref, "//") || strings.Contains(ref, "/.") {
		return false
	}
	return !strings.HasSuffix(ref, "/") && !strings.HasSuffix(ref, ".") && !strings.HasSuffix(ref, ".lock")
}

func refKeySuffix(ref string) string {
	if ref == "" {
		return ""
	}
	return "@" + ref
}

// refDirSuffix keeps branch work directories flat and distinct from the
// default branch's.
func refDirSuffix(ref string) string {
	if ref == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(ref))
	return "-ref-" + hex.EncodeToString(sum[:8])
}

func tipRevision(ref string) string {
	if ref == "" {
		return "HEAD"
	}
	return "refs/heads/" + ref
}

func refJob(job, ref string) string {
	return job + refKeySuffix(ref)
}

func (s *Strategy) branchSnapshotsEnabled() bool {
	return s.config.SnapshotInterval > 0 && s.config.BranchSnapshotMaxRefs > 0
}

type trackedRefs struct {
	mu   sync.Mutex
	refs map[string]time.Time // branch -> last requested
}

// trackRef records a request for ref and evicts the least recently requested
// branch when the repository exceeds the configured limit.
func (s *Strategy) trackRef(upstream, ref string) {
	value, _ := s.branchSnapshots.LoadOrStore(upstream, &trackedRefs{refs: map[string]time.Time{}})
	tracked := value.(*trackedRefs)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	tracked.refs[ref] = time.Now()
	for len(tracked.refs) > s.config.BranchSnapshotMaxRefs {
		oldest, oldestAt := "", time.Time{}
		for name, at := range tracked.refs {
			if oldest == "" || at.Before(oldestAt) {
				oldest, oldestAt = name, at
			}
		}
		delete(tracked.refs, oldest)
	}
}

func (s *Strategy) forgetRef(upstream, ref string) {
	if value, ok := s.branchSnapshots.Load(upstream); ok {
		tracked := value.(*trackedRefs)
		tracked.mu.Lock()
		delete(tracked.refs, ref)
		tracked.mu.Unlock()
	}
}

// activeRefs returns the branches requested within the idle timeout and
// stops tracking the rest.
func (s *Strategy) activeRefs(upstream string) []string {
	value, ok := s.branchSnapshots.Load(upstream)
	if !ok {
		return nil
	}
	tracked := value.(*trackedRefs)
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	var active []string
	for name, at := range tracked.refs {
		if time.Since(at) > s.config.BranchSnapshotIdleTimeout {
			delete(tracked.refs, name)
			continue
		}
		active = append(active, name)
	}
	return active
}

func (s *Strategy) handleRefSnapshotRequest(w http.ResponseWriter, r *http.Request, upstreamURL, repoName, ref string, start time.Time) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		span.SetAttributes(attribute.String("cachew.ref", ref))
	}

	if !validBranchName(ref) {
		http.Error(w, "ref query parameter must be a branch name", http.StatusBadRequest)
		return
	}
	if !s.branchSnapshotsEnabled() {
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Branch snapshots are disabled", http.StatusNotFound)
		return
	}

	cacheKey := snapshotCacheKey(upstreamURL, ref)
	if r.Method == http.MethodHead {
		s.serveSnapshotHead(ctx, w, r, cacheKey, repoName, start)
		return
	}

	repo, err := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "upstream", upstreamURL, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	s.trackRef(upstreamURL, ref)
	s.scheduleBranchSnapshotJobs(repo)

	reader, headers, err := s.cache.Open(ctx, cacheKey, httputil.ConditionalOptions(r)...)
	switch {
	case errors.Is(err, os.ErrNotExist):
		s.scheduleBranchSnapshot(ctx, repo, ref)
		w.Header().Set("Cache-Control", "no-store")
		http.Error(w, "Snapshot not cached", http.StatusNotFound)
	case err != nil && !errors.Is(err, cache.ErrNotModified) &&
		!errors.Is(err, cache.ErrPreconditionFailed) && !errors.Is(err, cache.ErrRangeNotSatisfiable):
		logger.ErrorContext(ctx, "Failed to open branch snapshot from cache", "upstream", upstreamURL, "ref", ref, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	case repo.State() == gitclone.StateReady:
		if serveErr := s.serveSnapshotWithBundle(ctx, w, reader, headers, err, repo, upstreamURL, ref, repoName, start); serveErr != nil {
			logger.WarnContext(ctx, "Failed to serve branch snapshot", "upstream", upstreamURL, "ref", ref, "error", serveErr)
		}
	default:
		// Without a ready mirror there is no tip to build a delta bundle
		// against; the client freshens the checkout itself.
		if _, serveErr := s.serveOpenedSnapshot(ctx, w, reader, headers, err, repoName, "cold_cache", start); serveErr != nil {
			logger.WarnContext(ctx, "Failed to serve branch snapshot", "upstream", upstreamURL, "ref", ref, "error", serveErr)
		}
	}
}

// scheduleBranchSnapshot builds ref's snapshot in the background after a
// cache miss. Concurrent misses for the same branch share one build.
func (s *Strategy) scheduleBranchSnapshot(ctx context.Context, repo *gitclone.Repository, ref string) {
	upstream := repo.UpstreamURL()
	key := upstream + refKeySuffix(ref)
	if _, loaded := s.branchSnapshotBuilds.LoadOrStore(key, true); loaded {
		return
	}
	accepted := s.scheduler.TrySubmit(upstream, "branch-snapshot", coldPreparationQueueLimit, func(ctx context.Context) error {
		defer s.branchSnapshotBuilds.Delete(key)
		ctx, cancel := context.WithTimeout(ctx, s.coldPreparationTimeout())
		defer cancel()
		if err := s.ensureCloneReady(ctx, repo); err != nil {
			return err
		}
		// The branch may be newer than the mirror's last fetch.
		if s.getMirrorTip(ctx, repo, ref) == "" {
			if err := s.freshenMirror(ctx, repo); err != nil {
				logging.FromContext(ctx).WarnContext(ctx, "Failed to freshen mirror for branch snapshot", "upstream", upstream, "ref", ref, "error", err)
			}
		}
		return s.generateBranchSnapshots(ctx, repo, ref, 0)
	})
	if !accepted {
		s.branchSnapshotBuilds.Delete(key)
		logging.FromContext(ctx).WarnContext(ctx, "Branch snapshot queue is full", "upstream", upstream, "ref", ref)
	}
}

// scheduleBranchSnapshotJobs registers one periodic job per repository that
// refreshes every branch still being requested.
func (s *Strategy) scheduleBranchSnapshotJobs(repo *gitclone.Repository) {
	upstream := repo.UpstreamURL()
	if _, loaded := s.branchSnapshotJobs.LoadOrStore(upstream, true); loaded {
		return
	}
	interval := s.config.SnapshotInterval
	run := func(ctx context.Context) error {
		refs := s.activeRefs(upstream)
		if len(refs) == 0 || repo.State() != gitclone.StateReady {
			return nil
		}
		if err := s.doFetch(ctx, repo); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Pre-snapshot fetch failed", "upstream", upstream, "error", err)
		}
		var errs []error
		for _, ref := range refs {
			errs = append(errs, s.generateBranchSnapshots(ctx, repo, ref, interval))
		}
		return errors.Join(errs...)
	}
	delay, interval := s.snapshotSchedule(interval)
	submit := func() { s.scheduler.SubmitPeriodicJob(upstream, "branch-snapshot-periodic", interval, run) }
	// The first run is due an interval after the on-demand build.
	time.AfterFunc(delay+interval, submit)
}

// generateBranchSnapshots builds ref's working-tree and LFS snapshots under
// cross-replica coordination. A branch that no longer exists is untracked.
func (s *Strategy) generateBranchSnapshots(ctx context.Context, repo *gitclone.Repository, ref string, interval time.Duration) error {
	upstream := repo.UpstreamURL()
	if s.getMirrorTip(ctx, repo, ref) == "" {
		logging.FromContext(ctx).InfoContext(ctx, "Branch not in mirror, dropping branch snapshot", "upstream", upstream, "ref", ref)
		s.forgetRef(upstream, ref)
		return nil
	}
	return errors.Join(
		s.coordinatedSnapshotJob(refJob(snapshotJobBase, ref), repo, interval, func(ctx context.Context) (string, error) {
			return s.generateAndUploadSnapshot(ctx, repo, ref)
		})(ctx),
		s.coordinatedSnapshotJob(refJob(snapshotJobLFS, ref), repo, interval, func(ctx context.Context) (string, error) {
			return s.generateAndUploadLFSSnapshot(ctx, repo, ref)
		})(ctx),
	)
}
