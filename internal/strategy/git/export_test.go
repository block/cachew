package git

import (
	"context"
	"io"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
)

// Exports unexported symbols for use by external test packages.

func IsGitRequest(pathValue string) bool { return isGitRequest(pathValue) }

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	_, err := s.generateAndUploadSnapshot(ctx, repo)
	return err
}

func (s *Strategy) GenerateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	_, err := s.generateAndUploadMirrorSnapshot(ctx, repo)
	return err
}

// RunCoordinatedSnapshot exports the coordinated base-snapshot job for tests.
func (s *Strategy) RunCoordinatedSnapshot(ctx context.Context, repo *gitclone.Repository, interval time.Duration) error {
	return s.coordinatedSnapshotJob(snapshotJobBase, repo, interval, func(ctx context.Context) (string, error) {
		return s.generateAndUploadSnapshot(ctx, repo)
	})(ctx)
}

// RunFailingCoordinatedSnapshot causes a snapshot job to fail so tests can check claim release.
func (s *Strategy) RunFailingCoordinatedSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.coordinatedSnapshotJob(snapshotJobBase, repo, 0, func(context.Context) (string, error) {
		return "", errors.New("snapshot generation failed")
	})(ctx)
}

// CacheBundle exports cacheBundle for testing.
func (s *Strategy) CacheBundle(ctx context.Context, key cache.Key, r io.Reader) error {
	return s.cacheBundle(ctx, key, r)
}

// MirrorPreparationScheduled lets tests check for a queued or active preparation job.
func (s *Strategy) MirrorPreparationScheduled(upstream string) bool {
	_, ok := s.mirrorPreparations.Load(upstream)
	return ok
}

// PeriodicJobsScheduled lets tests check the periodic jobs that the strategy registered.
func (s *Strategy) PeriodicJobsScheduled(upstream string) (snapshot, repack bool) {
	_, snapshot = s.snapshotJobsScheduled.Load(upstream)
	_, repack = s.repackJobsScheduled.Load(upstream)
	return snapshot, repack
}

// SetColdPreparationDelay lets tests control the delay before a snapshot claim.
func (s *Strategy) SetColdPreparationDelay(delay func() time.Duration) {
	s.coldPreparationDelay = delay
}

// ColdPreparationQueueLimitForTest returns the queue limit for tests in external packages.
func ColdPreparationQueueLimitForTest() int {
	return coldPreparationQueueLimit
}

// ClaimSnapshotForTest lets tests request a base snapshot claim.
func (s *Strategy) ClaimSnapshotForTest(upstream string) (bool, error) {
	return s.snapshotCoord.Claim(snapshotJobBase, upstream, 0)
}
