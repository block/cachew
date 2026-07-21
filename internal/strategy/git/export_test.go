package git

import (
	"context"
	"io"
	"time"

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

// CacheBundle exports cacheBundle for testing.
func (s *Strategy) CacheBundle(ctx context.Context, key cache.Key, r io.Reader) error {
	return s.cacheBundle(ctx, key, r)
}
