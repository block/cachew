package git

import (
	"context"
	"io"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
)

// Exports unexported symbols for use by external test packages.

func IsGitRequest(pathValue string) bool { return isGitRequest(pathValue) }

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadSnapshot(ctx, repo)
}

func (s *Strategy) GenerateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadMirrorSnapshot(ctx, repo)
}

// CacheBundle exports cacheBundle for testing.
func (s *Strategy) CacheBundle(ctx context.Context, key cache.Key, r io.Reader) error {
	return s.cacheBundle(ctx, key, r)
}

// ReadSnapshotPointer exports readSnapshotPointer for testing.
func (s *Strategy) ReadSnapshotPointer(ctx context.Context, upstreamURL string) (string, bool) {
	return s.readSnapshotPointer(ctx, upstreamURL)
}

// SnapshotCommitCacheKey exports snapshotCommitCacheKey for testing.
func SnapshotCommitCacheKey(upstreamURL, commit string) cache.Key {
	return snapshotCommitCacheKey(upstreamURL, commit)
}

// ParseImmutableSnapshotPath exports parseImmutableSnapshotPath for testing.
func ParseImmutableSnapshotPath(pathValue string) (string, string, bool) {
	return parseImmutableSnapshotPath(pathValue)
}
