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
