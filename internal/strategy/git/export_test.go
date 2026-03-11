package git

import (
	"context"

	"github.com/block/cachew/internal/gitclone"
)

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadSnapshot(ctx, repo)
}

func ConvertSnapshotToMirror(ctx context.Context, repoPath, upstreamURL string) error {
	return convertSnapshotToMirror(ctx, repoPath, upstreamURL)
}
