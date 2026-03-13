package git

import (
	"context"

	"github.com/block/cachew/internal/gitclone"
)

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadSnapshot(ctx, repo)
}

func (s *Strategy) GenerateAndUploadMirrorSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadMirrorSnapshot(ctx, repo)
}
