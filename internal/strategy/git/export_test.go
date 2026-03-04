package git

import (
	"context"

	"github.com/block/cachew/internal/gitclone"
)

func (s *Strategy) GenerateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	return s.generateAndUploadSnapshot(ctx, repo, 0)
}

func (s *Strategy) GenerateAndUploadShallowSnapshot(ctx context.Context, repo *gitclone.Repository, depth int) error {
	return s.generateAndUploadSnapshot(ctx, repo, depth)
}
