package git

import (
	"context"

	"github.com/block/cachew/internal/gitclone"
)

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "repack-periodic", s.config.RepackInterval, func(ctx context.Context) error {
		return repo.Repack(ctx)
	})
}
