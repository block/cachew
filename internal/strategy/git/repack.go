package git

import (
	"context"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/jobscheduler"
)

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(jobscheduler.Job{Queue: repo.UpstreamURL(), ID: "repack-periodic", Cost: CostRepack, Run: func(ctx context.Context) error {
		return repo.Repack(ctx)
	}}, s.config.RepackInterval)
}
