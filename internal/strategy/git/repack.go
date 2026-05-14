package git

import (
	"context"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/gitclone"
)

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "repack-periodic", s.config.RepackInterval, func(ctx context.Context) error {
		start := time.Now()
		err := repo.Repack(ctx)
		status := "success"
		if err != nil {
			status = "error"
		}
		s.metrics.recordOperation(ctx, "repack", status, time.Since(start))
		return errors.Wrap(err, "repack")
	})
}
