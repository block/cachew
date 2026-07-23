package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/internal/gitclone"
)

// repackEnabled reports whether any repack variant is configured to run.
func (s *Strategy) repackEnabled() bool {
	return s.config.RepackInterval > 0 || s.config.FullRepackInterval > 0
}

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	if s.config.RepackInterval > 0 {
		s.schedulePeriodicRepack(repo, "repack-periodic", "repack", s.config.RepackInterval, repo.Repack)
	}
	if s.config.FullRepackInterval > 0 {
		s.schedulePeriodicRepack(repo, "repack-full-periodic", "repack_full", s.config.FullRepackInterval, repo.RepackFull)
	}
}

// schedulePeriodicRepack runs repack on the given interval, recording the
// before/after pack count, duration, and outcome. operation distinguishes the
// geometric ("repack") and full ("repack_full") variants in metrics and traces.
func (s *Strategy) schedulePeriodicRepack(repo *gitclone.Repository, jobID, operation string, interval time.Duration, repack func(context.Context) error) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), jobID, interval, func(ctx context.Context) (returnErr error) {
		upstream := repo.UpstreamURL()
		ctx, span := tracer.Start(ctx, "git."+operation,
			trace.WithAttributes(
				attribute.String("cachew.operation", operation),
				attribute.String("cachew.upstream", upstream),
			),
		)
		defer func() {
			if returnErr != nil {
				span.RecordError(returnErr)
				span.SetStatus(codes.Error, returnErr.Error())
			}
			span.End()
		}()

		// Pack count before and after gives us a direct view of how much the
		// repack consolidated. A flat before/after ratio over time means
		// fragmentation is outpacing the schedule.
		if before, err := countPackFiles(repo.Path()); err == nil {
			s.metrics.recordRepackPackCount(ctx, upstream, "before", before)
			span.SetAttributes(attribute.Int("cachew.pack_count_before", before))
		}

		start := time.Now()
		err := repack(ctx)
		status := "success"
		if err != nil {
			status = "error"
		}
		s.metrics.recordOperation(ctx, operation, status, time.Since(start))

		if after, countErr := countPackFiles(repo.Path()); countErr == nil {
			s.metrics.recordRepackPackCount(ctx, upstream, "after", after)
			span.SetAttributes(attribute.Int("cachew.pack_count_after", after))
		}

		return errors.Wrap(err, operation)
	})
}

// countPackFiles returns the number of .pack files in the mirror's
// objects/pack directory. Returns 0, nil if the directory is missing.
func countPackFiles(repoPath string) (int, error) {
	entries, err := os.ReadDir(filepath.Join(repoPath, "objects", "pack"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, errors.Wrap(err, "read pack dir")
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".pack") {
			count++
		}
	}
	return count, nil
}
