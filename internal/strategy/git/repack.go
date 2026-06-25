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

func (s *Strategy) scheduleRepackJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "repack-periodic", s.config.RepackInterval, func(ctx context.Context) (returnErr error) {
		upstream := repo.UpstreamURL()
		ctx, span := tracer.Start(ctx, "git.repack",
			trace.WithAttributes(
				attribute.String("cachew.operation", "repack"),
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

		// Pack count before and after gives us a direct view of how much
		// the geometric repack actually consolidated. A flat before/after
		// ratio over time means fragmentation is outpacing the schedule.
		if before, err := countPackFiles(repo.Path()); err == nil {
			s.metrics.recordRepackPackCount(ctx, upstream, "before", before)
			span.SetAttributes(attribute.Int("cachew.pack_count_before", before))
		}

		start := time.Now()
		err := repo.Repack(ctx)
		status := "success"
		if err != nil {
			status = "error"
		}
		s.metrics.recordOperation(ctx, "repack", status, time.Since(start))

		if after, countErr := countPackFiles(repo.Path()); countErr == nil {
			s.metrics.recordRepackPackCount(ctx, upstream, "after", after)
			span.SetAttributes(attribute.Int("cachew.pack_count_after", after))
		}

		return errors.Wrap(err, "repack")
	}, s.config.IdleTimeout)
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
