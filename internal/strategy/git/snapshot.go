package git

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
	"github.com/block/cachew/internal/snapshot"
)

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Snapshot generation started", slog.String("upstream", upstream))

	startTime := time.Now()
	cacheKey := cache.NewKey(upstream + ".snapshot")
	ttl := 7 * 24 * time.Hour
	excludePatterns := []string{"*.lock"}

	err := errors.Wrap(snapshot.Create(ctx, s.cache, cacheKey, repo.Path(), ttl, excludePatterns), "create snapshot")
	duration := time.Since(startTime)

	if err != nil {
		logger.ErrorContext(ctx, "Snapshot generation failed", slog.String("upstream", upstream), slog.String("error", err.Error()))
		if ops := metrics.FromContext(ctx); ops != nil {
			ops.RecordOperation(ctx, "git.snapshot.generate", "failure", duration,
				attribute.String("repository_url", upstream))
		}
		return err
	}

	logger.InfoContext(ctx, "Snapshot generation completed", slog.String("upstream", upstream))
	if ops := metrics.FromContext(ctx); ops != nil {
		ops.RecordOperation(ctx, "git.snapshot.generate", "success", duration,
			attribute.String("repository_url", upstream))
	}
	return nil
}

func (s *Strategy) scheduleSnapshotJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "snapshot-periodic", s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadSnapshot(ctx, repo)
	})
}

func (s *Strategy) handleSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	s.serveCachedArtifact(w, r, host, pathValue, "snapshot")
}
