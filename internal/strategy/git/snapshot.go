package git

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, fmt.Sprintf("Snapshot generation started: %s", upstream), "upstream", upstream)

	cacheKey := cache.NewKey(upstream + ".snapshot")
	ttl := 7 * 24 * time.Hour
	excludePatterns := []string{"*.lock"}

	err := errors.Wrap(snapshot.Create(ctx, s.cache, cacheKey, repo.Path(), ttl, excludePatterns), "create snapshot")
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Snapshot generation failed for %s: %v", upstream, err), "upstream", upstream, "error", err)
		return err
	}

	logger.InfoContext(ctx, fmt.Sprintf("Snapshot generation completed: %s", upstream), "upstream", upstream)
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
