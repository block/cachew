package git

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func snapshotDirForURL(mirrorRoot, upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return filepath.Join(mirrorRoot, ".snapshots", "unknown")
	}
	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(mirrorRoot, ".snapshots", parsed.Host, repoPath)
}

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Snapshot generation started", slog.String("upstream", upstream))

	mirrorRoot := s.cloneManager.Config().MirrorRoot
	snapshotDir := snapshotDirForURL(mirrorRoot, upstream)

	// Clean any previous snapshot working directory.
	if err := os.RemoveAll(snapshotDir); err != nil {
		return errors.Wrap(err, "remove previous snapshot dir")
	}
	if err := os.MkdirAll(filepath.Dir(snapshotDir), 0o750); err != nil {
		return errors.Wrap(err, "create snapshot parent dir")
	}

	// Local clone from the mirror — git hardlinks objects by default.
	// #nosec G204 - repo.Path() and snapshotDir are controlled by us
	cmd := exec.CommandContext(ctx, "git", "clone", repo.Path(), snapshotDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(snapshotDir)
		return errors.Wrapf(err, "git clone for snapshot: %s", string(output))
	}

	cacheKey := cache.NewKey(upstream + ".snapshot")
	ttl := 7 * 24 * time.Hour
	excludePatterns := []string{"*.lock"}

	err := snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, ttl, excludePatterns)

	// Always clean up the snapshot working directory.
	if rmErr := os.RemoveAll(snapshotDir); rmErr != nil {
		logger.WarnContext(ctx, "Failed to clean up snapshot dir", slog.String("error", rmErr.Error()))
	}

	if err != nil {
		logger.ErrorContext(ctx, "Snapshot generation failed", slog.String("upstream", upstream), slog.String("error", err.Error()))
		return errors.Wrap(err, "create snapshot")
	}

	logger.InfoContext(ctx, "Snapshot generation completed", slog.String("upstream", upstream))
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
