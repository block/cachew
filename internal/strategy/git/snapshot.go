package git

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func snapshotDirForURL(mirrorRoot, upstreamURL string) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve snapshot directory")
	}
	return filepath.Join(mirrorRoot, ".snapshots", repoPath), nil
}

// remoteURLForSnapshot returns the URL to embed as remote.origin.url in snapshots.
// When a server URL is configured, it returns the cachew URL for the repo so that
// git pull goes through cachew. Otherwise it falls back to the upstream URL.
func (s *Strategy) remoteURLForSnapshot(upstream string) string {
	if s.config.ServerURL == "" {
		return upstream
	}
	repoPath, err := gitclone.RepoPathFromURL(upstream)
	if err != nil {
		return upstream
	}
	return s.config.ServerURL + "/git/" + repoPath
}

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Snapshot generation started", slog.String("upstream", upstream))

	mu := s.snapshotMutexFor(upstream)
	mu.Lock()
	defer mu.Unlock()

	mirrorRoot := s.cloneManager.Config().MirrorRoot
	snapshotDir, err := snapshotDirForURL(mirrorRoot, upstream)
	if err != nil {
		return err
	}

	// Clean any previous snapshot working directory.
	if err := os.RemoveAll(snapshotDir); err != nil {
		return errors.Wrap(err, "remove previous snapshot dir")
	}
	if err := os.MkdirAll(filepath.Dir(snapshotDir), 0o750); err != nil {
		return errors.Wrap(err, "create snapshot parent dir")
	}

	// Hold a read lock to exclude concurrent fetches while cloning.
	if err := repo.WithReadLock(func() error {
		// #nosec G204 - repo.Path() and snapshotDir are controlled by us
		cmd := exec.CommandContext(ctx, "git", "clone", repo.Path(), snapshotDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "git clone for snapshot: %s", string(output))
		}

		// git clone from a local path sets remote.origin.url to that path; restore it.
		// #nosec G204 - remoteURL is derived from controlled inputs
		cmd = exec.CommandContext(ctx, "git", "-C", snapshotDir, "remote", "set-url", "origin", s.remoteURLForSnapshot(upstream))
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "fix snapshot remote URL: %s", string(output))
		}
		return nil
	}); err != nil {
		_ = os.RemoveAll(snapshotDir)
		return errors.WithStack(err)
	}

	cacheKey := cache.NewKey(upstream + ".snapshot")
	ttl := 7 * 24 * time.Hour
	excludePatterns := []string{"*.lock"}

	err = snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, ttl, excludePatterns, s.config.ZstdThreads)

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

func (s *Strategy) snapshotMutexFor(upstreamURL string) *sync.Mutex {
	mu, _ := s.snapshotMu.LoadOrStore(upstreamURL, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Strategy) handleSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.tar.zst"))
	upstreamURL := "https://" + host + "/" + repoPath
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")

	reader, headers, err := s.cache.Open(ctx, cacheKey)
	if errors.Is(err, os.ErrNotExist) {
		repo, repoErr := s.cloneManager.GetOrCreate(ctx, upstreamURL)
		if repoErr != nil {
			logger.ErrorContext(ctx, "Failed to get or create clone",
				slog.String("upstream", upstreamURL),
				slog.String("error", repoErr.Error()))
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		if cloneErr := s.ensureCloneReady(ctx, repo); cloneErr != nil {
			logger.ErrorContext(ctx, "Clone unavailable for snapshot",
				slog.String("upstream", upstreamURL),
				slog.String("error", cloneErr.Error()))
			http.Error(w, "Repository unavailable", http.StatusServiceUnavailable)
			return
		}
		if genErr := s.generateAndUploadSnapshot(ctx, repo); genErr != nil {
			logger.ErrorContext(ctx, "On-demand snapshot generation failed",
				slog.String("upstream", upstreamURL),
				slog.String("error", genErr.Error()))
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		reader, headers, err = s.cache.Open(ctx, cacheKey)
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.DebugContext(ctx, "snapshot not found in cache", slog.String("upstream", upstreamURL))
			http.NotFound(w, r)
			return
		}
		logger.ErrorContext(ctx, "Failed to open snapshot from cache",
			slog.String("upstream", upstreamURL),
			slog.String("error", err.Error()))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	for key, values := range headers {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	if _, err = io.Copy(w, reader); err != nil {
		logger.ErrorContext(ctx, "Failed to stream snapshot",
			slog.String("upstream", upstreamURL),
			slog.String("error", err.Error()))
	}
}
