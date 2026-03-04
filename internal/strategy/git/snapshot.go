package git

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func snapshotDirForURL(mirrorRoot, upstreamURL string, depth int) (string, error) {
	repoPath, err := gitclone.RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "resolve snapshot directory")
	}
	dir := filepath.Join(mirrorRoot, ".snapshots", repoPath)
	if depth > 0 {
		dir += fmt.Sprintf("-depth-%d", depth)
	}
	return dir, nil
}

func snapshotCacheKey(upstreamURL string, depth int) cache.Key {
	suffix := ".snapshot"
	if depth > 0 {
		suffix = fmt.Sprintf(".snapshot-depth-%d", depth)
	}
	return cache.NewKey(upstreamURL + suffix)
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

func (s *Strategy) generateAndUploadSnapshot(ctx context.Context, repo *gitclone.Repository, depth int) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, "Snapshot generation started",
		slog.String("upstream", upstream),
		slog.Int("depth", depth))

	mu := s.snapshotMutexFor(upstream, depth)
	mu.Lock()
	defer mu.Unlock()

	mirrorRoot := s.cloneManager.Config().MirrorRoot
	snapshotDir, err := snapshotDirForURL(mirrorRoot, upstream, depth)
	if err != nil {
		return err
	}

	// Clean any previous snapshot working directory.
	// #nosec G703 - snapshotDir is constructed from mirrorRoot + sanitised repo path
	if err := os.RemoveAll(snapshotDir); err != nil {
		return errors.Wrap(err, "remove previous snapshot dir")
	}
	// #nosec G703 - snapshotDir is constructed from mirrorRoot + sanitised repo path
	if err := os.MkdirAll(filepath.Dir(snapshotDir), 0o750); err != nil {
		return errors.Wrap(err, "create snapshot parent dir")
	}

	// Hold a read lock to exclude concurrent fetches while cloning.
	if err := repo.WithReadLock(func() error {
		// #nosec G204 - repo.Path(), snapshotDir, and depth are controlled by us
		args := []string{"clone"}
		source := repo.Path()
		if depth > 0 {
			args = append(args, "--depth", strconv.Itoa(depth), "--no-checkout")
			// git ignores --depth for local path clones (uses hardlinks instead).
			// Use file:// protocol to force pack transfer with depth support.
			absPath, err := filepath.Abs(source)
			if err != nil {
				return errors.Wrap(err, "resolve absolute path for file:// URL")
			}
			source = "file://" + absPath
		}
		args = append(args, source, snapshotDir)
		cmd := exec.CommandContext(ctx, "git", args...)
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
		// #nosec G703 - snapshotDir is constructed from mirrorRoot + sanitised repo path
		_ = os.RemoveAll(snapshotDir)
		return errors.WithStack(err)
	}

	cacheKey := snapshotCacheKey(upstream, depth)
	ttl := 7 * 24 * time.Hour
	excludePatterns := []string{"*.lock"}

	err = snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, ttl, excludePatterns, s.config.ZstdThreads)

	// Always clean up the snapshot working directory.
	// #nosec G703 - snapshotDir is constructed from mirrorRoot + sanitised repo path
	if rmErr := os.RemoveAll(snapshotDir); rmErr != nil {
		logger.WarnContext(ctx, "Failed to clean up snapshot dir", slog.String("error", rmErr.Error()))
	}
	if err != nil {
		logger.ErrorContext(ctx, "Snapshot generation failed",
			slog.String("upstream", upstream),
			slog.Int("depth", depth),
			slog.String("error", err.Error()))
		return errors.Wrap(err, "create snapshot")
	}

	logger.InfoContext(ctx, "Snapshot generation completed",
		slog.String("upstream", upstream),
		slog.Int("depth", depth))
	return nil
}

func (s *Strategy) scheduleSnapshotJobs(repo *gitclone.Repository) {
	s.scheduleSnapshotJobsWithDepth(repo, 0)
}

func (s *Strategy) scheduleSnapshotJobsWithDepth(repo *gitclone.Repository, depth int) {
	jobID := "snapshot-periodic"
	if depth > 0 {
		jobID = fmt.Sprintf("snapshot-depth-%d-periodic", depth)
	}
	scheduleKey := repo.UpstreamURL() + ":" + jobID
	if _, alreadyScheduled := s.scheduledSnapshots.LoadOrStore(scheduleKey, true); alreadyScheduled {
		return
	}
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), jobID, s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadSnapshot(ctx, repo, depth)
	})
}

func (s *Strategy) snapshotMutexFor(upstreamURL string, depth int) *sync.Mutex {
	key := upstreamURL
	if depth > 0 {
		key = fmt.Sprintf("%s-depth-%d", upstreamURL, depth)
	}
	mu, _ := s.snapshotMu.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (s *Strategy) handleSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	repoPath := ExtractRepoPath(strings.TrimSuffix(pathValue, "/snapshot.tar.zst"))
	upstreamURL := "https://" + host + "/" + repoPath

	var depth int
	if d := r.URL.Query().Get("depth"); d != "" {
		var err error
		depth, err = strconv.Atoi(d)
		if err != nil || depth < 0 {
			http.Error(w, "invalid depth parameter", http.StatusBadRequest)
			return
		}
	}

	cacheKey := snapshotCacheKey(upstreamURL, depth)

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
		if genErr := s.generateAndUploadSnapshot(ctx, repo, depth); genErr != nil {
			logger.ErrorContext(ctx, "On-demand snapshot generation failed",
				slog.String("upstream", upstreamURL),
				slog.String("error", genErr.Error()))
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		// Schedule periodic refresh so this snapshot stays fresh.
		if s.config.SnapshotInterval > 0 {
			s.scheduleSnapshotJobsWithDepth(repo, depth)
		}
		reader, headers, err = s.cache.Open(ctx, cacheKey)
	}
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.DebugContext(ctx, "Snapshot not found in cache",
				slog.String("upstream", upstreamURL),
				slog.Int("depth", depth))
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
