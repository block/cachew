package git

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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

	err = snapshot.Create(ctx, s.cache, cacheKey, snapshotDir, ttl, excludePatterns)

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
	s.serveCachedArtifact(w, r, host, pathValue, "snapshot.tar.zst", "snapshot")
}

// generateAndUploadLFSSnapshot fetches the LFS objects for the repository's default
// branch and archives them as a separate tar.zst served at /git/{repo}/lfs-snapshot.tar.zst.
//
// The archive stores paths relative to .git/ (e.g. ./lfs/objects/xx/yy/sha256) so that
// the client can extract it directly into the repo's .git/ directory.
//
// This is called on the same schedule as generateAndUploadSnapshot so the LFS archive
// stays current with the mirror. It requires the GitHub App token manager to be
// configured so that git-lfs can authenticate with GitHub when fetching objects not
// already present in the mirror's .git/lfs/ store.
func (s *Strategy) generateAndUploadLFSSnapshot(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	// Verify git-lfs is available before doing any work.
	if _, err := exec.LookPath("git-lfs"); err != nil {
		logger.WarnContext(ctx, "git-lfs not found, skipping LFS snapshot",
			slog.String("upstream", upstream))
		return nil
	}

	logger.InfoContext(ctx, "LFS snapshot generation started", slog.String("upstream", upstream))

	mirrorRoot := s.cloneManager.Config().MirrorRoot
	snapshotDir, err := snapshotDirForURL(mirrorRoot, upstream)
	if err != nil {
		return err
	}
	lfsWorkDir := snapshotDir + "-lfs"

	if err := os.RemoveAll(lfsWorkDir); err != nil {
		return errors.Wrap(err, "remove previous LFS work dir")
	}
	if err := os.MkdirAll(filepath.Dir(lfsWorkDir), 0o750); err != nil {
		return errors.Wrap(err, "create LFS work dir parent")
	}

	// Clone the mirror into a working directory under a read lock.
	if err := repo.WithReadLock(func() error {
		// #nosec G204
		cmd := exec.CommandContext(ctx, "git", "clone", repo.Path(), lfsWorkDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			return errors.Wrapf(err, "git clone for LFS snapshot: %s", string(output))
		}
		return nil
	}); err != nil {
		_ = os.RemoveAll(lfsWorkDir)
		return errors.WithStack(err)
	}

	defer func() {
		if rmErr := os.RemoveAll(lfsWorkDir); rmErr != nil {
			logger.WarnContext(ctx, "Failed to clean up LFS work dir",
				slog.String("error", rmErr.Error()))
		}
	}()

	// Restore the upstream URL for LFS fetch. git-lfs contacts the LFS server
	// at {remote.origin.url}/info/lfs; we must point it at GitHub, not cachew.
	// #nosec G204
	if output, err := exec.CommandContext(ctx, "git", "-C", lfsWorkDir,
		"remote", "set-url", "origin", upstream).CombinedOutput(); err != nil {
		return errors.Wrapf(err, "restore LFS origin URL: %s", string(output))
	}

	// Inject a GitHub App token so git-lfs can authenticate when downloading
	// LFS objects that are not already in the local object store.
	fetchEnv := os.Environ()
	if s.tokenManager != nil {
		// Extract org from upstream URL, e.g. "squareup" from "https://github.com/squareup/repo".
		org := orgFromUpstreamURL(upstream)
		if org != "" {
			token, tokenErr := s.tokenManager.GetTokenForOrg(ctx, org)
			if tokenErr != nil {
				logger.WarnContext(ctx, "Failed to get GitHub token for LFS fetch — proceeding unauthenticated",
					slog.String("org", org), slog.String("error", tokenErr.Error()))
			} else if token != "" {
				// Embed credentials in the remote URL for this working directory only.
				authedURL := fmt.Sprintf("https://x-access-token:%s@github.com/%s",
					token, strings.TrimPrefix(upstream, "https://github.com/"))
				// #nosec G204
				if output, err := exec.CommandContext(ctx, "git", "-C", lfsWorkDir,
					"remote", "set-url", "origin", authedURL).CombinedOutput(); err != nil {
					return errors.Wrapf(err, "set authed LFS origin URL: %s", string(output))
				}
			}
		}
	}

	// Fetch all LFS objects for HEAD (the default branch).
	// #nosec G204
	fetchCmd := exec.CommandContext(ctx, "git", "-C", lfsWorkDir, "lfs", "fetch", "origin", "HEAD")
	fetchCmd.Env = fetchEnv
	if output, err := fetchCmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "git lfs fetch: %s", string(output))
	}

	// Check whether any LFS objects were actually downloaded.
	lfsDir := filepath.Join(lfsWorkDir, ".git", "lfs")
	if _, err := os.Stat(lfsDir); os.IsNotExist(err) {
		logger.InfoContext(ctx, "No LFS objects in repository, skipping LFS snapshot",
			slog.String("upstream", upstream))
		return nil
	}

	// Archive the ./lfs/ subtree relative to .git/ so the client can extract
	// directly into .git/ and get .git/lfs/objects/xx/yy/sha256.
	cacheKey := cache.NewKey(upstream + ".lfs-snapshot")
	ttl := 7 * 24 * time.Hour
	gitDir := filepath.Join(lfsWorkDir, ".git")

	if err := snapshot.CreateSubdir(ctx, s.cache, cacheKey, gitDir, "lfs", ttl); err != nil {
		return errors.Wrap(err, "create LFS snapshot")
	}

	logger.InfoContext(ctx, "LFS snapshot generation completed", slog.String("upstream", upstream))
	return nil
}

// orgFromUpstreamURL extracts the GitHub organisation name from an upstream URL.
// Returns "" for non-github.com URLs or malformed paths.
func orgFromUpstreamURL(upstream string) string {
	trimmed := strings.TrimPrefix(upstream, "https://github.com/")
	if trimmed == upstream {
		return "" // not a github.com URL
	}
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func (s *Strategy) scheduleLFSSnapshotJobs(repo *gitclone.Repository) {
	s.scheduler.SubmitPeriodicJob(repo.UpstreamURL(), "lfs-snapshot-periodic", s.config.SnapshotInterval, func(ctx context.Context) error {
		return s.generateAndUploadLFSSnapshot(ctx, repo)
	})
}

func (s *Strategy) handleLFSSnapshotRequest(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	s.serveCachedArtifact(w, r, host, pathValue, "lfs-snapshot.tar.zst", "lfs-snapshot")
}
