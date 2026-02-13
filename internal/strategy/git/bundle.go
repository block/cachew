package git

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) generateAndUploadBundle(ctx context.Context, repo *gitclone.Repository) error {
	logger := logging.FromContext(ctx)
	upstream := repo.UpstreamURL()

	logger.InfoContext(ctx, fmt.Sprintf("Bundle generation started: %s", upstream), "upstream", upstream)

	cacheKey := cache.NewKey(upstream + ".bundle")

	headers := http.Header{
		"Content-Type": []string{"application/x-git-bundle"},
	}
	ttl := 7 * 24 * time.Hour
	w, err := s.cache.Create(ctx, cacheKey, headers, ttl)
	if err != nil {
		return errors.Wrap(err, "create cache entry")
	}
	defer w.Close()

	err = errors.Wrap(repo.WithReadLock(func() error {
		var stderr bytes.Buffer
		// Use --branches --remotes to include all branches but exclude tags (which can be massive)
		// #nosec G204 - repo.Path() is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "bundle", "create", "-", "--branches", "--remotes")
		cmd.Stdout = w
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "bundle generation failed: %s", stderr.String())
		}

		return nil
	}), "generate bundle")
	if err != nil {
		logger.ErrorContext(ctx, fmt.Sprintf("Bundle generation failed for %s: %v", upstream, err), "upstream", upstream, "error", err)
		return err
	}

	logger.InfoContext(ctx, fmt.Sprintf("Bundle generation completed: %s", upstream), "upstream", upstream)
	return nil
}
