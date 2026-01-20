package git

import (
	"context"
	"io"
	"log/slog"
	"net/textproto"
	"strings"
	"time"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) generateAndUploadBundle(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	logger.InfoContext(ctx, "Generating bundle",
		slog.String("upstream", c.upstreamURL))

	cacheKey := cache.NewKey(c.upstreamURL + ".bundle")

	headers := textproto.MIMEHeader{
		"Content-Type": []string{"application/x-git-bundle"},
	}
	ttl := 7 * 24 * time.Hour
	w, err := s.cache.Create(ctx, cacheKey, headers, ttl)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create cache entry",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}
	defer w.Close()

	// Use --branches --remotes to include all branches but exclude tags (which can be massive)
	// #nosec G204 - c.path is controlled by us
	args := []string{"-C", c.path, "bundle", "create", "-", "--branches", "--remotes"}
	cmd, err := gitCommand(ctx, "", args...)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create git command",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}
	cmd.Stdout = w

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create stderr pipe",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}

	logger.DebugContext(ctx, "Starting bundle generation",
		slog.String("upstream", c.upstreamURL),
		slog.String("command", "git "+strings.Join(args, " ")))

	if err := cmd.Start(); err != nil {
		logger.ErrorContext(ctx, "Failed to start bundle generation",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}

	stderr, _ := io.ReadAll(stderrPipe) //nolint:errcheck

	if err := cmd.Wait(); err != nil {
		logger.ErrorContext(ctx, "Failed to generate bundle",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()),
			slog.String("stderr", string(stderr)))
		return
	}

	if len(stderr) > 0 {
		logger.DebugContext(ctx, "Bundle generation stderr",
			slog.String("upstream", c.upstreamURL),
			slog.String("stderr", string(stderr)))
	}

	logger.InfoContext(ctx, "Bundle uploaded successfully",
		slog.String("upstream", c.upstreamURL))
}
