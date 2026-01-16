package git

import (
	"context"
	"io"
	"log/slog"
	"net/textproto"
	"os"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

// cloneBundleLoop generates bundles periodically for a single clone.
func (s *Strategy) cloneBundleLoop(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	// Generate bundle immediately on start if one doesn't exist
	s.generateAndUploadBundleIfMissing(ctx, c)

	ticker := time.NewTicker(s.config.BundleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.DebugContext(ctx, "Bundle generator shutting down",
				slog.String("upstream", c.upstreamURL))
			return

		case <-ticker.C:
			s.generateAndUploadBundle(ctx, c)
		}
	}
}

// generateAndUploadBundleIfMissing generates a bundle only if one doesn't exist in cache.
func (s *Strategy) generateAndUploadBundleIfMissing(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	// Check if bundle already exists in cache
	cacheKey := cache.NewKey(c.upstreamURL + ".bundle")

	reader, _, err := s.cache.Open(ctx, cacheKey)
	if err == nil {
		// Bundle exists, close and skip generation
		_ = reader.Close()
		logger.DebugContext(ctx, "Bundle already exists in cache, skipping generation",
			slog.String("upstream", c.upstreamURL))
		return
	}

	// Only generate if the error is that the bundle doesn't exist
	if !errors.Is(err, os.ErrNotExist) {
		logger.ErrorContext(ctx, "Failed to check for existing bundle",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}

	// Bundle doesn't exist, generate it
	s.generateAndUploadBundle(ctx, c)
}

// generateAndUploadBundle generates a bundle and streams it directly to cache.
func (s *Strategy) generateAndUploadBundle(ctx context.Context, c *clone) {
	logger := logging.FromContext(ctx)

	logger.InfoContext(ctx, "Generating bundle",
		slog.String("upstream", c.upstreamURL))

	cacheKey := cache.NewKey(c.upstreamURL + ".bundle")

	// Create cache writer
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

	// Stream bundle directly to cache
	// #nosec G204 - c.path is controlled by us
	cmd, err := gitCommand(ctx, "", "-C", c.path,
		"bundle", "create", "-", "--branches")
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create git command",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}
	cmd.Stdout = w

	// Capture stderr for error reporting
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create stderr pipe",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}

	if err := cmd.Start(); err != nil {
		logger.ErrorContext(ctx, "Failed to start bundle generation",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return
	}

	stderr, _ := io.ReadAll(stderrPipe) //nolint:errcheck // Only used for logging

	if err := cmd.Wait(); err != nil {
		logger.ErrorContext(ctx, "Failed to generate bundle",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()),
			slog.String("stderr", string(stderr)))
		return
	}

	logger.InfoContext(ctx, "Bundle uploaded successfully",
		slog.String("upstream", c.upstreamURL))
}
