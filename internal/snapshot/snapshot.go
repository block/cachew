// Package snapshot provides streaming directory archival and restoration using tar and zstd.
package snapshot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
)

//nolint:gochecknoglobals // OTel tracer instances are package-scoped by convention
var tracer = otel.Tracer("github.com/block/cachew/internal/snapshot")

// Create archives directory with tar and zstd, streaming it to the cache without temporary files.
//
// It preserves permissions, ownership and symlinks. Excludes use tar's --exclude syntax; threads 0 uses all
// cores. extraHeaders are merged into the metadata with the default Content-Type and Content-Disposition.
func Create(ctx context.Context, remote cache.Cache, key cache.Key, directory string, ttl time.Duration, excludePatterns []string, threads int, extraHeaders ...http.Header) error {
	return CreatePaths(ctx, remote, key, directory, filepath.Base(directory), []string{"."}, ttl, excludePatterns, threads, extraHeaders...)
}

// CreatePaths archives includePaths within baseDir with tar and zstd, then uploads the archive to the cache.
//
// It preserves permissions, ownership and symlinks. Each path is relative to baseDir and must exist.
// Excludes use tar's --exclude syntax; threads 0 uses all CPU cores.
func CreatePaths(ctx context.Context, remote cache.Cache, key cache.Key, baseDir, archiveName string, includePaths []string, ttl time.Duration, excludePatterns []string, threads int, extraHeaders ...http.Header) (returnErr error) {
	ctx, span := tracer.Start(ctx, "cachew.snapshot.create_and_upload",
		trace.WithAttributes(
			attribute.String("cache.key", key.String()),
			attribute.String("snapshot.archive_name", archiveName),
			attribute.Int("snapshot.include_paths", len(includePaths)),
			attribute.Int("snapshot.exclude_patterns", len(excludePatterns)),
			attribute.Int("snapshot.threads", threads),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	wc, err := remote.Create(ctx, key, Headers(archiveName, extraHeaders...), ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	if err := client.Archive(ctx, wc, baseDir, includePaths, excludePatterns, threads); err != nil {
		return errors.Join(err, wc.Abort(err))
	}
	return errors.Wrap(wc.Close(), "failed to close writer")
}

// Headers returns the headers stored with a snapshot archive named archiveName.
//
// Later extra headers override earlier ones.
func Headers(archiveName string, extraHeaders ...http.Header) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/zstd")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", archiveName+".tar.zst"))
	for _, eh := range extraHeaders {
		for k, vals := range eh {
			for _, v := range vals {
				headers.Set(k, v)
			}
		}
	}
	return headers
}

// StreamTo archives directory with tar and zstd and streams it to w, without uploading to a cache.
//
// It serves a client immediately on a cache miss while a background job populates the cache.
func StreamTo(ctx context.Context, w io.Writer, directory string, excludePatterns []string, threads int) error {
	return errors.WithStack(client.Archive(ctx, w, directory, []string{"."}, excludePatterns, threads))
}

// Restore streams an archive from the cache and extracts it to directory without temporary files.
//
// It decompresses with zstd and extracts with tar, preserving permissions, ownership and symlinks.
// threads controls zstd parallelism; 0 uses all CPU cores.
func Restore(ctx context.Context, remote cache.Cache, key cache.Key, directory string, threads int) (returnErr error) {
	ctx, span := tracer.Start(ctx, "cachew.snapshot.restore",
		trace.WithAttributes(
			attribute.String("cache.key", key.String()),
			attribute.Int("snapshot.threads", threads),
		),
	)
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		span.End()
	}()

	rc, _, err := remote.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to open object")
	}
	defer rc.Close()

	return Extract(ctx, rc, directory, threads)
}

// Extract decompresses a zstd+tar stream into directory, preserving permissions, ownership and symlinks.
//
// threads controls zstd parallelism; 0 uses all CPU cores.
func Extract(ctx context.Context, r io.Reader, directory string, threads int) error {
	return errors.WithStack(client.Extract(ctx, r, directory, threads))
}
