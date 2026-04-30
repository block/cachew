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

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
)

// Create archives a directory using tar with zstd compression, then uploads to the cache.
//
// The archive preserves all file permissions, ownership, and symlinks.
// The operation is fully streaming - no temporary files are created.
// Exclude patterns use tar's --exclude syntax.
// threads controls zstd parallelism; 0 uses all available CPU cores.
// Any extra headers are merged into the cache metadata alongside the default
// Content-Type and Content-Disposition headers.
func Create(ctx context.Context, remote cache.Cache, key cache.Key, directory string, ttl time.Duration, excludePatterns []string, threads int, extraHeaders ...http.Header) error {
	return CreatePaths(ctx, remote, key, directory, filepath.Base(directory), []string{"."}, ttl, excludePatterns, threads, extraHeaders...)
}

// CreatePaths archives named paths within baseDir using tar with zstd compression,
// then uploads the resulting archive to the cache.
//
// The archive preserves all file permissions, ownership, and symlinks.
// Each entry in includePaths is archived relative to baseDir and must exist.
// Exclude patterns use tar's --exclude syntax.
// threads controls zstd parallelism; 0 uses all available CPU cores.
func CreatePaths(ctx context.Context, remote cache.Cache, key cache.Key, baseDir, archiveName string, includePaths []string, ttl time.Duration, excludePatterns []string, threads int, extraHeaders ...http.Header) error {
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

	// Wrap the context so we can cancel the upload on archive failure,
	// preventing partial data from being persisted.
	createCtx, cancelCreate := context.WithCancelCause(ctx)
	defer cancelCreate(nil)

	wc, err := remote.Create(createCtx, key, headers, ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	if err := client.Archive(ctx, wc, baseDir, includePaths, excludePatterns, threads); err != nil {
		cancelCreate(err)
		return errors.Join(err, wc.Close())
	}
	return errors.Wrap(wc.Close(), "failed to close writer")
}

// StreamTo archives a directory using tar with zstd compression and streams the
// output directly to w. Unlike Create, it does not upload to any cache backend.
// This is used on cache miss to serve the client immediately while a background
// job populates the cache.
func StreamTo(ctx context.Context, w io.Writer, directory string, excludePatterns []string, threads int) error {
	return errors.WithStack(client.Archive(ctx, w, directory, []string{"."}, excludePatterns, threads))
}

// Restore downloads an archive from the cache and extracts it to a directory.
//
// The archive is decompressed with zstd and extracted with tar, preserving
// all file permissions, ownership, and symlinks.
// The operation is fully streaming - no temporary files are created.
// threads controls zstd parallelism; 0 uses all available CPU cores.
func Restore(ctx context.Context, remote cache.Cache, key cache.Key, directory string, threads int) error {
	rc, _, err := remote.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to open object")
	}
	defer rc.Close()

	return Extract(ctx, rc, directory, threads)
}

// Extract decompresses a zstd+tar stream into directory, preserving all file
// permissions, ownership, and symlinks. threads controls zstd parallelism;
// 0 uses all available CPU cores.
func Extract(ctx context.Context, r io.Reader, directory string, threads int) error {
	return errors.WithStack(client.Extract(ctx, r, directory, threads))
}
