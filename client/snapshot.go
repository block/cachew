package client

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/alecthomas/errors"
)

// SnapshotOptions control how an archive is created and uploaded.
type SnapshotOptions struct {
	// TTL for the uploaded object. Zero uses the server default.
	TTL time.Duration
	// Exclude patterns (tar --exclude syntax).
	Exclude []string
	// ZstdThreads controls zstd parallelism; 0 uses all CPU cores.
	ZstdThreads int
	// ExtraHeaders are merged into the upload headers alongside Content-Type
	// and Content-Disposition.
	ExtraHeaders http.Header
}

// RestoreOptions control how an archive is downloaded and extracted.
type RestoreOptions struct {
	// ZstdThreads controls zstd parallelism; 0 uses all CPU cores.
	ZstdThreads int
}

// Snapshot archives a directory and uploads the tar+zstd stream under the
// given key.
func (c *Client) Snapshot(ctx context.Context, key Key, directory string, opts SnapshotOptions) error {
	return c.SnapshotPaths(ctx, key, directory, filepath.Base(directory), []string{"."}, opts)
}

// SnapshotPaths archives named paths within baseDir and uploads the tar+zstd
// stream under the given key. archiveName is used to set the upload's
// Content-Disposition filename.
func (c *Client) SnapshotPaths(ctx context.Context, key Key, baseDir, archiveName string, includePaths []string, opts SnapshotOptions) error {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/zstd")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", archiveName+".tar.zst"))
	for k, values := range opts.ExtraHeaders {
		for _, v := range values {
			headers.Set(k, v)
		}
	}

	wc, err := c.Create(ctx, key, headers, opts.TTL)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	if err := Archive(ctx, wc, baseDir, includePaths, opts.Exclude, opts.ZstdThreads); err != nil {
		return errors.Join(err, wc.Close())
	}
	return errors.Wrap(wc.Close(), "failed to close writer")
}

// Restore downloads an archive by key and extracts it into directory.
func (c *Client) Restore(ctx context.Context, key Key, directory string, opts RestoreOptions) error {
	rc, _, err := c.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to open object")
	}
	defer rc.Close()

	return errors.WithStack(Extract(ctx, rc, directory, opts.ZstdThreads))
}
