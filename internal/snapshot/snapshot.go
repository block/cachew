// Package snapshot provides streaming directory archival and restoration using tar and zstd.
package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/alecthomas/errors"

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
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	// Verify directory exists
	if info, err := os.Stat(directory); err != nil {
		return errors.Wrap(err, "failed to stat directory")
	} else if !info.IsDir() {
		return errors.Errorf("not a directory: %s", directory)
	}

	headers := make(http.Header)
	headers.Set("Content-Type", "application/zstd")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(directory)+".tar.zst"))
	for _, eh := range extraHeaders {
		for k, vals := range eh {
			for _, v := range vals {
				headers.Set(k, v)
			}
		}
	}

	wc, err := remote.Create(ctx, key, headers, ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	tarArgs := []string{"-cpf", "-", "-C", directory}
	for _, pattern := range excludePatterns {
		tarArgs = append(tarArgs, "--exclude", pattern)
	}
	tarArgs = append(tarArgs, ".")

	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	zstdCmd := exec.CommandContext(ctx, "zstd", "-c", fmt.Sprintf("-T%d", threads)) //nolint:gosec // threads is a validated integer, not user input

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Join(errors.Wrap(err, "failed to create tar stdout pipe"), wc.Close())
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	zstdCmd.Stdin = tarStdout
	zstdCmd.Stdout = wc
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "failed to start tar"), wc.Close())
	}

	if err := zstdCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "failed to start zstd"), tarCmd.Wait(), wc.Close())
	}

	tarErr := tarCmd.Wait()
	zstdErr := zstdCmd.Wait()
	closeErr := wc.Close()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar failed: %w: %s", tarErr, tarStderr.String()))
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd failed: %w: %s", zstdErr, zstdStderr.String()))
	}
	if closeErr != nil {
		errs = append(errs, errors.Wrap(closeErr, "failed to close writer"))
	}

	return errors.Join(errs...)
}

// StreamTo archives a directory using tar with zstd compression and streams the
// output directly to w. Unlike Create, it does not upload to any cache backend.
// This is used on cache miss to serve the client immediately while a background
// job populates the cache.
func StreamTo(ctx context.Context, w io.Writer, directory string, excludePatterns []string, threads int) error {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	if info, err := os.Stat(directory); err != nil {
		return errors.Wrap(err, "failed to stat directory")
	} else if !info.IsDir() {
		return errors.Errorf("not a directory: %s", directory)
	}

	tarArgs := []string{"-cpf", "-", "-C", directory}
	for _, pattern := range excludePatterns {
		tarArgs = append(tarArgs, "--exclude", pattern)
	}
	tarArgs = append(tarArgs, ".")

	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	zstdCmd := exec.CommandContext(ctx, "zstd", "-c", fmt.Sprintf("-T%d", threads)) //nolint:gosec // threads is a validated integer, not user input

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to create tar stdout pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	zstdCmd.Stdin = tarStdout
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start tar")
	}

	if err := zstdCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "failed to start zstd"), tarCmd.Wait())
	}

	tarErr := tarCmd.Wait()
	zstdErr := zstdCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar failed: %w: %s", tarErr, tarStderr.String()))
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd failed: %w: %s", zstdErr, zstdStderr.String()))
	}

	return errors.Join(errs...)
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
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	if err := os.MkdirAll(directory, 0o750); err != nil {
		return errors.Wrap(err, "failed to create target directory")
	}

	zstdCmd := exec.CommandContext(ctx, "zstd", "-dc", fmt.Sprintf("-T%d", threads)) //nolint:gosec // threads is a validated integer, not user input
	tarCmd := exec.CommandContext(ctx, "tar", "-xpf", "-", "-C", directory)

	zstdCmd.Stdin = r
	zstdStdout, err := zstdCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to create zstd stdout pipe")
	}

	var zstdStderr, tarStderr bytes.Buffer
	zstdCmd.Stderr = &zstdStderr

	tarCmd.Stdin = zstdStdout
	tarCmd.Stderr = &tarStderr

	if err := zstdCmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start zstd")
	}

	if err := tarCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "failed to start tar"), zstdCmd.Wait())
	}

	zstdErr := zstdCmd.Wait()
	tarErr := tarCmd.Wait()

	var errs []error
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd failed: %w: %s", zstdErr, zstdStderr.String()))
	}
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar failed: %w: %s", tarErr, tarStderr.String()))
	}

	return errors.Join(errs...)
}
