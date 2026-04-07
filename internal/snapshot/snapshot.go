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
	return CreatePaths(ctx, remote, key, directory, filepath.Base(directory), []string{"."}, ttl, excludePatterns, threads, extraHeaders...)
}

// CreatePaths archives named paths within baseDir using tar with zstd compression,
// then uploads the resulting archive to the cache.
//
// The archive preserves all file permissions, ownership, and symlinks.
// Each entry in includePaths is archived relative to baseDir and must exist.
// This allows callers to archive either an entire directory with "." or a
// specific subtree such as "lfs" while preserving that relative path prefix.
// Exclude patterns use tar's --exclude syntax.
// threads controls zstd parallelism; 0 uses all available CPU cores.
func CreatePaths(ctx context.Context, remote cache.Cache, key cache.Key, baseDir, archiveName string, includePaths []string, ttl time.Duration, excludePatterns []string, threads int, extraHeaders ...http.Header) error {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	if len(includePaths) == 0 {
		return errors.New("includePaths must not be empty")
	}

	if info, err := os.Stat(baseDir); err != nil {
		return errors.Wrap(err, "failed to stat base directory")
	} else if !info.IsDir() {
		return errors.Errorf("not a directory: %s", baseDir)
	}
	for _, path := range includePaths {
		targetPath := filepath.Join(baseDir, path)
		if _, err := os.Stat(targetPath); err != nil {
			return errors.Wrapf(err, "failed to stat include path %q", path)
		}
	}

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

	wc, err := remote.Create(ctx, key, headers, ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	tarArgs := []string{"-cpf", "-", "-C", baseDir}
	for _, pattern := range excludePatterns {
		tarArgs = append(tarArgs, "--exclude", pattern)
	}
	tarArgs = append(tarArgs, "--")
	tarArgs = append(tarArgs, includePaths...)

	if err := runTarZstdPipeline(ctx, tarArgs, threads, wc); err != nil {
		return errors.Join(err, wc.Close())
	}
	return errors.Wrap(wc.Close(), "failed to close writer")
}

// runTarZstdPipeline runs tar piped through zstd, writing compressed output to w.
// The caller is responsible for closing w after this returns.
func runTarZstdPipeline(ctx context.Context, tarArgs []string, threads int, w io.Writer) error {
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	zstdCmd := exec.CommandContext(ctx, "zstd", "-c", fmt.Sprintf("-T%d", threads)) //nolint:gosec // threads is a validated integer, not user input

	// Use manual pipe so we can close both ends in the parent after starting
	// children. This prevents deadlock if zstd exits while tar is still writing:
	// closing the read end ensures tar receives SIGPIPE instead of blocking.
	pr, pw, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stdout = pw
	tarCmd.Stderr = &tarStderr

	zstdCmd.Stdin = pr
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		pw.Close() //nolint:errcheck,gosec // best-effort cleanup
		pr.Close() //nolint:errcheck,gosec // best-effort cleanup
		return errors.Wrap(err, "failed to start tar")
	}
	pw.Close() //nolint:errcheck,gosec // parent no longer needs write end; tar holds its own copy

	if err := zstdCmd.Start(); err != nil {
		pr.Close() //nolint:errcheck,gosec // let tar receive SIGPIPE so it exits
		return errors.Join(errors.Wrap(err, "failed to start zstd"), tarCmd.Wait())
	}
	pr.Close() //nolint:errcheck,gosec // parent no longer needs read end; if zstd dies, tar gets SIGPIPE

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

	pr, pw, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}

	var tarStderr, zstdStderr bytes.Buffer
	tarCmd.Stdout = pw
	tarCmd.Stderr = &tarStderr

	zstdCmd.Stdin = pr
	zstdCmd.Stdout = w
	zstdCmd.Stderr = &zstdStderr

	if err := tarCmd.Start(); err != nil {
		pw.Close() //nolint:errcheck,gosec // best-effort cleanup
		pr.Close() //nolint:errcheck,gosec // best-effort cleanup
		return errors.Wrap(err, "failed to start tar")
	}
	pw.Close() //nolint:errcheck,gosec // parent no longer needs write end; tar holds its own copy

	if err := zstdCmd.Start(); err != nil {
		pr.Close() //nolint:errcheck,gosec // let tar receive SIGPIPE so it exits
		return errors.Join(errors.Wrap(err, "failed to start zstd"), tarCmd.Wait())
	}
	pr.Close() //nolint:errcheck,gosec // parent no longer needs read end; if zstd dies, tar gets SIGPIPE

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

	pr, pw, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}

	var zstdStderr, tarStderr bytes.Buffer
	zstdCmd.Stdin = r
	zstdCmd.Stdout = pw
	zstdCmd.Stderr = &zstdStderr

	tarCmd.Stdin = pr
	tarCmd.Stderr = &tarStderr

	if err := zstdCmd.Start(); err != nil {
		pw.Close() //nolint:errcheck,gosec // best-effort cleanup
		pr.Close() //nolint:errcheck,gosec // best-effort cleanup
		return errors.Wrap(err, "failed to start zstd")
	}
	pw.Close() //nolint:errcheck,gosec // parent no longer needs write end; zstd holds its own copy

	if err := tarCmd.Start(); err != nil {
		pr.Close() //nolint:errcheck,gosec // let zstd receive SIGPIPE so it exits
		return errors.Join(errors.Wrap(err, "failed to start tar"), zstdCmd.Wait())
	}
	pr.Close() //nolint:errcheck,gosec // parent no longer needs read end; if tar dies, zstd gets SIGPIPE

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
