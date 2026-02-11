// Package snapshot provides streaming directory archival and restoration using tar and zstd.
package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
)

// Create archives a directory using tar with zstd compression, then uploads to the cache.
//
// The archive preserves all file permissions, ownership, and symlinks.
// The operation is fully streaming - no temporary files are created.
// Exclude patterns use tar's --exclude syntax.
func Create(ctx context.Context, remote cache.Cache, strategyName string, key cache.Key, directory string, ttl time.Duration, excludePatterns []string) error {
	// Verify directory exists
	if info, err := os.Stat(directory); err != nil {
		return errors.Wrap(err, "failed to stat directory")
	} else if !info.IsDir() {
		return errors.Errorf("not a directory: %s", directory)
	}

	headers := make(http.Header)
	headers.Set("Content-Type", "application/zstd")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(directory)+".tar.zst"))

	wc, err := remote.Create(ctx, strategyName, key, headers, ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	tarArgs := []string{"-cpf", "-", "-C", directory}
	for _, pattern := range excludePatterns {
		tarArgs = append(tarArgs, "--exclude", pattern)
	}
	tarArgs = append(tarArgs, ".")

	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	zstdCmd := exec.CommandContext(ctx, "zstd", "-c", "-T0")

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

// Restore downloads an archive from the cache and extracts it to a directory.
//
// The archive is decompressed with zstd and extracted with tar, preserving
// all file permissions, ownership, and symlinks.
// The operation is fully streaming - no temporary files are created.
func Restore(ctx context.Context, remote cache.Cache, strategyName string, key cache.Key, directory string) error {
	rc, _, err := remote.Open(ctx, strategyName, key)
	if err != nil {
		return errors.Wrap(err, "failed to open object")
	}
	defer rc.Close()

	// Create target directory if it doesn't exist
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return errors.Wrap(err, "failed to create target directory")
	}

	zstdCmd := exec.CommandContext(ctx, "zstd", "-dc", "-T0")
	tarCmd := exec.CommandContext(ctx, "tar", "-xpf", "-", "-C", directory)

	zstdCmd.Stdin = rc
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
