package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/alecthomas/errors"
)

// Archive writes a tar+zstd stream of the given paths to w. Each entry in
// includePaths is relative to baseDir and must exist. Exclude patterns use
// tar's --exclude syntax. threads controls zstd parallelism; 0 uses all CPU
// cores.
func Archive(ctx context.Context, w io.Writer, baseDir string, includePaths []string, excludePatterns []string, threads int) error {
	if threads <= 0 {
		threads = runtime.GOMAXPROCS(0)
	}

	if len(includePaths) == 0 {
		return errors.New("includePaths must not be empty")
	}

	info, err := os.Stat(baseDir)
	if err != nil {
		return errors.Wrap(err, "failed to stat base directory")
	}
	if !info.IsDir() {
		return errors.Errorf("not a directory: %s", baseDir)
	}
	for _, path := range includePaths {
		if _, err := os.Stat(filepath.Join(baseDir, path)); err != nil {
			return errors.Wrapf(err, "failed to stat include path %q", path)
		}
	}

	tarArgs := []string{"-cpf", "-", "-C", baseDir}
	for _, pattern := range excludePatterns {
		tarArgs = append(tarArgs, "--exclude", pattern)
	}
	tarArgs = append(tarArgs, "--")
	tarArgs = append(tarArgs, includePaths...)

	return runTarZstdPipeline(ctx, tarArgs, threads, w)
}

// Extract decompresses a zstd+tar stream from r into directory, preserving
// file permissions, ownership, and symlinks. threads controls zstd
// parallelism; 0 uses all CPU cores.
func Extract(ctx context.Context, r io.Reader, directory string, threads int) error {
	if threads <= 0 {
		threads = runtime.GOMAXPROCS(0)
	}

	if err := os.MkdirAll(directory, 0o750); err != nil {
		return errors.Wrap(err, "failed to create target directory")
	}

	zstdCmd := exec.CommandContext(ctx, "zstd", "-dc", fmt.Sprintf("-T%d", threads)) //nolint:gosec
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
		pw.Close() //nolint:errcheck,gosec
		pr.Close() //nolint:errcheck,gosec
		return errors.Wrap(err, "failed to start zstd")
	}
	pw.Close() //nolint:errcheck,gosec

	if err := tarCmd.Start(); err != nil {
		pr.Close() //nolint:errcheck,gosec
		return errors.Join(errors.Wrap(err, "failed to start tar"), zstdCmd.Wait())
	}
	pr.Close() //nolint:errcheck,gosec

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

// runTarZstdPipeline runs tar piped through pzstd, writing compressed output
// to w. The caller is responsible for closing w after this returns.
func runTarZstdPipeline(ctx context.Context, tarArgs []string, threads int, w io.Writer) error {
	tarCmd := exec.CommandContext(ctx, "tar", tarArgs...)
	zstdCmd := exec.CommandContext(ctx, "pzstd", "-c", fmt.Sprintf("-p%d", threads)) //nolint:gosec

	// Manual pipe so we can close both ends in the parent after starting
	// children. Prevents deadlock if zstd exits while tar is still writing:
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
		pw.Close() //nolint:errcheck,gosec
		pr.Close() //nolint:errcheck,gosec
		return errors.Wrap(err, "failed to start tar")
	}
	pw.Close() //nolint:errcheck,gosec

	if err := zstdCmd.Start(); err != nil {
		pr.Close() //nolint:errcheck,gosec
		return errors.Join(errors.Wrap(err, "failed to start zstd"), tarCmd.Wait())
	}
	pr.Close() //nolint:errcheck,gosec

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
