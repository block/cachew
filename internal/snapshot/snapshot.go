// Package snapshot provides streaming directory archival and restoration using tar and zstd.
package snapshot

import (
	"bufio"
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
	"github.com/klauspost/compress/zstd"

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

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Join(errors.Wrap(err, "failed to create tar stdout pipe"), wc.Close())
	}

	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Join(errors.Wrap(err, "failed to start tar"), wc.Close())
	}

	// Compression uses the in-process klauspost/compress/zstd encoder with NumCPU
	// goroutines, producing parallel frames that can be decompressed in parallel.
	// This eliminates the zstd subprocess (one fewer fork/exec, one fewer
	// kernel pipe) and removes the runtime dependency on the zstd binary.
	enc, err := zstd.NewWriter(wc,
		zstd.WithEncoderConcurrency(threads),
		zstd.WithWindowSize(zstd.MaxWindowSize))
	if err != nil {
		return errors.Join(errors.Wrap(err, "failed to create zstd encoder"), tarCmd.Wait(), wc.Close())
	}

	_, copyErr := io.Copy(enc, tarStdout)
	tarStdout.Close() //nolint:errcheck,gosec // best-effort; tar will exit via SIGPIPE
	encErr := enc.Close()
	tarErr := tarCmd.Wait()
	closeErr := wc.Close()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar failed: %w: %s", tarErr, tarStderr.String()))
	}
	if copyErr != nil {
		errs = append(errs, errors.Wrap(copyErr, "failed to copy tar output to zstd encoder"))
	}
	if encErr != nil {
		errs = append(errs, errors.Wrap(encErr, "failed to close zstd encoder"))
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

	tarStdout, err := tarCmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to create tar stdout pipe")
	}

	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	if err := tarCmd.Start(); err != nil {
		return errors.Wrap(err, "failed to start tar")
	}

	enc, err := zstd.NewWriter(w,
		zstd.WithEncoderConcurrency(threads),
		zstd.WithWindowSize(zstd.MaxWindowSize))
	if err != nil {
		return errors.Join(errors.Wrap(err, "failed to create zstd encoder"), tarCmd.Wait())
	}

	_, copyErr := io.Copy(enc, tarStdout)
	tarStdout.Close() //nolint:errcheck,gosec // best-effort; tar will exit via SIGPIPE
	encErr := enc.Close()
	tarErr := tarCmd.Wait()

	var errs []error
	if tarErr != nil {
		errs = append(errs, errors.Errorf("tar failed: %w: %s", tarErr, tarStderr.String()))
	}
	if copyErr != nil {
		errs = append(errs, errors.Wrap(copyErr, "failed to copy tar output to zstd encoder"))
	}
	if encErr != nil {
		errs = append(errs, errors.Wrap(encErr, "failed to close zstd encoder"))
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

	// Decompression uses the in-process Go zstd decoder to avoid subprocess IPC
	// overhead (no kernel pipes, no process spawning, no goroutine synchronization
	// across process boundaries).
	// Buffer between the source reader and the zstd decoder. The reader may be an
	// io.Pipe (zero-copy, one Read per Write), so without buffering each small
	// decoder read stalls the upstream goroutine. 8 MiB lets the decoder read
	// ahead while the source fills the next chunk.
	dec, err := zstd.NewReader(bufio.NewReaderSize(r, 8<<20), zstd.WithDecoderConcurrency(threads))
	if err != nil {
		return errors.Wrap(err, "failed to create zstd decoder")
	}
	defer dec.Close()

	tarCmd := exec.CommandContext(ctx, "tar", "-xpf", "-", "-C", directory)
	tarCmd.Stdin = dec

	var tarStderr bytes.Buffer
	tarCmd.Stderr = &tarStderr

	if err := tarCmd.Run(); err != nil {
		return errors.Errorf("tar failed: %w: %s", err, tarStderr.String())
	}

	return nil
}
