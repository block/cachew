// Package snapshot provides streaming directory archival and restoration using tar and zstd.
package snapshot

import (
	"archive/tar"
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
	"strings"
	"sync"
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

const (
	// extractWorkers is the number of goroutines writing files concurrently
	// during parallel tar extraction. Hides per-file open/write/close syscall
	// latency so the tar-stream reader (and download pipeline behind it) is not
	// stalled waiting for individual file writes to complete.
	// Benchmarked on r8id.metal-48xlarge (NVMe, 96 cores) with a 334K-file
	// bundle: 64 workers = 6.27s, 128 = 6.84s (extra GC pressure outweighs
	// any I/O concurrency gain).
	extractWorkers = 64
	// maxParallelFileSize is the largest file that will be buffered in memory
	// and dispatched to the worker pool. Files larger than this are written
	// inline in the main goroutine to keep peak memory bounded.
	// At 4 MiB, 99.97% of Gradle cache entries go through the parallel path.
	maxParallelFileSize = 4 << 20 // 4 MiB
)

// Extract decompresses a zstd+tar stream into directory, preserving all file
// permissions, ownership, and symlinks. threads controls zstd parallelism;
// 0 uses all available CPU cores.
//
// The single-threaded bottleneck on restore is writing files to disk. Even
// though tar entries must be read sequentially (the format has no index), the
// actual file writes are independent. The extractor dispatches each entry
// (buffered in memory, ≤4 MiB) to one of 64 worker goroutines that write
// concurrently. This hides the per-file syscall latency (~20µs × N files)
// behind parallelism.
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

	return extractTarParallel(ctx, dec, directory)
}

type writeJob struct {
	target string
	mode   os.FileMode
	data   []byte
}

// safePath validates that name is a relative path that stays within dir when
// joined. It rejects absolute paths and parent traversals (".."). Returns the
// resolved path under dir.
func safePath(dir, name string) (string, error) {
	clean := filepath.Clean(name)
	if filepath.IsAbs(clean) {
		return "", errors.Errorf("path %q is absolute", name)
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.Errorf("path %q escapes destination directory", name)
	}
	joined := filepath.Join(dir, clean)
	if !strings.HasPrefix(joined, dir+string(os.PathSeparator)) && joined != dir {
		return "", errors.Errorf("path %q resolves outside destination directory", name)
	}
	return joined, nil
}

// extractTarParallel reads a tar stream and writes files using a pool of
// goroutines. The main goroutine reads tar entries and buffers small file
// contents; workers write those files to disk concurrently. Large files are
// written inline to keep memory use bounded.
func extractTarParallel(ctx context.Context, r io.Reader, dir string) error {
	// Resolve dir to absolute so containment checks are reliable.
	var err error
	dir, err = filepath.Abs(dir)
	if err != nil {
		return errors.Wrap(err, "resolve destination directory")
	}

	jobs := make(chan writeJob, extractWorkers*2)

	var (
		wg           sync.WaitGroup
		writeErrOnce sync.Once
		writeErr     error
	)

	for range extractWorkers {
		wg.Go(func() {
			for job := range jobs {
				f, err := os.OpenFile(job.target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, job.mode)
				if err != nil {
					writeErrOnce.Do(func() { writeErr = errors.Errorf("open %s: %w", filepath.Base(job.target), err) })
					continue
				}
				if _, err := f.Write(job.data); err != nil {
					f.Close() //nolint:errcheck,gosec
					writeErrOnce.Do(func() { writeErr = errors.Errorf("write %s: %w", filepath.Base(job.target), err) })
					continue
				}
				if err := f.Close(); err != nil {
					writeErrOnce.Do(func() { writeErr = errors.Errorf("close %s: %w", filepath.Base(job.target), err) })
				}
			}
		})
	}

	copyBuf := make([]byte, 1<<20) // reused only for inline large-file writes

	// createdDirs is accessed only by the main goroutine, so no mutex needed.
	createdDirs := make(map[string]struct{})
	ensureDir := func(d string, mode os.FileMode) error {
		if _, ok := createdDirs[d]; ok {
			return nil
		}
		if err := os.MkdirAll(d, mode); err != nil { //nolint:gosec // path is validated by caller
			return errors.Wrap(err, "mkdir")
		}
		createdDirs[d] = struct{}{}
		return nil
	}

	tr := tar.NewReader(r)
	var readErr error
loop:
	for {
		if err := ctx.Err(); err != nil {
			readErr = errors.Wrap(err, "context cancelled")
			break
		}

		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			readErr = errors.Wrap(err, "read tar entry")
			break
		}

		target, err := safePath(dir, hdr.Name)
		if err != nil {
			readErr = errors.Errorf("unsafe tar entry %q: %w", hdr.Name, err)
			break
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := ensureDir(target, hdr.FileInfo().Mode()); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break loop
			}

		case tar.TypeLink:
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for hardlink %s: %w", hdr.Name, err)
				break loop
			}
			linkTarget, err := safePath(dir, hdr.Linkname)
			if err != nil {
				readErr = errors.Errorf("unsafe hardlink target %q: %w", hdr.Linkname, err)
				break loop
			}
			if err := os.Link(linkTarget, target); err != nil {
				readErr = errors.Errorf("hardlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break loop
			}

		case tar.TypeReg:
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir %s: %w", hdr.Name, err)
				break loop
			}

			if hdr.Size <= maxParallelFileSize {
				// Buffer in memory and dispatch to worker pool so the main
				// goroutine can continue reading the tar stream immediately.
				buf := make([]byte, hdr.Size)
				if _, err := io.ReadFull(tr, buf); err != nil {
					readErr = errors.Errorf("read %s: %w", hdr.Name, err)
					break loop
				}
				// Propagate worker errors early.
				if writeErr != nil {
					readErr = writeErr
					break loop
				}
				jobs <- writeJob{target: target, mode: hdr.FileInfo().Mode(), data: buf}
			} else {
				// Large file: write inline to keep memory bounded.
				f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode()) //nolint:gosec // path traversal guarded above
				if err != nil {
					readErr = errors.Errorf("open %s: %w", hdr.Name, err)
					break loop
				}
				if _, err := io.CopyBuffer(f, io.LimitReader(tr, hdr.Size), copyBuf); err != nil {
					f.Close() //nolint:errcheck,gosec
					readErr = errors.Errorf("write %s: %w", hdr.Name, err)
					break loop
				}
				if err := f.Close(); err != nil {
					readErr = errors.Errorf("close %s: %w", hdr.Name, err)
					break loop
				}
			}

		case tar.TypeSymlink:
			if err := ensureDir(filepath.Dir(target), 0o755); err != nil {
				readErr = errors.Errorf("mkdir for symlink %s: %w", hdr.Name, err)
				break loop
			}
			if _, err := safePath(dir, hdr.Linkname); err != nil {
				readErr = errors.Errorf("unsafe symlink target %q: %w", hdr.Linkname, err)
				break loop
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				readErr = errors.Errorf("symlink %s → %s: %w", hdr.Name, hdr.Linkname, err)
				break loop
			}
		}
	}

	close(jobs)
	wg.Wait()

	if readErr != nil {
		return readErr
	}
	return writeErr
}
