package client

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"github.com/alecthomas/errors"
	"golang.org/x/sys/unix"
)

const (
	maxExtractWriters = 8
	// decodedPipeBytes sizes the zstd pipe and the read buffer, cutting syscalls per decoded megabyte.
	decodedPipeBytes = 1 << 20
)

// extractStream decodes r with zstd and applies the tar stream in-process.
//
// A single tar process spends most of a restore waiting on per-file syscalls, so file contents are written
// concurrently while entries keep GNU tar's -xp semantics and archive order.
func extractStream(ctx context.Context, r io.Reader, directory string, threads int) error {
	zstdCmd := exec.CommandContext(ctx, "zstd", "-dc", fmt.Sprintf("-T%d", threads)) //nolint:gosec
	pr, pw, err := os.Pipe()
	if err != nil {
		return errors.Wrap(err, "failed to create pipe")
	}
	var zstdStderr bytes.Buffer
	zstdCmd.Stdin = r
	zstdCmd.Stdout = pw
	zstdCmd.Stderr = &zstdStderr
	if err := zstdCmd.Start(); err != nil {
		pw.Close() //nolint:errcheck,gosec
		pr.Close() //nolint:errcheck,gosec
		return errors.Wrap(err, "failed to start zstd")
	}
	pw.Close() //nolint:errcheck,gosec

	_, _ = unix.FcntlInt(pr.Fd(), unix.F_SETPIPE_SZ, decodedPipeBytes) //nolint:errcheck // only costs throughput
	br := bufio.NewReaderSize(pr, decodedPipeBytes)
	writers := min(runtime.GOMAXPROCS(0), maxExtractWriters)
	extractErr := newTarExtractor(directory, writers).extract(ctx, br)
	if extractErr == nil {
		// tar.Reader stops at the end-of-archive marker; consume the padding so zstd isn't killed by SIGPIPE.
		if _, err := io.Copy(io.Discard, br); err != nil {
			extractErr = errors.Wrap(err, "failed to read archive")
		}
	}
	pr.Close() //nolint:errcheck,gosec
	zstdErr := zstdCmd.Wait()

	var errs []error
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd failed: %w: %s", zstdErr, zstdStderr.String()))
	}
	if extractErr != nil {
		errs = append(errs, extractErr)
	}
	return errors.Join(errs...)
}

// nextTarEntry wraps tar.Reader.Next, leaving name checks to the extractor.
//
// GODEBUG=tarinsecurepath=0 makes Next reject names that GNU tar extracts after stripping leading slashes.
func nextTarEntry(tr *tar.Reader) (*tar.Header, error) { //nolint:privatedoc // explains why
	hdr, err := tr.Next()
	if errors.Is(err, tar.ErrInsecurePath) && hdr != nil {
		return hdr, nil
	}
	return hdr, errors.WithStack(err)
}
