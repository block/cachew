//go:build !linux

package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/alecthomas/errors"
)

// extractStream pipes r through zstd into the system tar.
func extractStream(ctx context.Context, r io.Reader, directory string, threads int) error {
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
