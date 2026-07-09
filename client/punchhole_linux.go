//go:build linux

package client

import (
	"os"

	"github.com/alecthomas/errors"
	"golang.org/x/sys/unix"
)

// punchHole deallocates the byte range [off, off+length) of f, leaving a hole
// that reads as zeros without changing the file size. off and length must be
// multiples of the filesystem block size.
func punchHole(f *os.File, off, length int64) error {
	//nolint:gosec // file descriptors fit in int; the conversion every unix.* call site uses
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, off, length)
	return errors.Wrap(err, "punch hole")
}
