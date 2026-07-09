//go:build darwin

package client

import (
	"os"
	"runtime"
	"unsafe"

	"github.com/alecthomas/errors"
	"golang.org/x/sys/unix"
)

// fpunchhole mirrors struct fpunchhole_t from <sys/fcntl.h>. x/sys/unix
// defines F_PUNCHHOLE for darwin but neither the argument struct nor a
// wrapper, so we pass the pointer through FcntlInt the same way
// unix.FcntlFstore does for F_PREALLOCATE.
type fpunchhole struct {
	flags    uint32 // fp_flags: no flags are currently defined
	reserved uint32
	offset   int64
	length   int64
}

// punchHole deallocates the byte range [off, off+length) of f, leaving a hole
// that reads as zeros without changing the file size. off and length must be
// multiples of the filesystem block size. Requires APFS.
func punchHole(f *os.File, off, length int64) error {
	arg := fpunchhole{offset: off, length: length}
	//nolint:gosec // pointers fit in int on darwin; the same pattern unix.FcntlFstore uses
	_, err := unix.FcntlInt(f.Fd(), unix.F_PUNCHHOLE, int(uintptr(unsafe.Pointer(&arg))))
	runtime.KeepAlive(&arg)
	return errors.Wrap(err, "punch hole")
}
