package client

import (
	"os"
	"strings"

	"github.com/alecthomas/errors"
	"golang.org/x/sys/unix"
)

// destDir resolves extraction paths against an open descriptor for the destination, as GNU tar does after -C.
//
// Only the member name then counts towards PATH_MAX, so a long destination path doesn't make deep members fail.
type destDir struct {
	fd int
	// path is the cleaned destination that member paths are joined onto; dir is the caller's path, opened as given.
	path string
	dir  string
}

func (d *destDir) open() error {
	// Like tar -C, symlinks are followed, so a ".." after one leads somewhere filepath.Clean wouldn't.
	fd, err := unix.Open(d.dir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return errors.Wrapf(err, "failed to open %s", d.dir)
	}
	d.fd = fd
	return nil
}

func (d *destDir) close() error {
	return errors.Wrapf(unix.Close(d.fd), "failed to close %s", d.path)
}

func (d *destDir) rel(path string) string {
	// Callers build paths by joining member names onto the destination; anything else stays absolute.
	if path == d.path {
		return "."
	}
	prefix := strings.TrimSuffix(d.path, "/") + "/"
	if rel, ok := strings.CutPrefix(path, prefix); ok && rel != "" {
		return rel
	}
	return path
}

func (d *destDir) lstat(path string) (unix.Stat_t, error) {
	var st unix.Stat_t
	err := unix.Fstatat(d.fd, d.rel(path), &st, unix.AT_SYMLINK_NOFOLLOW)
	return st, errors.Wrapf(err, "failed to stat %s", path)
}

func (d *destDir) stat(path string) (unix.Stat_t, error) {
	var st unix.Stat_t
	err := unix.Fstatat(d.fd, d.rel(path), &st, 0)
	return st, errors.Wrapf(err, "failed to stat %s", path)
}

func (d *destDir) mkdir(path string, mode uint32) error {
	return errors.Wrapf(unix.Mkdirat(d.fd, d.rel(path), mode), "failed to create directory %s", path)
}

func (d *destDir) chmod(path string, mode uint32) error {
	return errors.Wrapf(unix.Fchmodat(d.fd, d.rel(path), mode, 0), "failed to chmod %s", path)
}

func (d *destDir) lchown(path string, uid, gid int) error {
	err := unix.Fchownat(d.fd, d.rel(path), uid, gid, unix.AT_SYMLINK_NOFOLLOW)
	return errors.Wrapf(err, "failed to chown %s", path)
}

func (d *destDir) setTimes(path string, times []unix.Timespec) error {
	err := unix.UtimesNanoAt(d.fd, d.rel(path), times, unix.AT_SYMLINK_NOFOLLOW)
	return errors.Wrapf(err, "failed to set times on %s", path)
}

func (d *destDir) create(path string, perm uint32) (*os.File, error) {
	for {
		fd, err := unix.Openat(d.fd, d.rel(path), unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, perm)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create %s", path)
		}
		return os.NewFile(uintptr(fd), path), nil
	}
}

func (d *destDir) symlink(target, path string) error {
	return errors.Wrapf(unix.Symlinkat(target, d.fd, d.rel(path)), "failed to create symlink %s", path)
}

func (d *destDir) link(source, path string) error {
	err := unix.Linkat(d.fd, d.rel(source), d.fd, d.rel(path), 0)
	return errors.Wrapf(err, "failed to link %s to %s", path, source)
}

func (d *destDir) mknod(path string, mode uint32, dev int) error {
	return errors.Wrapf(unix.Mknodat(d.fd, d.rel(path), mode, dev), "failed to create %s", path)
}

func (d *destDir) remove(path string) error {
	// As os.Remove: rmdir(file) fails with ENOTDIR, which shows that unlink's error is the one to report.
	rel := d.rel(path)
	err := unix.Unlinkat(d.fd, rel, 0)
	if err == nil {
		return nil
	}
	rmdirErr := unix.Unlinkat(d.fd, rel, unix.AT_REMOVEDIR)
	if rmdirErr == nil {
		return nil
	}
	if !errors.Is(rmdirErr, unix.ENOTDIR) {
		err = rmdirErr
	}
	return errors.Wrapf(err, "failed to remove %s", path)
}

func isDirStat(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR
}

func isSymlinkStat(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFLNK
}

func statID(st unix.Stat_t) fileID {
	return fileID{dev: uint64(st.Dev), ino: st.Ino} //nolint:unconvert // Dev is uint32 on some architectures
}
