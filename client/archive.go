package client

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

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
		if strings.Contains(path, "\\") || !filepath.IsLocal(path) {
			return errors.Errorf("include path %q must be relative and stay within base directory", path)
		}
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
// file permissions and symlinks. threads controls zstd parallelism; 0 uses all
// CPU cores.
//
// Existing read-only directories are made owner-writable so entries can be
// replaced. Archived directory modes are applied after their contents, and a
// failed extract restores pre-existing read-only directories.
func Extract(ctx context.Context, r io.Reader, directory string, threads int) (rerr error) {
	if threads <= 0 {
		threads = runtime.GOMAXPROCS(0)
	}

	if err := os.MkdirAll(directory, 0o750); err != nil {
		return errors.Wrap(err, "failed to create target directory")
	}

	relaxed, err := makeTreeWritable(directory)
	defer func() {
		if rerr != nil {
			restoreDirModes(relaxed)
		}
	}()
	if err != nil {
		return errors.Wrap(err, "failed to make target directory writable")
	}

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

	extractErr := safeExtractTar(ctx, pr, directory)
	pr.Close() //nolint:errcheck,gosec
	zstdErr := zstdCmd.Wait()

	var errs []error
	if extractErr != nil {
		errs = append(errs, extractErr)
	}
	if zstdErr != nil {
		errs = append(errs, errors.Errorf("zstd failed: %w: %s", zstdErr, zstdStderr.String()))
	}
	return errors.Join(errs...)
}

func safeExtractTar(ctx context.Context, r io.Reader, directory string) error {
	tr := tar.NewReader(r)
	var dirs []dirMode
	for {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		hdr, err := tr.Next()
		switch {
		case errors.Is(err, io.EOF):
			return applyDirModes(dirs)
		case err != nil:
			return errors.Wrap(err, "failed to read tar header")
		}

		if err := extractTarEntry(directory, hdr, tr, &dirs); err != nil {
			return err
		}
	}
}

func applyDirModes(dirs []dirMode) error {
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i].path, dirs[i].mode); err != nil {
			return errors.Wrapf(err, "failed to chmod %s", dirs[i].path)
		}
	}
	return nil
}

func extractTarEntry(directory string, hdr *tar.Header, r io.Reader, dirs *[]dirMode) error {
	target, rel, err := safeArchivePath(directory, hdr.Name)
	if err != nil {
		return err
	}
	mode := tarMode(hdr)
	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := mkdirAllWithin(directory, rel, mode|0o700); err != nil {
			return err
		}
		*dirs = append(*dirs, dirMode{path: target, mode: mode})
	case tar.TypeReg, 0:
		return extractRegularFile(directory, rel, target, mode, r)
	case tar.TypeSymlink:
		return extractSymlink(directory, rel, target, hdr.Linkname)
	case tar.TypeLink:
		return extractHardLink(directory, rel, target, hdr.Linkname)
	case tar.TypeXHeader, tar.TypeXGlobalHeader:
		return nil
	default:
		return errors.Errorf("unsupported tar entry %q type %d", hdr.Name, hdr.Typeflag)
	}
	return nil
}

func tarMode(hdr *tar.Header) fs.FileMode {
	return fs.FileMode(uint32(hdr.Mode & int64(fs.ModePerm)))
}

func extractRegularFile(directory, rel, target string, mode fs.FileMode, r io.Reader) error {
	if err := mkdirAllWithin(directory, filepath.Dir(rel), 0o750); err != nil {
		return err
	}
	if err := ensureNoSymlinkParents(directory, rel); err != nil {
		return err
	}
	if err := rejectSymlink(target); err != nil {
		return err
	}
	if err := removeExistingNonDir(target); err != nil {
		return err
	}
	return writeRegularFile(target, mode, r)
}

func extractSymlink(directory, rel, target, linkname string) error {
	if err := validateSymlinkTarget(rel, linkname); err != nil {
		return err
	}
	if err := mkdirAllWithin(directory, filepath.Dir(rel), 0o750); err != nil {
		return err
	}
	if err := ensureNoSymlinkParents(directory, rel); err != nil {
		return err
	}
	if err := removeExistingNonDir(target); err != nil {
		return err
	}
	if err := os.Symlink(linkname, target); err != nil {
		return errors.Wrapf(err, "failed to create symlink %s", target)
	}
	return nil
}

func extractHardLink(directory, rel, target, linkname string) error {
	linkTarget, _, err := safeArchivePath(directory, linkname)
	if err != nil {
		return err
	}
	if err := rejectSymlink(linkTarget); err != nil {
		return err
	}
	if err := mkdirAllWithin(directory, filepath.Dir(rel), 0o750); err != nil {
		return err
	}
	if err := ensureNoSymlinkParents(directory, rel); err != nil {
		return err
	}
	if err := removeExistingNonDir(target); err != nil {
		return err
	}
	if err := os.Link(linkTarget, target); err != nil {
		return errors.Wrapf(err, "failed to create hard link %s", target)
	}
	return nil
}

func safeArchivePath(root, name string) (string, string, error) {
	if strings.Contains(name, "\\") {
		return "", "", errors.Errorf("archive path %q contains a path separator unsupported by tar", name)
	}
	rel := filepath.FromSlash(name)
	if !filepath.IsLocal(rel) {
		return "", "", errors.Errorf("archive path %q must be relative and stay within target directory", name)
	}
	rel = filepath.Clean(rel)
	return filepath.Join(root, rel), rel, nil
}

func validateSymlinkTarget(rel, linkname string) error {
	if strings.Contains(linkname, "\\") {
		return errors.Errorf("symlink target %q contains a path separator unsupported by tar", linkname)
	}
	linkTarget := filepath.FromSlash(linkname)
	if filepath.IsAbs(linkTarget) || !filepath.IsLocal(filepath.Join(filepath.Dir(rel), linkTarget)) {
		return errors.Errorf("symlink target %q must be relative and stay within target directory", linkname)
	}
	return nil
}

func mkdirAllWithin(root, rel string, mode fs.FileMode) error {
	if rel == "." {
		return nil
	}
	current := root
	for part := range strings.SplitSeq(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		switch {
		case err == nil && info.Mode()&fs.ModeSymlink != 0:
			return errors.Errorf("refusing to extract through symlink %s", current)
		case err == nil && !info.IsDir():
			return errors.Errorf("refusing to create directory over non-directory %s", current)
		case err == nil:
			continue
		case !errors.Is(err, os.ErrNotExist):
			return errors.Wrapf(err, "failed to stat %s", current)
		}
		if err := os.Mkdir(current, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return errors.Wrapf(err, "failed to create directory %s", current)
		}
	}
	return nil
}

func ensureNoSymlinkParents(root, rel string) error {
	dir := filepath.Dir(rel)
	if dir == "." {
		return nil
	}
	current := root
	for part := range strings.SplitSeq(dir, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return errors.Wrapf(err, "failed to stat %s", current)
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			return errors.Errorf("refusing to extract through symlink %s", current)
		}
	}
	return nil
}

func rejectSymlink(path string) error {
	info, err := os.Lstat(path) //nolint:gosec
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "failed to stat %s", path)
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return errors.Errorf("refusing to replace symlink %s", path)
	}
	return nil
}

func removeExistingNonDir(path string) error {
	info, err := os.Lstat(path) //nolint:gosec
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "failed to stat %s", path)
	}
	if info.IsDir() {
		return errors.Errorf("refusing to replace directory %s", path)
	}
	if err := os.Remove(path); err != nil { //nolint:gosec
		return errors.Wrapf(err, "failed to remove %s", path)
	}
	return nil
}

func writeRegularFile(path string, mode fs.FileMode, r io.Reader) (rerr error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec
	if err != nil {
		return errors.Wrapf(err, "failed to create file %s", path)
	}
	defer func() {
		rerr = errors.Join(rerr, f.Close())
	}()
	if _, err := io.Copy(f, r); err != nil {
		return errors.Wrapf(err, "failed to write file %s", path)
	}
	if err := f.Chmod(mode); err != nil {
		return errors.Wrapf(err, "failed to chmod %s", path)
	}
	return nil
}

// dirMode is a directory path and its pre-relaxation mode.
type dirMode struct {
	path string
	mode fs.FileMode
}

// makeTreeWritable adds owner-write to root and every directory beneath it,
// returning the changed directories with their original modes. tar overwrites
// a file by unlinking it, which requires a writable parent directory.
func makeTreeWritable(root string) ([]dirMode, error) {
	var changed []dirMode
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		switch {
		case walkErr != nil:
			return walkErr
		case !d.IsDir():
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return errors.Wrapf(err, "failed to stat %s", path)
		}
		// Keep setuid/setgid/sticky so relaxing and restoring don't strip them.
		mode := info.Mode() & (fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky)
		if mode&0o200 == 0 {
			if err := os.Chmod(path, mode|0o200); err != nil {
				return errors.Wrapf(err, "failed to make %s writable", path)
			}
			changed = append(changed, dirMode{path: path, mode: mode})
		}
		return nil
	})
	return changed, errors.Wrap(err, "failed to walk target directory")
}

// restoreDirModes best-effort restores modes relaxed by makeTreeWritable;
// failures are ignored so the extraction error stays the one reported.
func restoreDirModes(dirs []dirMode) {
	for _, d := range dirs {
		os.Chmod(d.path, d.mode) //nolint:errcheck,gosec
	}
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
