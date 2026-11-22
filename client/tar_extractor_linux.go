package client

import (
	"archive/tar"
	"context"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
)

// GNU tar type flags that archive/tar has no names for.
const (
	gnuTypeDumpDir      = 'D'
	gnuTypeMultiVolume  = 'M'
	gnuTypeVolumeHeader = 'V'
)

const (
	// maxBufferedFile is the largest file read whole before it is queued; larger files are streamed in chunks.
	maxBufferedFile = 1 << 20
	streamChunkSize = 4 << 20
	// maxBufferedBytes bounds the file contents held in memory while they wait for a writer.
	maxBufferedBytes = 64 << 20
	copyBufferBytes  = 1 << 20
	// pruneInFlightAt is the pending-write map size that triggers dropping completed writes.
	pruneInFlightAt = 1024
	// blockSize is the tar record unit; a complete archive is a whole number of blocks.
	blockSize = 512
)

// entryStat is the metadata applied to an extracted entry.
type entryStat struct {
	uid, gid int
	mode     uint32
	mtime    unix.Timespec
}

// fileWrite is a buffered regular file queued for a writer goroutine.
type fileWrite struct {
	target string
	stat   entryStat
	data   []byte
	done   chan struct{}
}

// fileID identifies an inode.
type fileID struct {
	dev, ino uint64
}

// delayedLink is a placeholder file that becomes a link once extraction finishes, as in GNU tar.
//
// GNU tar never creates a symlink with an absolute or ".." target, or a hard link to one, until the end, so later
// entries cannot be written through it.
type delayedLink struct {
	path    string
	id      fileID
	symlink bool
	target  string
	stat    entryStat
	// replaced is set once a later entry claims the path; inode numbers alone can be reused.
	replaced bool
}

// tarExtractor applies a tar stream to root with the behaviour of GNU tar's "tar -xp".
//
// Regular files up to maxBufferedFile are written by writer goroutines and larger ones are streamed to their own
// goroutine. Before an entry touches a path it waits for any pending write to it, so entries apply in archive order.
type tarExtractor struct {
	root       string
	asRoot     bool
	umask      int
	numWriters int
	buffer     *semaphore.Weighted
	jobs       chan fileWrite
	streams    sync.WaitGroup
	// chunks recycles stream buffers; fresh 4 MiB allocations cost more in page faults than the write itself.
	chunks   chan []byte
	failOnce sync.Once
	failed   chan struct{}
	failErr  error

	// The fields below belong to the goroutine reading the archive.
	inFlight map[string]chan struct{}
	// dirs holds directories known to exist whose path goes through no symlink.
	dirs         map[string]struct{}
	dirStack     dirStack
	delayed      []delayedLink
	delayedAt    map[string]int
	placeholders map[fileID]struct{}
	users        map[string]int
	groups       map[string]int
	globalPAX    map[string]string
	copyBuf      []byte
}

func newTarExtractor(root string, writers int) *tarExtractor {
	root = filepath.Clean(root)
	return &tarExtractor{
		root:         root,
		asRoot:       os.Geteuid() == 0,
		umask:        processUmask(),
		numWriters:   writers,
		buffer:       semaphore.NewWeighted(maxBufferedBytes),
		jobs:         make(chan fileWrite, 4*writers),
		chunks:       make(chan []byte, maxBufferedBytes/streamChunkSize),
		failed:       make(chan struct{}),
		inFlight:     map[string]chan struct{}{},
		dirs:         map[string]struct{}{root: {}},
		dirStack:     newDirStack(),
		delayedAt:    map[string]int{},
		placeholders: map[fileID]struct{}{},
		users:        map[string]int{},
		groups:       map[string]int{},
		copyBuf:      make([]byte, copyBufferBytes),
	}
}

func processUmask() int {
	// unix.Umask reads the umask by changing it, racing other goroutines; unknown is 0o777, assuming all bits masked.
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0o777
	}
	for line := range strings.Lines(string(status)) {
		if value, ok := strings.CutPrefix(line, "Umask:"); ok {
			umask, err := strconv.ParseInt(strings.TrimSpace(value), 8, 32)
			if err != nil {
				return 0o777
			}
			return int(umask)
		}
	}
	return 0o777
}

func (e *tarExtractor) extract(ctx context.Context, r io.Reader) error {
	var writers sync.WaitGroup
	for range e.numWriters {
		writers.Go(e.runWriter)
	}
	archive := &countingReader{r: r}
	if err := e.readEntries(ctx, tar.NewReader(archive), archive); err != nil {
		// Queued writes are skipped once extraction has failed.
		e.fail(err)
	}
	close(e.jobs)
	writers.Wait()
	e.streams.Wait()
	if e.failErr != nil {
		return e.failErr
	}
	return e.finish()
}

func (e *tarExtractor) runWriter() {
	for job := range e.jobs {
		select {
		case <-e.failed:
		default:
			err := writeFile(job.target, job.stat, e.asRoot, removeEntry, func(f *os.File) error {
				_, err := f.Write(job.data)
				return errors.WithStack(err)
			})
			if err != nil {
				e.fail(err)
			}
		}
		e.buffer.Release(int64(len(job.data)))
		close(job.done)
	}
}

// countingReader counts the bytes tar.Reader consumes, which shows where the archive ended.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err //nolint:wrapcheck // io.Reader errors such as io.EOF must pass through unwrapped
}

func (c *countingReader) checkEnd() error {
	// tar.Reader reports io.EOF for an empty stream and for one cut off in a member's padding; GNU tar rejects both.
	switch {
	case c.n == 0:
		return errors.New("archive is empty")
	case c.n%blockSize != 0:
		return errors.Errorf("unexpected EOF in archive after %d bytes", c.n)
	}
	return nil
}

func (e *tarExtractor) fail(err error) {
	e.failOnce.Do(func() {
		e.failErr = err
		close(e.failed)
	})
}

func (e *tarExtractor) readEntries(ctx context.Context, tr *tar.Reader, archive *countingReader) error {
	for {
		select {
		case <-e.failed:
			return nil
		default:
		}
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		hdr, err := nextTarEntry(tr)
		if errors.Is(err, io.EOF) {
			return archive.checkEnd()
		}
		if err != nil {
			return errors.Wrap(err, "failed to read archive")
		}
		if err := e.entry(ctx, tr, hdr); err != nil {
			return err
		}
	}
}

func (e *tarExtractor) entry(ctx context.Context, tr *tar.Reader, hdr *tar.Header) error {
	if hdr.Typeflag == tar.TypeXGlobalHeader {
		// As in GNU tar, each global header replaces the previous one, even an empty one.
		e.globalPAX = hdr.PAXRecords
		return checkGlobalPAX(e.globalPAX)
	}
	if err := applyGlobalPAX(hdr, e.globalPAX); err != nil {
		return err
	}
	if containsDotDot(hdr.Name) {
		return errors.Errorf("%s: member name contains '..'", hdr.Name)
	}
	name := memberName(hdr.Name)
	e.dirStack.finishNonAncestors(name)
	switch hdr.Typeflag {
	case gnuTypeVolumeHeader:
		return nil
	case gnuTypeMultiVolume:
		return errors.Errorf("%s: cannot extract a file continued from another volume", hdr.Name)
	}
	target := filepath.Join(e.root, name)
	if e.dirStack.touchesFinished(e.root, target) {
		if err := e.flushFinished(); err != nil {
			return err
		}
	}
	isDir := isDirEntry(hdr)
	if target == e.root {
		if !isDir {
			return errors.Errorf("%s: cannot replace the target directory", hdr.Name)
		}
		return e.recordDir(name, target, hdr, false, nil)
	}
	claimed, hadPlaceholder := e.claim(target, isDir)
	viaLink, err := e.ensureParent(name, target)
	if err != nil {
		return err
	}
	if viaLink {
		// The path may alias one with a queued write or an applied directory, so settle both first.
		if err := e.flushFinished(); err != nil {
			return err
		}
	}
	if err := e.apply(ctx, tr, hdr, name, target, viaLink); err != nil {
		return err
	}
	if hadPlaceholder && e.delayed[claimed].replaced {
		// The inode is free for reuse, so it must not identify a placeholder any more.
		delete(e.placeholders, e.delayed[claimed].id)
	}
	return nil
}

func memberName(name string) string {
	// GNU tar strips trailing slashes and then leading slashes and ".." prefixes; a lone "/" names the root.
	if trimmed := strings.TrimRight(name, "/"); trimmed != "" {
		name = trimmed
	}
	return saferName(name)
}

func (e *tarExtractor) apply(
	ctx context.Context, tr *tar.Reader, hdr *tar.Header, name, target string, viaLink bool,
) error {
	switch {
	case isDirEntry(hdr):
		return e.dir(name, target, hdr, viaLink)
	case hdr.Typeflag == tar.TypeSymlink:
		return e.symlink(name, target, hdr, viaLink)
	case hdr.Typeflag == tar.TypeLink:
		return e.hardlink(name, target, hdr, viaLink)
	case hdr.Typeflag == tar.TypeChar, hdr.Typeflag == tar.TypeBlock, hdr.Typeflag == tar.TypeFifo:
		return e.node(target, hdr, viaLink)
	default:
		// GNU tar extracts regular, contiguous, sparse and unknown entry types as regular files.
		return e.regular(ctx, tr, hdr, target, viaLink)
	}
}

func isDirEntry(hdr *tar.Header) bool {
	switch hdr.Typeflag {
	case tar.TypeDir, gnuTypeDumpDir:
		return true
	case tar.TypeReg, tar.TypeCont:
		return strings.HasSuffix(hdr.Name, "/")
	}
	return false
}

func containsDotDot(name string) bool {
	return slices.Contains(strings.Split(name, "/"), "..")
}

func saferName(name string) string {
	// GNU tar drops everything up to the last ".." component, then any leading slashes.
	start := 0
	for i := 0; i < len(name); {
		if strings.HasPrefix(name[i:], "..") && (i+2 == len(name) || name[i+2] == '/') {
			start = i + 2
		}
		next := strings.IndexByte(name[i:], '/')
		if next < 0 {
			break
		}
		i += next + 1
	}
	name = strings.TrimLeft(name[start:], "/")
	if name == "" {
		return "."
	}
	return name
}

func (e *tarExtractor) claim(target string, isDir bool) (int, bool) {
	e.wait(target)
	i, ok := e.delayedAt[target]
	if ok {
		e.delayed[i].replaced = true
		delete(e.delayedAt, target)
	}
	if !isDir {
		e.forgetDir(target)
	}
	return i, ok
}

func (e *tarExtractor) forgetDir(path string) {
	if _, ok := e.dirs[path]; !ok {
		return
	}
	prefix := path + string(filepath.Separator)
	for dir := range e.dirs {
		if dir == path || strings.HasPrefix(dir, prefix) {
			delete(e.dirs, dir)
		}
	}
}

func (e *tarExtractor) wait(path string) {
	if done, ok := e.inFlight[path]; ok {
		<-done
		delete(e.inFlight, path)
	}
}

func (e *tarExtractor) waitAll() {
	for _, done := range e.inFlight {
		<-done
	}
	clear(e.inFlight)
}

func (e *tarExtractor) pruneInFlight() {
	// Completed writes are dropped in batches so the map stays proportional to the writer queue.
	if len(e.inFlight) < pruneInFlightAt {
		return
	}
	for path, done := range e.inFlight {
		select {
		case <-done:
			delete(e.inFlight, path)
		default:
		}
	}
}

// ensureParent creates target's missing parent directories and reports whether its path goes through a symlink.
//
// Like GNU tar it follows existing symlinks to directories; placeholders for unsafe links are regular files, so
// paths through them fail.
func (e *tarExtractor) ensureParent(name, target string) (bool, error) {
	parent := filepath.Dir(target)
	if _, ok := e.dirs[parent]; ok {
		return false, nil
	}
	viaLink, err := e.ensureParent(name, parent)
	if err != nil {
		return false, err
	}
	e.wait(parent)
	info, err := os.Lstat(parent)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		err = e.mkdirParent(name, parent, viaLink)
	case err != nil:
		err = errors.WithStack(err)
	case info.IsDir():
	case info.Mode()&fs.ModeSymlink != 0:
		viaLink = true
		err = requireDir(parent)
	default:
		err = errors.Errorf("%s: not a directory", parent)
	}
	if err != nil {
		return false, err
	}
	if !viaLink {
		e.dirs[parent] = struct{}{}
	}
	return viaLink, nil
}

func requireDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if !info.IsDir() {
		return errors.Errorf("%s: not a directory", path)
	}
	return nil
}

func (e *tarExtractor) mkdirParent(name, path string, viaLink bool) error {
	// GNU tar gives unlisted parents the umask's mode, owner-writable and searchable until extraction finishes.
	if err := os.Mkdir(path, 0o777); err != nil {
		return errors.WithStack(err)
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return errors.Wrapf(err, "failed to stat %s", path)
	}
	desired := st.Mode & 0o777
	if !e.asRoot && desired&0o300 != 0o300 {
		if err := unix.Chmod(path, st.Mode&0o7777|0o300); err != nil {
			return errors.Wrapf(err, "failed to chmod %s", path)
		}
	}
	return e.dirStack.push(pendingDir{
		name:     e.parentName(name, path),
		path:     path,
		stat:     entryStat{mode: desired},
		interdir: true,
		viaLink:  viaLink,
	})
}

func (e *tarExtractor) parentName(name, path string) string {
	// GNU tar names a created parent by the member name's prefix, which name comparisons later rely on.
	for i := range len(name) {
		if name[i] == '/' && filepath.Join(e.root, name[:i]) == path {
			return name[:i]
		}
	}
	rel, err := filepath.Rel(e.root, path)
	if err != nil {
		return path
	}
	return rel
}

func (e *tarExtractor) dir(name, target string, hdr *tar.Header, viaLink bool) error {
	// Like GNU tar, keep an existing directory but replace anything else, even a symlink to a directory.
	info, err := os.Lstat(target)
	switch {
	case err == nil && info.IsDir():
		return e.recordDir(name, target, hdr, viaLink, nil)
	case err == nil:
		if err := e.removeExisting(target, viaLink); err != nil {
			return err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return errors.WithStack(err)
	}
	created, err := e.mkdir(target, hdr)
	if err != nil {
		return err
	}
	return e.recordDir(name, target, hdr, viaLink, &created)
}

func (e *tarExtractor) mkdir(target string, hdr *tar.Header) (uint32, error) {
	// Owner-only until applyDirs sets the archived mode, so other users cannot use it early.
	mode := uint32(hdr.Mode) & 0o700 //nolint:gosec // mode bits only
	if !e.asRoot {
		mode |= 0o300
	}
	if err := unix.Mkdir(target, mode); err != nil {
		return 0, errors.Wrapf(err, "failed to create directory %s", target)
	}
	// GNU tar clears the umask for -p; this process can't, so undo it where it would affect the owner.
	if e.umask&int(mode) != 0 {
		return mode, errors.Wrapf(unix.Chmod(target, mode), "failed to chmod %s", target)
	}
	return mode, nil
}

func (e *tarExtractor) recordDir(name, target string, hdr *tar.Header, viaLink bool, created *uint32) error {
	stat, err := e.stat(hdr)
	if err != nil {
		return err
	}
	if !viaLink {
		e.dirs[target] = struct{}{}
	}
	dir := pendingDir{name: name, path: target, stat: stat, viaLink: viaLink}
	if created != nil {
		dir.created, dir.wasCreated = *created, true
	}
	return e.dirStack.push(dir)
}

func (e *tarExtractor) removeExisting(path string, viaLink bool) error {
	if info, err := os.Lstat(path); err == nil && info.IsDir() {
		// Queued writes may still be filling the directory, which GNU tar would find non-empty.
		e.waitAll()
		if viaLink {
			// The directory may also be cached under a path without symlinks.
			e.dirs = map[string]struct{}{e.root: {}}
		}
	}
	return removeEntry(path)
}

func removeEntry(path string) error {
	return errors.Wrapf(os.Remove(path), "failed to replace %s", path)
}

func createReplacing(path string, remove func(string) error, create func() error) error {
	// GNU tar's default overwrites an existing entry by removing it and retrying once.
	err := create()
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	if err := remove(path); err != nil {
		return err
	}
	return create()
}

func (e *tarExtractor) regular(
	ctx context.Context, tr *tar.Reader, hdr *tar.Header, target string, viaLink bool,
) error {
	stat, err := e.stat(hdr)
	if err != nil {
		return err
	}
	switch {
	case viaLink:
		// Written before the next entry, since a later path may alias this one.
		remove := func(path string) error { return e.removeExisting(path, viaLink) }
		return writeFile(target, stat, e.asRoot, remove, func(f *os.File) error {
			// Hiding ReadFrom keeps io.CopyBuffer on the large buffer instead of os.File's 32 KiB copy loop.
			_, err := io.CopyBuffer(struct{ io.Writer }{f}, tr, e.copyBuf)
			return errors.Wrapf(err, "failed to write %s", target)
		})
	case hdr.Size > maxBufferedFile:
		return e.stream(ctx, tr, hdr, target, stat)
	}
	if err := e.buffer.Acquire(ctx, hdr.Size); err != nil {
		return errors.WithStack(err)
	}
	data := make([]byte, hdr.Size)
	if _, err := io.ReadFull(tr, data); err != nil {
		e.buffer.Release(hdr.Size)
		return errors.Wrapf(err, "failed to read %s", hdr.Name)
	}
	done := make(chan struct{})
	e.inFlight[target] = done
	e.jobs <- fileWrite{target: target, stat: stat, data: data, done: done}
	e.pruneInFlight()
	return nil
}

func (e *tarExtractor) stream(ctx context.Context, tr *tar.Reader, hdr *tar.Header, target string, stat entryStat) error {
	// A goroutine writes the file while the reader moves on, so decoding continues during large writes.
	chunks := make(chan []byte, 4)
	done := make(chan struct{})
	e.inFlight[target] = done
	e.streams.Go(func() {
		defer close(done)
		err := writeFile(target, stat, e.asRoot, removeEntry, func(f *os.File) error {
			for chunk := range chunks {
				_, err := f.Write(chunk)
				e.releaseChunk(chunk)
				if err != nil {
					return errors.Wrapf(err, "failed to write %s", target)
				}
			}
			return nil
		})
		for chunk := range chunks {
			e.releaseChunk(chunk)
		}
		if err != nil {
			e.fail(err)
		}
	})
	defer close(chunks)
	for remaining := hdr.Size; remaining > 0; {
		n := min(remaining, streamChunkSize)
		if err := e.buffer.Acquire(ctx, n); err != nil {
			return errors.WithStack(err)
		}
		chunk := e.newChunk(n)
		if _, err := io.ReadFull(tr, chunk); err != nil {
			e.releaseChunk(chunk)
			return errors.Wrapf(err, "failed to read %s", hdr.Name)
		}
		chunks <- chunk
		remaining -= n
	}
	return nil
}

func (e *tarExtractor) newChunk(n int64) []byte {
	select {
	case chunk := <-e.chunks:
		return chunk[:n]
	default:
		return make([]byte, n, streamChunkSize)
	}
}

func (e *tarExtractor) releaseChunk(chunk []byte) {
	e.buffer.Release(int64(len(chunk)))
	select {
	case e.chunks <- chunk[:cap(chunk)]:
	default:
	}
}

func writeFile(
	target string, stat entryStat, asRoot bool, remove func(string) error, write func(*os.File) error,
) error {
	var f *os.File
	err := createReplacing(target, remove, func() error {
		var err error
		f, err = os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		return errors.WithStack(err)
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create %s", target)
	}
	err = write(f)
	if err == nil {
		err = setFileStat(f, stat, asRoot)
	}
	return errors.Join(err, errors.Wrapf(f.Close(), "failed to close %s", target))
}

func setFileStat(f *os.File, stat entryStat, asRoot bool) error {
	// Mode goes last, as in GNU tar, because chown can clear set-ID bits.
	if err := setMtime(f.Name(), stat); err != nil {
		return err
	}
	if asRoot {
		if err := f.Chown(stat.uid, stat.gid); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.Wrapf(unix.Fchmod(int(f.Fd()), stat.mode), "failed to chmod %s", f.Name()) //nolint:gosec // fd
}

func (e *tarExtractor) setPathStat(path string, stat entryStat, keepMode bool) error {
	if err := setMtime(path, stat); err != nil {
		return err
	}
	if e.asRoot {
		if err := os.Lchown(path, stat.uid, stat.gid); err != nil {
			return errors.WithStack(err)
		}
	}
	if keepMode {
		return nil
	}
	return errors.Wrapf(unix.Chmod(path, stat.mode), "failed to chmod %s", path)
}

func setMtime(path string, stat entryStat) error {
	// GNU tar leaves atime alone outside incremental mode.
	times := []unix.Timespec{{Nsec: unix.UTIME_OMIT}, stat.mtime}
	err := unix.UtimesNanoAt(unix.AT_FDCWD, path, times, unix.AT_SYMLINK_NOFOLLOW)
	return errors.Wrapf(err, "failed to set times on %s", path)
}

func (e *tarExtractor) stat(hdr *tar.Header) (entryStat, error) {
	mtime, err := unix.TimeToTimespec(hdr.ModTime)
	if err != nil {
		return entryStat{}, errors.Wrapf(err, "%s: unsupported modification time", hdr.Name)
	}
	stat := entryStat{mode: uint32(hdr.Mode) & 0o7777, mtime: mtime} //nolint:gosec // mode bits only
	if e.asRoot {
		stat.uid = cachedID(e.users, hdr.Uname, hdr.Uid, userID)
		stat.gid = cachedID(e.groups, hdr.Gname, hdr.Gid, groupID)
	}
	return stat, nil
}

func cachedID(cache map[string]int, name string, numeric int, lookup func(string) (int, error)) int {
	// GNU tar prefers the archived owner name and falls back to the numeric ID when the name is unknown.
	if name == "" {
		return numeric
	}
	id, ok := cache[name]
	if !ok {
		var err error
		if id, err = lookup(name); err != nil {
			id = -1
		}
		cache[name] = id
	}
	if id < 0 {
		return numeric
	}
	return id
}

func userID(name string) (int, error) {
	if id, ok := nssID("passwd", name); ok {
		return id, nil
	}
	u, err := user.Lookup(name)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	id, err := strconv.Atoi(u.Uid)
	return id, errors.WithStack(err)
}

func groupID(name string) (int, error) {
	if id, ok := nssID("group", name); ok {
		return id, nil
	}
	g, err := user.LookupGroup(name)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	id, err := strconv.Atoi(g.Gid)
	return id, errors.WithStack(err)
}

func nssID(database, name string) (int, bool) {
	// GNU tar resolves names through NSS, as getent does; os/user without cgo only reads /etc/passwd and /etc/group.
	//
	// A leading "-" would be parsed as a getent option.
	if strings.HasPrefix(name, "-") {
		return 0, false
	}
	out, err := exec.Command("getent", database, name).Output() //nolint:gosec,noctx // fixed binary, name is one argument
	if err != nil {
		return 0, false
	}
	line, _, _ := strings.Cut(string(out), "\n")
	fields := strings.Split(line, ":")
	// getent also accepts a numeric ID as the key, which getpwnam doesn't.
	if len(fields) < 3 || fields[0] != name {
		return 0, false
	}
	id, err := strconv.Atoi(fields[2])
	return id, err == nil
}

func isUnsafeLinkTarget(target string) bool {
	// GNU tar defers symlinks whose target is absolute or has a ".." component.
	return strings.HasPrefix(target, "/") || containsDotDot(target)
}

func (e *tarExtractor) symlink(name, target string, hdr *tar.Header, viaLink bool) error {
	stat, err := e.stat(hdr)
	if err != nil {
		return err
	}
	if isUnsafeLinkTarget(hdr.Linkname) {
		return e.placeholder(name, target, delayedLink{symlink: true, target: hdr.Linkname, stat: stat}, viaLink)
	}
	remove := func(path string) error { return e.removeExisting(path, viaLink) }
	err = createReplacing(target, remove, func() error { return errors.WithStack(os.Symlink(hdr.Linkname, target)) })
	if err != nil {
		return errors.Wrapf(err, "failed to create symlink %s", target)
	}
	return e.setPathStat(target, stat, true)
}

// placeholder creates the empty file GNU tar puts where a deferred link goes, and records the link.
func (e *tarExtractor) placeholder(name, target string, link delayedLink, viaLink bool) error {
	for attempt := 0; ; attempt++ {
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0)
		if err == nil {
			info, err := f.Stat()
			closeErr := f.Close()
			if err = errors.Join(err, closeErr); err != nil {
				return errors.Wrapf(err, "failed to create placeholder %s", target)
			}
			link.path = target
			link.id = idOf(info)
			e.delayedAt[target] = len(e.delayed)
			e.delayed = append(e.delayed, link)
			e.placeholders[link.id] = struct{}{}
			return e.dirStack.markLinkParent(name)
		}
		if attempt > 0 || !errors.Is(err, fs.ErrExist) {
			return errors.Wrapf(err, "failed to create placeholder %s", target)
		}
		if e.isPlaceholder(target) {
			// GNU tar keeps the first of several deferred links at the same path.
			e.keepDelayed(target)
			return nil
		}
		if err := e.removeExisting(target, viaLink); err != nil {
			return err
		}
	}
}

func (e *tarExtractor) keepDelayed(path string) {
	// The placeholder stays in place, so undo claim marking it replaced.
	for i := len(e.delayed) - 1; i >= 0; i-- {
		if e.delayed[i].path == path && e.delayed[i].replaced {
			e.delayed[i].replaced = false
			e.delayedAt[path] = i
			return
		}
	}
}

func (e *tarExtractor) isPlaceholder(path string) bool {
	if len(e.placeholders) == 0 {
		return false
	}
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	_, ok := e.placeholders[idOf(info)]
	return ok
}

func idOf(info fs.FileInfo) fileID {
	// os reports syscall.Stat_t, a different type from unix.Stat_t.
	sys, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileID{}
	}
	return fileID{dev: uint64(sys.Dev), ino: sys.Ino} //nolint:unconvert // Dev is uint32 on some architectures
}

func (e *tarExtractor) hardlink(name, target string, hdr *tar.Header, viaLink bool) error {
	source := filepath.Join(e.root, saferName(hdr.Linkname))
	if _, ok := e.dirs[filepath.Dir(source)]; ok {
		e.wait(source)
	} else {
		// The source path may go through a symlink to a file with a queued write.
		e.waitAll()
	}
	if e.isPlaceholder(source) {
		return e.placeholder(name, target, delayedLink{target: source}, viaLink)
	}
	for attempt := 0; ; attempt++ {
		err := os.Link(source, target)
		if err == nil {
			return nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return errors.Wrapf(err, "failed to link %s to %s", target, source)
		}
		if source == target || sameFile(source, target) {
			return nil
		}
		if attempt > 0 {
			return errors.Wrapf(err, "failed to link %s to %s", target, source)
		}
		if err := e.removeExisting(target, viaLink); err != nil {
			return err
		}
	}
}

func sameFile(a, b string) bool {
	ai, err := os.Lstat(a)
	if err != nil {
		return false
	}
	bi, err := os.Lstat(b)
	return err == nil && os.SameFile(ai, bi)
}

func (e *tarExtractor) node(target string, hdr *tar.Header, viaLink bool) error {
	stat, err := e.stat(hdr)
	if err != nil {
		return err
	}
	var kind uint32
	switch hdr.Typeflag {
	case tar.TypeChar:
		kind = unix.S_IFCHR
	case tar.TypeBlock:
		kind = unix.S_IFBLK
	default:
		kind = unix.S_IFIFO
	}
	dev := unix.Mkdev(uint32(hdr.Devmajor), uint32(hdr.Devminor)) //nolint:gosec // device numbers are 32-bit
	remove := func(path string) error { return e.removeExisting(path, viaLink) }
	err = createReplacing(target, remove, func() error {
		return errors.WithStack(unix.Mknod(target, kind|stat.mode&0o777, int(dev))) //nolint:gosec // Linux dev_t
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create %s", target)
	}
	return e.setPathStat(target, stat, false)
}
