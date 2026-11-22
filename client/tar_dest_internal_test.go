//go:build linux

package client

import (
	"archive/tar"
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestUnmaskedMkdirKeepsProcessUmask(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.Chmod(dir, fs.ModeSetgid|0o755))
	old := syscall.Umask(0o277)
	t.Cleanup(func() { syscall.Umask(old) })
	dest := &destDir{fd: -1, path: dir, dir: dir}
	assert.NoError(t, dest.open())
	umask, ok := dest.unmaskedThread()
	if !ok {
		assert.NoError(t, dest.close())
		t.Skip("this process can't give a thread its own umask")
	}

	assert.Equal(t, uint32(0o277), umask)
	assert.NoError(t, dest.mkdirUnmasked(filepath.Join(dir, "d"), 0o700))
	assert.NoError(t, dest.close())
	info, err := os.Stat(filepath.Join(dir, "d"))
	assert.NoError(t, err)
	assert.Equal(t, fs.ModeDir|fs.ModeSetgid|0o700, info.Mode())
	assert.Equal(t, 0o277, processUmask())
}

func TestMkdirWithoutUnmaskedThreadKeepsInheritedSetgid(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.Chmod(dir, fs.ModeSetgid|0o755))
	old := syscall.Umask(0o277)
	t.Cleanup(func() { syscall.Umask(old) })
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	assert.NoError(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeDir, Name: "child/", Mode: 0o700}))
	assert.NoError(t, tw.Close())
	e := newTarExtractor(dir, 1)
	// As where unshare is refused, such as under Docker's default seccomp profile.
	e.dest.unmaskedTried = true

	assert.NoError(t, e.extract(t.Context(), &archive))
	info, err := os.Stat(filepath.Join(dir, "child"))
	assert.NoError(t, err)
	assert.Equal(t, fs.ModeDir|fs.ModeSetgid|0o700, info.Mode())
}
