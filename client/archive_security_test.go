package client_test

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestArchiveRejectsIncludePathsOutsideBase(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	assert.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))

	var out bytes.Buffer
	assert.Error(t, client.Archive(t.Context(), &out, base, []string{"../secret.txt"}, nil, 0))
	assert.Error(t, client.Archive(t.Context(), &out, base, []string{outside}, nil, 0))
}

func TestExtractRejectsTraversalEntries(t *testing.T) {
	parent := t.TempDir()
	dst := filepath.Join(parent, "dst")
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "../../pwned", Mode: 0o600, Size: int64(len("pwned"))}))
		_, err := tw.Write([]byte("pwned"))
		assert.NoError(t, err)
	})

	err := client.Extract(t.Context(), bytes.NewReader(payload), dst, 0)
	assert.Error(t, err)
	_, statErr := os.Stat(filepath.Join(parent, "pwned"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestExtractRejectsSymlinkTraversal(t *testing.T) {
	parent := t.TempDir()
	dst := filepath.Join(parent, "dst")
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "..", Mode: 0o777}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "link/pwned", Mode: 0o600, Size: int64(len("pwned"))}))
		_, err := tw.Write([]byte("pwned"))
		assert.NoError(t, err)
	})

	err := client.Extract(t.Context(), bytes.NewReader(payload), dst, 0)
	assert.Error(t, err)
	_, statErr := os.Stat(filepath.Join(parent, "pwned"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestExtractAllowsSymlinkWithinTarget(t *testing.T) {
	dst := t.TempDir()
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "target", Mode: 0o600, Size: int64(len("ok"))}))
		_, err := tw.Write([]byte("ok"))
		assert.NoError(t, err)
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "sub", Typeflag: tar.TypeDir, Mode: 0o755}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "sub/link", Typeflag: tar.TypeSymlink, Linkname: "../target", Mode: 0o777}))
	})

	assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(payload), dst, 0))
	target, err := os.Readlink(filepath.Join(dst, "sub", "link"))
	assert.NoError(t, err)
	assert.Equal(t, "../target", target)
}

func TestExtractRejectsHardLinkThroughSymlinkedParent(t *testing.T) {
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	assert.NoError(t, os.Mkdir(outside, 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0o600))
	dst := filepath.Join(parent, "dst")
	assert.NoError(t, os.Mkdir(dst, 0o750))
	assert.NoError(t, os.Symlink(outside, filepath.Join(dst, "escape")))
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "stolen", Typeflag: tar.TypeLink, Linkname: "escape/secret"}))
	})

	err := client.Extract(t.Context(), bytes.NewReader(payload), dst, 0)
	assert.Error(t, err)
	_, statErr := os.Lstat(filepath.Join(dst, "stolen"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestExtractRejectsChainedSymlinkEscape(t *testing.T) {
	for _, linkname := range []string{"s/..", "s/m/../..", "./s/.."} {
		t.Run(linkname, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "dst")
			payload := compressedTar(t, func(tw *tar.Writer) {
				assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "a", Typeflag: tar.TypeDir, Mode: 0o755}))
				assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "a/b", Typeflag: tar.TypeSymlink, Linkname: ".."}))
				assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "s", Typeflag: tar.TypeSymlink, Linkname: "a/b"}))
				assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "u", Typeflag: tar.TypeSymlink, Linkname: linkname}))
			})

			err := client.Extract(t.Context(), bytes.NewReader(payload), dst, 0)
			assert.Error(t, err)
			_, statErr := os.Lstat(filepath.Join(dst, "u"))
			assert.True(t, os.IsNotExist(statErr))
		})
	}
}

func TestExtractRejectsHardLinkThroughArchivedSymlinkChain(t *testing.T) {
	parent := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(parent, "secret"), []byte("secret"), 0o600))
	dst := filepath.Join(parent, "dst")
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "a", Typeflag: tar.TypeDir, Mode: 0o755}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "a/b", Typeflag: tar.TypeSymlink, Linkname: ".."}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "s", Typeflag: tar.TypeSymlink, Linkname: "a/b"}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "u", Typeflag: tar.TypeSymlink, Linkname: "s/.."}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "stolen", Typeflag: tar.TypeLink, Linkname: "u/secret"}))
	})

	err := client.Extract(t.Context(), bytes.NewReader(payload), dst, 0)
	assert.Error(t, err)
	_, statErr := os.Lstat(filepath.Join(dst, "stolen"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestExtractAllowsDanglingAndLoopingSymlinks(t *testing.T) {
	dst := t.TempDir()
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "dangling", Typeflag: tar.TypeSymlink, Linkname: "missing"}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "sub/up", Typeflag: tar.TypeSymlink, Linkname: "./../dangling"}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "loop1", Typeflag: tar.TypeSymlink, Linkname: "loop2"}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "loop2", Typeflag: tar.TypeSymlink, Linkname: "loop1"}))
	})

	assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(payload), dst, 0))
	target, err := os.Readlink(filepath.Join(dst, "dangling"))
	assert.NoError(t, err)
	assert.Equal(t, "missing", target)
}

func TestExtractRestoresModTimes(t *testing.T) {
	dst := t.TempDir()
	modTime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	payload := compressedTar(t, func(tw *tar.Writer) {
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "dir", Typeflag: tar.TypeDir, Mode: 0o555, ModTime: modTime}))
		assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "dir/file", Mode: 0o644, Size: int64(len("ok")), ModTime: modTime}))
		_, err := tw.Write([]byte("ok"))
		assert.NoError(t, err)
	})

	assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(payload), dst, 0))
	t.Cleanup(func() { os.Chmod(filepath.Join(dst, "dir"), 0o755) }) //nolint:errcheck,gosec
	fileInfo, err := os.Stat(filepath.Join(dst, "dir", "file"))
	assert.NoError(t, err)
	assert.True(t, fileInfo.ModTime().Equal(modTime), "file mtime %s", fileInfo.ModTime())
	dirInfo, err := os.Stat(filepath.Join(dst, "dir"))
	assert.NoError(t, err)
	assert.True(t, dirInfo.ModTime().Equal(modTime), "dir mtime %s", dirInfo.ModTime())
}

func TestExtractConsumesTrailingPadding(t *testing.T) {
	dst := t.TempDir()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	assert.NoError(t, tw.WriteHeader(&tar.Header{Name: "file", Mode: 0o600, Size: int64(len("ok"))}))
	_, err := tw.Write([]byte("ok"))
	assert.NoError(t, err)
	assert.NoError(t, tw.Close())
	// Larger than a pipe buffer, so zstd is still writing when the tar reader hits end-of-archive.
	tarBuf.Write(make([]byte, 1<<20))

	assert.NoError(t, client.Extract(t.Context(), bytes.NewReader(zstdCompress(t, tarBuf.Bytes())), dst, 0))
	content, err := os.ReadFile(filepath.Join(dst, "file"))
	assert.NoError(t, err)
	assert.Equal(t, "ok", string(content))
}

func compressedTar(t *testing.T, writeEntries func(*tar.Writer)) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	writeEntries(tw)
	assert.NoError(t, tw.Close())
	return zstdCompress(t, tarBuf.Bytes())
}

func zstdCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	cmd := exec.Command("zstd", "-q", "-c")
	cmd.Stdin = bytes.NewReader(data)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	assert.NoError(t, cmd.Run(), stderr.String())
	return out.Bytes()
}
