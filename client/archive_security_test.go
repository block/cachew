package client_test

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

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

func compressedTar(t *testing.T, writeEntries func(*tar.Writer)) []byte {
	t.Helper()
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	writeEntries(tw)
	assert.NoError(t, tw.Close())

	cmd := exec.Command("zstd", "-q", "-c")
	cmd.Stdin = &tarBuf
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	assert.NoError(t, cmd.Run(), stderr.String())
	return out.Bytes()
}
