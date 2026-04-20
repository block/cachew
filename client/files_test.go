package client_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestHashFilesDeterministic(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	assert.NoError(t, os.WriteFile(a, []byte("alpha"), 0o644))
	assert.NoError(t, os.WriteFile(b, []byte("bravo"), 0o644))

	h1, err := client.HashFiles(a, b)
	assert.NoError(t, err)
	h2, err := client.HashFiles(b, a)
	assert.NoError(t, err)
	assert.Equal(t, h1, h2, "hash should be independent of argument order")

	h3, err := client.HashFiles(filepath.Join(dir, "*"))
	assert.NoError(t, err)
	assert.Equal(t, h1, h3, "glob and explicit patterns should hash identically")
}

func TestHashFilesInvalidation(t *testing.T) {
	baseline := func(t *testing.T) (string, client.Key) {
		t.Helper()
		dir := t.TempDir()
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("v1"), 0o644))
		h, err := client.HashFiles(filepath.Join(dir, "a"))
		assert.NoError(t, err)
		return dir, h
	}

	t.Run("ContentChangeInvalidates", func(t *testing.T) {
		dir, h1 := baseline(t)
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("v2"), 0o644))
		h2, err := client.HashFiles(filepath.Join(dir, "a"))
		assert.NoError(t, err)
		assert.NotEqual(t, h1, h2)
	})

	t.Run("AddedFileInvalidates", func(t *testing.T) {
		dir, h1 := baseline(t)
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "b"), []byte("extra"), 0o644))
		h2, err := client.HashFiles(filepath.Join(dir, "*"))
		assert.NoError(t, err)
		assert.NotEqual(t, h1, h2)
	})

	t.Run("RenameInvalidates", func(t *testing.T) {
		dir := t.TempDir()
		a := filepath.Join(dir, "a")
		renamed := filepath.Join(dir, "a2")
		assert.NoError(t, os.WriteFile(a, []byte("same"), 0o644))
		h1, err := client.HashFiles(a)
		assert.NoError(t, err)
		assert.NoError(t, os.Rename(a, renamed))
		h2, err := client.HashFiles(renamed)
		assert.NoError(t, err)
		assert.NotEqual(t, h1, h2, "identical contents under a different path should hash differently")
	})
}

func TestHashFilesErrors(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
	}{
		{name: "NoPatterns", patterns: nil},
		{name: "NoMatches", patterns: []string{filepath.Join(t.TempDir(), "missing-*")}},
		{name: "DirectoryOnly", patterns: []string{t.TempDir()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.HashFiles(tt.patterns...)
			assert.Error(t, err)
		})
	}
}

func TestHashFilesDoubleStar(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "go.sum"), []byte("root"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "a", "go.sum"), []byte("one"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "a", "b", "go.sum"), []byte("two"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "a", "other.txt"), []byte("ignore"), 0o644))

	h1, err := client.HashFiles(filepath.Join(dir, "**", "go.sum"))
	assert.NoError(t, err)

	h2, err := client.HashFiles(
		filepath.Join(dir, "go.sum"),
		filepath.Join(dir, "a", "go.sum"),
		filepath.Join(dir, "a", "b", "go.sum"),
	)
	assert.NoError(t, err)

	assert.Equal(t, h1, h2, "** should match all nested files matching the suffix")
}

func TestHashFilesSkipsDirectories(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0o644))

	h1, err := client.HashFiles(filepath.Join(dir, "*"))
	assert.NoError(t, err)

	h2, err := client.HashFiles(filepath.Join(dir, "a"))
	assert.NoError(t, err)

	assert.Equal(t, h1, h2, "directories should be skipped, not cause errors")
}

func TestHashKeySaveRestore(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("files")
	defer c.Close()
	ctx := t.Context()

	src := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(src, "go.sum"), []byte("sumv1"), 0o644))
	assert.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "hello.txt"), []byte("world"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "sub", "deep.txt"), []byte("deep"), 0o644))

	key, err := client.HashFiles(filepath.Join(src, "go.sum"))
	assert.NoError(t, err)

	dst := filepath.Join(t.TempDir(), "restore")

	hit, err := c.Restore(ctx, key, dst)
	assert.NoError(t, err)
	assert.False(t, hit, "fresh key should miss")

	assert.NoError(t, c.Save(ctx, key, src, []string{"hello.txt", "sub"}))

	hit, err = c.Restore(ctx, key, dst)
	assert.NoError(t, err)
	assert.True(t, hit, "saved key should hit")

	got, err := os.ReadFile(filepath.Join(dst, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "world", string(got))

	deep, err := os.ReadFile(filepath.Join(dst, "sub", "deep.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "deep", string(deep))
}
