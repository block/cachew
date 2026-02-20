package snapshot_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func TestCreateAndRestoreRoundTrip(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "file2.txt"), []byte("content2"), 0o600))
	assert.NoError(t, os.Mkdir(filepath.Join(srcDir, "subdir"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "subdir", "file3.txt"), []byte("content3"), 0o644))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	headers, err := mem.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "application/zstd", headers.Get("Content-Type"))

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	content1, err := os.ReadFile(filepath.Join(dstDir, "file1.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content1", string(content1))

	content2, err := os.ReadFile(filepath.Join(dstDir, "file2.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content2", string(content2))

	content3, err := os.ReadFile(filepath.Join(dstDir, "subdir", "file3.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content3", string(content3))

	info2, err := os.Stat(filepath.Join(dstDir, "file2.txt"))
	assert.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info2.Mode().Perm())
}

func TestCreateWithExcludePatterns(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "include.txt"), []byte("included"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "exclude.log"), []byte("excluded"), 0o644))
	assert.NoError(t, os.Mkdir(filepath.Join(srcDir, "logs"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "logs", "app.log"), []byte("excluded"), 0o644))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, []string{"*.log", "logs"})
	assert.NoError(t, err)

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	_, err = os.Stat(filepath.Join(dstDir, "include.txt"))
	assert.NoError(t, err)

	_, err = os.Stat(filepath.Join(dstDir, "exclude.log"))
	assert.IsError(t, err, os.ErrNotExist)

	_, err = os.Stat(filepath.Join(dstDir, "logs"))
	assert.IsError(t, err, os.ErrNotExist)
}

func TestCreatePreservesSymlinks(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "target.txt"), []byte("target"), 0o644))
	assert.NoError(t, os.Symlink("target.txt", filepath.Join(srcDir, "link.txt")))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	info, err := os.Lstat(filepath.Join(dstDir, "link.txt"))
	assert.NoError(t, err)
	assert.Equal(t, os.ModeSymlink, info.Mode()&os.ModeSymlink)

	target, err := os.Readlink(filepath.Join(dstDir, "link.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "target.txt", target)
}

func TestCreateNonexistentDirectory(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	err = snapshot.Create(ctx, mem, key, "/nonexistent/directory", time.Hour, nil)
	assert.Error(t, err)
}

func TestCreateNotADirectory(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	tmpFile := filepath.Join(t.TempDir(), "file.txt")
	assert.NoError(t, os.WriteFile(tmpFile, []byte("content"), 0o644))

	err = snapshot.Create(ctx, mem, key, tmpFile, time.Hour, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a directory")
}

func TestCreateContextCancellation(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	for i := range 100 {
		content := bytes.Repeat([]byte("data"), 10000)
		filename := filepath.Join(srcDir, fmt.Sprintf("file%d.txt", i))
		assert.NoError(t, os.WriteFile(filename, content, 0o644))
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = snapshot.Create(cancelCtx, mem, key, srcDir, time.Hour, nil)
	assert.Error(t, err)
}

func TestRestoreNonexistentKey(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.Error(t, err)
}

func TestRestoreCreatesTargetDirectory(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("content"), 0o644))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	dstDir := filepath.Join(t.TempDir(), "nested", "target")
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "content", string(content))
}

func TestRestoreContextCancellation(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	for i := range 100 {
		content := bytes.Repeat([]byte("data"), 10000)
		filename := filepath.Join(srcDir, fmt.Sprintf("file%d.txt", i))
		assert.NoError(t, os.WriteFile(filename, content, 0o644))
	}

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	dstDir := t.TempDir()
	err = snapshot.Restore(cancelCtx, mem, key, dstDir)
	assert.Error(t, err)
}

func TestCreateEmptyDirectory(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	entries, err := os.ReadDir(dstDir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(entries))
}

func TestCreateWithNestedDirectories(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	deepPath := filepath.Join(srcDir, "a", "b", "c", "d", "e")
	assert.NoError(t, os.MkdirAll(deepPath, 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(deepPath, "deep.txt"), []byte("deep content"), 0o644))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	dstDir := t.TempDir()
	err = snapshot.Restore(ctx, mem, key, dstDir)
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "a", "b", "c", "d", "e", "deep.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "deep content", string(content))
}

func TestCreateSetsCorrectHeaders(t *testing.T) {
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 100, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	key := cache.Key{1, 2, 3}

	srcDir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("content"), 0o644))

	err = snapshot.Create(ctx, mem, key, srcDir, time.Hour, nil)
	assert.NoError(t, err)

	headers, err := mem.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "application/zstd", headers.Get("Content-Type"))
	assert.Contains(t, headers.Get("Content-Disposition"), "attachment")
	assert.Contains(t, headers.Get("Content-Disposition"), ".tar.zst")
}
