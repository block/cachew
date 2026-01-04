package cache_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
)

func testKey(s string) cache.Key {
	return cache.NewKey(s)
}

func TestDiskStorageExpiry(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: dir})
	assert.NoError(t, err)

	assert.Equal(t, int64(0), disk.Size())

	key1 := testKey("first")
	key2 := testKey("second")

	w1, err := disk.Create(ctx, key1, time.Second*3)
	assert.NoError(t, err)
	_, err = w1.Write([]byte("first file data"))
	assert.NoError(t, err)
	assert.NoError(t, w1.Close())

	w2, err := disk.Create(ctx, key2, time.Second)
	assert.NoError(t, err)
	_, err = w2.Write([]byte("second file data"))
	assert.NoError(t, err)
	assert.NoError(t, w2.Close())

	expectedSize := int64(len("first file data") + len("second file data"))
	assert.Equal(t, expectedSize, disk.Size())

	time.Sleep(time.Second * 2)

	r, err := disk.Open(ctx, key1)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	_, err = disk.Open(ctx, key2)
	assert.IsError(t, err, os.ErrNotExist)

	assert.Equal(t, int64(len("first file data")), disk.Size())
}

func TestDiskAsyncEviction(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	disk, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		EvictInterval: 500 * time.Millisecond,
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, disk.Close()) })

	assert.Equal(t, int64(0), disk.Size(), "initial size should be 0")

	keyExpire1 := testKey("expire1")
	keyExpire2 := testKey("expire2")
	keyKeep1 := testKey("keep1")
	keyKeep2 := testKey("keep2")

	// Create files that will expire
	w1, err := disk.Create(ctx, keyExpire1, 800*time.Millisecond)
	assert.NoError(t, err)
	_, err = w1.Write([]byte("expires soon"))
	assert.NoError(t, err)
	assert.NoError(t, w1.Close())

	w2, err := disk.Create(ctx, keyExpire2, 800*time.Millisecond)
	assert.NoError(t, err)
	_, err = w2.Write([]byte("expires soon"))
	assert.NoError(t, err)
	assert.NoError(t, w2.Close())

	// Create files that won't expire
	w3, err := disk.Create(ctx, keyKeep1, 10*time.Second)
	assert.NoError(t, err)
	_, err = w3.Write([]byte("keep this"))
	assert.NoError(t, err)
	assert.NoError(t, w3.Close())

	w4, err := disk.Create(ctx, keyKeep2, 10*time.Second)
	assert.NoError(t, err)
	_, err = w4.Write([]byte("keep this too"))
	assert.NoError(t, err)
	assert.NoError(t, w4.Close())

	expectedSize := int64(len("expires soon")*2 + len("keep this") + len("keep this too"))
	assert.Equal(t, expectedSize, disk.Size(), "size should match total written data")

	// Wait for expired files to be evicted
	time.Sleep(1500 * time.Millisecond)

	// Check filesystem directly to verify async eviction actually deleted files
	hexExpire1 := keyExpire1.String()
	_, err = os.Stat(filepath.Join(dir, hexExpire1[:2], hexExpire1))
	assert.IsError(t, err, os.ErrNotExist, "expire1 should be deleted from disk")

	hexExpire2 := keyExpire2.String()
	_, err = os.Stat(filepath.Join(dir, hexExpire2[:2], hexExpire2))
	assert.IsError(t, err, os.ErrNotExist, "expire2 should be deleted from disk")

	// Non-expired files should still exist on disk
	hexKeep1 := keyKeep1.String()
	_, err = os.Stat(filepath.Join(dir, hexKeep1[:2], hexKeep1))
	assert.NoError(t, err, "keep1 should still exist on disk")

	hexKeep2 := keyKeep2.String()
	_, err = os.Stat(filepath.Join(dir, hexKeep2[:2], hexKeep2))
	assert.NoError(t, err, "keep2 should still exist on disk")

	// Verify size only includes non-expired files
	expectedSizeAfterEviction := int64(len("keep this") + len("keep this too"))
	assert.Equal(t, expectedSizeAfterEviction, disk.Size(), "size should only include non-expired files")
}
