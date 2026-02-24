package cache_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestDiskEvictionBySize(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	// Create cache with 1MB limit and fast eviction
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer c.Close()

	// Create 3 entries of ~500KB each (total 1.5MB, exceeding 1MB limit)
	data := make([]byte, 500*1024)
	keys := []cache.Key{
		cache.NewKey("key1"),
		cache.NewKey("key2"),
		cache.NewKey("key3"),
	}

	for _, key := range keys {
		w, err := c.Create(ctx, key, nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write(data)
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
		time.Sleep(10 * time.Millisecond) // Ensure different access times
	}

	// Wait for eviction to run
	time.Sleep(200 * time.Millisecond)

	// key1 (oldest) should be evicted
	_, _, err = c.Open(ctx, keys[0])
	assert.Error(t, err)

	// key2 and key3 should still exist
	r2, _, err := c.Open(ctx, keys[1])
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())

	r3, _, err := c.Open(ctx, keys[2])
	assert.NoError(t, err)
	assert.NoError(t, r3.Close())
}

func TestDiskEvictionAcrossNamespaces(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	// Create cache with 1MB limit
	baseCache, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       1,
		MaxTTL:        time.Hour,
		EvictInterval: 50 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer baseCache.Close()

	// Create namespace views
	gitCache := baseCache.Namespace("git")
	gomodCache := baseCache.Namespace("gomod")

	// Create entries in different namespaces
	data := make([]byte, 500*1024)

	// git namespace
	gitKey := cache.NewKey("git-key")
	w, err := gitCache.Create(ctx, gitKey, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	time.Sleep(10 * time.Millisecond)

	// gomod namespace
	gomodKey := cache.NewKey("gomod-key")
	w, err = gomodCache.Create(ctx, gomodKey, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	time.Sleep(10 * time.Millisecond)

	// Another git entry to exceed limit
	gitKey2 := cache.NewKey("git-key2")
	w, err = gitCache.Create(ctx, gitKey2, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Wait for eviction
	time.Sleep(200 * time.Millisecond)

	// First git entry (oldest) should be evicted
	_, _, err = gitCache.Open(ctx, gitKey)
	assert.Error(t, err)

	// gomod entry should still exist
	r, _, err := gomodCache.Open(ctx, gomodKey)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Newer git entry should still exist
	r, _, err = gitCache.Open(ctx, gitKey2)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}
