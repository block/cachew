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
	tests := []struct {
		name     string
		readKey1 bool
		evicted  string
		retained []string
	}{
		{name: "EvictsLeastRecentlyWritten", evicted: "key1", retained: []string{"key2", "key3"}},
		{name: "ReadRefreshesRecency", readKey1: true, evicted: "key2", retained: []string{"key1", "key3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

			c, err := cache.NewDisk(ctx, cache.DiskConfig{
				Root:          dir,
				LimitMB:       1,
				MaxTTL:        time.Hour,
				EvictInterval: 50 * time.Millisecond,
			})
			assert.NoError(t, err)
			defer c.Close()

			data := make([]byte, 500*1024)
			write := func(name string) {
				w, err := c.Create(ctx, cache.NewKey(name), nil, time.Hour)
				assert.NoError(t, err)
				_, err = w.Write(data)
				assert.NoError(t, err)
				assert.NoError(t, w.Close())
				time.Sleep(10 * time.Millisecond)
			}

			write("key1")
			write("key2")
			if tt.readKey1 {
				r, _, err := c.Open(ctx, cache.NewKey("key1"))
				assert.NoError(t, err)
				assert.NoError(t, r.Close())
				time.Sleep(10 * time.Millisecond)
			}
			write("key3")

			time.Sleep(200 * time.Millisecond)

			_, _, err = c.Open(ctx, cache.NewKey(tt.evicted))
			assert.Error(t, err)
			for _, name := range tt.retained {
				r, _, err := c.Open(ctx, cache.NewKey(name))
				assert.NoError(t, err)
				assert.NoError(t, r.Close())
			}
		})
	}
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
