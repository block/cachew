package cache_test

import (
	"log/slog"
	"net/http"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestMemoryCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 3 * time.Second})
		assert.NoError(t, err)
		return c
	})
}

func TestMemoryCapsTTLAtMaxTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
		assert.NoError(t, err)
		created, updated := cache.NewKey("created"), cache.NewKey("updated")
		for _, key := range []cache.Key{created, updated} {
			w, err := c.Create(ctx, key, nil, 2*time.Hour)
			assert.NoError(t, err)
			assert.NoError(t, w.Close())
		}

		time.Sleep(30 * time.Minute)
		assert.NoError(t, c.UpdateMetadata(ctx, updated, http.Header{}, 2*time.Hour))
		time.Sleep(31 * time.Minute)
		_, err = c.Stat(ctx, created)
		assert.IsError(t, err, os.ErrNotExist)
		_, err = c.Stat(ctx, updated)
		assert.NoError(t, err)
		time.Sleep(30 * time.Minute)
		_, err = c.Stat(ctx, updated)
		assert.IsError(t, err, os.ErrNotExist)
	})
}

func TestMemoryCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{
		LimitMB: 50,
		MaxTTL:  10 * time.Minute,
	})
	assert.NoError(t, err)
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      8,
		TTL:              5 * time.Minute,
	})
}
