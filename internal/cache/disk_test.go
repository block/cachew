package cache_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestDiskCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		dir := t.TempDir()
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		c, err := cache.NewDisk(ctx, cache.DiskConfig{
			Root:   dir,
			MaxTTL: 100 * time.Millisecond,
		})
		assert.NoError(t, err)
		return c
	})
}

func TestDiskCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       50,
		MaxTTL:        10 * time.Minute,
		EvictInterval: time.Second,
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
