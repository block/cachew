package cache_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/logging"
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
