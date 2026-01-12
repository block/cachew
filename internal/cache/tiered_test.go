package cache_test

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/logging"
)

func TestTiered(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{})
		memory, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		return cache.MaybeNewTiered(ctx, []cache.Cache{memory, disk})
	})
}
