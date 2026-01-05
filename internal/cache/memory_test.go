package cache_test

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
)

func TestMemoryCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		ctx := t.Context()
		c, err := cache.NewMemoryCache(ctx, cache.MemoryCacheConfig{MaxTTL: 100 * time.Millisecond})
		assert.NoError(t, err)
		return c
	})
}
