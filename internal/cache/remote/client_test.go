package remote_test

import (
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/cache/remote"
	"github.com/block/sfptc/internal/logging"
)

func TestRemoteClient(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		ctx := t.Context()
		_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
		memCache, err := cache.NewMemoryCache(ctx, cache.MemoryCacheConfig{
			MaxTTL: 100 * time.Millisecond,
		})
		assert.NoError(t, err)
		t.Cleanup(func() { memCache.Close() })

		server := remote.NewServer(ctx, memCache)
		ts := httptest.NewServer(server)
		t.Cleanup(ts.Close)

		client := remote.NewClient(ts.URL)
		return client
	})
}
