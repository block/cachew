package cache_test

import (
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/logging"
	"github.com/block/sfptc/internal/strategy"
)

func TestRemoteClient(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		ctx := t.Context()
		_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
		memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{
			MaxTTL: 100 * time.Millisecond,
		})
		assert.NoError(t, err)
		t.Cleanup(func() { memCache.Close() })

		server, err := strategy.NewDefault(ctx, strategy.DefaultConfig{}, memCache)
		assert.NoError(t, err)
		ts := httptest.NewServer(server)
		t.Cleanup(ts.Close)

		client := cache.NewRemote(ts.URL)
		return client
	})
}
