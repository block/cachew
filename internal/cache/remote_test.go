package cache_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
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

		mux := http.NewServeMux()
		_, err = strategy.NewAPIV1(ctx, jobscheduler.New(ctx, jobscheduler.Config{}), struct{}{}, memCache, mux)
		assert.NoError(t, err)
		ts := httptest.NewServer(mux)
		t.Cleanup(ts.Close)

		client := cache.NewRemote(ts.URL)
		return client
	})
}
