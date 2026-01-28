package cache_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func TestRemoteCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		ctx := t.Context()
		_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
		memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{
			MaxTTL: 100 * time.Millisecond,
		})
		assert.NoError(t, err)
		t.Cleanup(func() { memCache.Close() })

		mux := http.NewServeMux()
		_, err = strategy.NewAPIV1(ctx, struct{}{}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux)
		assert.NoError(t, err)
		ts := httptest.NewServer(mux)
		t.Cleanup(ts.Close)

		client := cache.NewRemote(ts.URL)
		return client
	})
}

func TestRemoteCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	ctx := t.Context()
	_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{
		LimitMB: 50,
		MaxTTL:  10 * time.Minute,
	})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux)
	assert.NoError(t, err)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := cache.NewRemote(ts.URL)
	defer client.Close()

	cachetest.Soak(t, client, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      8,
		TTL:              5 * time.Minute,
	})
}
