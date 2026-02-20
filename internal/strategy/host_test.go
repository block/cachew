package strategy_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func TestHostCaching(t *testing.T) {
	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response"))
	}))
	defer backend.Close()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux)
	assert.NoError(t, err)

	// Request path must include the host prefix from the target URL
	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/test"

	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "response", w1.Body.String())
	assert.Equal(t, 1, callCount)

	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "response", w2.Body.String())
	assert.Equal(t, 1, callCount, "second request should be served from cache")
}

func TestHostNonOKStatus(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer backend.Close()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux)
	assert.NoError(t, err)

	// Request path must include the host prefix from the target URL
	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/missing"

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not found", w.Body.String())

	key := cache.NewKey(backend.URL + "/missing")
	_, _, err = memCache.Open(context.Background(), key)
	assert.Error(t, err, "non-OK responses should not be cached")
}

func TestHostInvalidTargetURL(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: "://invalid"}, memCache, mux)
	assert.Error(t, err)
}

func TestHostString(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	host, err := strategy.NewHost(ctx, strategy.HostConfig{Target: "https://example.com/prefix"}, memCache, mux)
	assert.NoError(t, err)

	assert.Equal(t, "host:example.com/prefix", host.String())
}
