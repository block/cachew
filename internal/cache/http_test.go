package cache_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
)

func TestCachedFetch(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))
	defer backend.Close()

	client := &http.Client{}

	// First request - should hit backend
	req1, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.URL+"/test", nil)
	assert.NoError(t, err)
	resp1, err := cache.Fetch(client, req1, memCache)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, "text/plain", resp1.Header.Get("Content-Type"))
	body1, err := io.ReadAll(resp1.Body)
	assert.NoError(t, err)
	assert.NoError(t, resp1.Body.Close())
	assert.Equal(t, "hello world", string(body1))
	assert.Equal(t, 1, callCount)

	// Second request - should hit cache
	req2, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.URL+"/test", nil)
	assert.NoError(t, err)
	resp2, err := cache.Fetch(client, req2, memCache)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, "text/plain", resp2.Header.Get("Content-Type"))
	body2, err := io.ReadAll(resp2.Body)
	assert.NoError(t, err)
	assert.NoError(t, resp2.Body.Close())
	assert.Equal(t, "hello world", string(body2))
	assert.Equal(t, 1, callCount, "should serve from cache")
}

func TestCachedFetchNonOKStatus(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer backend.Close()

	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, backend.URL+"/missing", nil)
	assert.NoError(t, err)

	resp, err := cache.Fetch(client, req, memCache)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.NoError(t, resp.Body.Close())
	assert.Equal(t, "not found", string(body))
}
