package strategy_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

// httpTransportMutexAndroidSDK ensures android-sdk tests don't run in parallel
// since they modify the global http.DefaultTransport
var httpTransportMutexAndroidSDK sync.Mutex //nolint:gochecknoglobals

type mockAndroidSDKTransport struct {
	backend           *httptest.Server
	originalTransport http.RoundTripper
}

func (m *mockAndroidSDKTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Host == "example.com" {
		newReq := req.Clone(req.Context())
		newReq.URL.Scheme = "http"
		newReq.URL.Host = m.backend.Listener.Addr().String()
		return m.originalTransport.RoundTrip(newReq)
	}
	return m.originalTransport.RoundTrip(req)
}

// ttlSpyCache wraps a cache and records the TTL passed to each Create call.
type ttlSpyCache struct {
	cache.Cache
	mu   sync.Mutex
	ttls []time.Duration
}

func (c *ttlSpyCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration) (io.WriteCloser, error) {
	c.mu.Lock()
	c.ttls = append(c.ttls, ttl)
	c.mu.Unlock()
	return c.Cache.Create(ctx, key, headers, ttl)
}

func setupAndroidSDKWithSpy(t *testing.T, feedTTL time.Duration, backend *httptest.Server) (*http.ServeMux, *ttlSpyCache, context.Context) {
	t.Helper()

	httpTransportMutexAndroidSDK.Lock()
	t.Cleanup(httpTransportMutexAndroidSDK.Unlock)

	originalTransport := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = originalTransport })                                          //nolint:reassign
	http.DefaultTransport = &mockAndroidSDKTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	spy := &ttlSpyCache{Cache: memCache}
	mux := http.NewServeMux()
	_, err = strategy.NewAndroidSDK(ctx, strategy.AndroidSDKConfig{FeedTTL: feedTTL}, spy, mux)
	assert.NoError(t, err)

	return mux, spy, ctx
}

func TestAndroidSDKTTLByFileType(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response"))
	}))
	defer backend.Close()

	feedTTL := 30 * time.Minute
	mux, spy, ctx := setupAndroidSDKWithSpy(t, feedTTL, backend)

	tests := []struct {
		name        string
		path        string
		expectedTTL time.Duration
	}{
		{"XML feed gets FeedTTL", "/android-sdk/example.com/repository2-3.xml", feedTTL},
		{"ZIP archive gets long TTL", "/android-sdk/example.com/platform-36.zip", 365 * 24 * time.Hour},
		{"TXT file gets long TTL", "/android-sdk/example.com/checksums.txt", 365 * 24 * time.Hour},
		{"JAR file gets long TTL", "/android-sdk/example.com/some-tool.jar", 365 * 24 * time.Hour},
	}

	for i, tt := range tests {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, tt.path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, tt.name)

		spy.mu.Lock()
		assert.Equal(t, tt.expectedTTL, spy.ttls[i], tt.name)
		spy.mu.Unlock()
	}
}

func TestAndroidSDKCaching(t *testing.T) {
	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached-content"))
	}))
	defer backend.Close()

	mux, _, ctx := setupAndroidSDKWithSpy(t, time.Hour, backend)

	// First request: cache miss
	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, "/android-sdk/example.com/sdk/platform-36_r02.zip", nil)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, callCount)

	// Second request: cache hit, no additional backend call
	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, "/android-sdk/example.com/sdk/platform-36_r02.zip", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, callCount, "should be served from cache")
	assert.Equal(t, "cached-content", w2.Body.String())
}

func TestAndroidSDKURLReconstruction(t *testing.T) {
	var receivedURL string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.RequestURI
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	mux, _, ctx := setupAndroidSDKWithSpy(t, time.Hour, backend)

	tests := []struct {
		name        string
		requestPath string
		expectedURI string
	}{
		{"simple path", "/android-sdk/example.com/path/to/resource.zip", "/path/to/resource.zip"},
		{"with query params", "/android-sdk/example.com/repo.xml?v=2&channel=stable", "/repo.xml?v=2&channel=stable"},
	}

	for _, tt := range tests {
		receivedURL = ""
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, tt.requestPath, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, tt.name)
		assert.Equal(t, tt.expectedURI, receivedURL, tt.name)
	}
}

func TestAndroidSDKMultipleFeedTypes(t *testing.T) {
	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response"))
	}))
	defer backend.Close()

	mux, _, ctx := setupAndroidSDKWithSpy(t, time.Hour, backend)

	paths := []string{
		"/android-sdk/example.com/repository2-3.xml",
		"/android-sdk/example.com/platform-36.zip",
		"/android-sdk/example.com/checksums.txt",
	}

	for i, path := range paths {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, i+1, callCount)

		// Second request should always be cached
		req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, path, nil)
		w2 := httptest.NewRecorder()
		mux.ServeHTTP(w2, req2)
		assert.Equal(t, i+1, callCount, "path %s should be served from cache", path)
	}
}
