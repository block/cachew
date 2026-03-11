package strategy_test

import (
	"context"
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

// httpTransportMutexProxy prevents concurrent modification of http.DefaultTransport
// across proxy tests, mirroring the same pattern used in hermit_test.go.
var httpTransportMutexProxy sync.Mutex //nolint:gochecknoglobals

// setupProxyTest creates an HTTPProxy and returns the handler that results from
// wrapping a fresh ServeMux via proxy.Intercept. This mirrors how config.Load
// wires up interceptor strategies in production.
func setupProxyTest(t *testing.T) (http.Handler, context.Context, cache.Cache) {
	t.Helper()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	p, err := strategy.NewHTTPProxy(ctx, memCache, mux)
	assert.NoError(t, err)

	// Wrap the mux with the proxy interceptor, just as config.Load does.
	return p.Intercept(mux), ctx, memCache
}

// TestHTTPProxyCaching verifies that a second identical proxy request is served
// from cache without hitting the upstream server.
func TestHTTPProxyCaching(t *testing.T) {
	httpTransportMutexProxy.Lock()
	defer httpTransportMutexProxy.Unlock()

	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("android-repository-content"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	handler, ctx, _ := setupProxyTest(t)

	// Simulate an absolute-form proxy request: GET http://dl.google.com/... HTTP/1.1
	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/android/repository/addons_list-5.xml", nil)
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "android-repository-content", w1.Body.String())
	assert.Equal(t, 1, callCount)

	// Second request — must be a cache hit.
	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/android/repository/addons_list-5.xml", nil)
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "android-repository-content", w2.Body.String())
	assert.Equal(t, 1, callCount, "second request should be served from cache")
}

// TestHTTPProxyNonAbsoluteRequest verifies that requests without an absolute
// http:// URI (i.e. normal relative-path requests to cachew itself) are passed
// through to the next handler (returning 404 from the empty mux in tests).
func TestHTTPProxyNonAbsoluteRequest(t *testing.T) {
	handler, ctx, _ := setupProxyTest(t)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/some/local/path", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestHTTPProxyNonGETNotIntercepted verifies that absolute-form requests with
// methods other than GET are passed through to the next handler, not cached.
func TestHTTPProxyNonGETNotIntercepted(t *testing.T) {
	handler, ctx, _ := setupProxyTest(t)

	// POST with an absolute-form URI should NOT be intercepted by the proxy.
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://dl.google.com/some/path", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Falls through to the empty mux → 404 (not 502 bad gateway from the proxy).
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestHTTPProxyDoesNotShadowSpecificRoutes verifies that more-specific mux
// routes still win for relative-path requests even when the proxy intercepts.
func TestHTTPProxyDoesNotShadowSpecificRoutes(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("healthy"))
	})

	p, err := strategy.NewHTTPProxy(ctx, memCache, mux)
	assert.NoError(t, err)
	handler := p.Intercept(mux)

	// A normal relative-path request to the API route should still be served.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "healthy", w.Body.String())
}

// TestHTTPProxyNonOKStatus verifies that non-200 responses from the upstream
// are proxied back to the client but are NOT stored in the cache.
func TestHTTPProxyNonOKStatus(t *testing.T) {
	httpTransportMutexProxy.Lock()
	defer httpTransportMutexProxy.Unlock()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("resource not found"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	handler, ctx, memCache := setupProxyTest(t)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/android/repository/missing.xml", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "resource not found", w.Body.String())

	// Nothing should have been written to the cache.
	key := cache.NewKey("https://dl.google.com/android/repository/missing.xml")
	_, _, err := memCache.Open(context.Background(), key)
	assert.Error(t, err, "non-OK responses should not be cached")
}

// TestHTTPProxyHTTPSUpgrade verifies that incoming http:// proxy requests are
// fetched from upstream over HTTPS (not HTTP).
func TestHTTPProxyHTTPSUpgrade(t *testing.T) {
	httpTransportMutexProxy.Lock()
	defer httpTransportMutexProxy.Unlock()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	handler, ctx, memCache := setupProxyTest(t)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/android/repository/sdkmanager.jar", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// The cache key should use the HTTPS-upgraded URL, not the original http:// one.
	httpsKey := cache.NewKey("https://dl.google.com/android/repository/sdkmanager.jar")
	cr, _, err := memCache.Open(context.Background(), httpsKey)
	assert.NoError(t, err, "response should be cached under the HTTPS URL key")
	if cr != nil {
		cr.Close()
	}

	// Verify the original http:// key is NOT used for caching.
	httpKey := cache.NewKey("http://dl.google.com/android/repository/sdkmanager.jar")
	_, _, err = memCache.Open(context.Background(), httpKey)
	assert.Error(t, err, "cache key should use HTTPS, not HTTP")
}

// TestHTTPProxyDifferentURLs verifies that requests to distinct URLs are cached
// independently — each unique URL results in exactly one upstream call.
func TestHTTPProxyDifferentURLs(t *testing.T) {
	httpTransportMutexProxy.Lock()
	defer httpTransportMutexProxy.Unlock()

	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("content for " + r.URL.Path))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	handler, ctx, _ := setupProxyTest(t)

	urls := []string{
		"http://dl.google.com/android/repository/addons_list-5.xml",
		"http://dl.google.com/android/repository/repository2-3.xml",
		"http://dl.google.com/android/repository/sys-img/android/sys-img2-1.xml",
	}

	for _, u := range urls {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	}

	assert.Equal(t, len(urls), callCount, "each distinct URL should hit upstream exactly once")

	// Repeat all requests — all should be cache hits now.
	for _, u := range urls {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	}

	assert.Equal(t, len(urls), callCount, "repeated requests should be served from cache")
}

// TestHTTPProxyString verifies the strategy identifier is "proxy".
func TestHTTPProxyString(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	p, err := strategy.NewHTTPProxy(ctx, memCache, mux)
	assert.NoError(t, err)
	assert.Equal(t, "proxy", p.String())
}

// TestHTTPProxyQueryStringInCacheKey verifies that query parameters are included
// in the cache key so requests for different query strings are cached separately.
func TestHTTPProxyQueryStringInCacheKey(t *testing.T) {
	httpTransportMutexProxy.Lock()
	defer httpTransportMutexProxy.Unlock()

	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	handler, ctx, _ := setupProxyTest(t)

	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/path?channel=stable", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req1)

	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://dl.google.com/path?channel=beta", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req2)

	assert.Equal(t, 2, callCount, "different query strings should result in separate upstream fetches")
}
