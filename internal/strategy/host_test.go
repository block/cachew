package strategy_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
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
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux, newTestSpools(t))
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
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL}, memCache, mux, newTestSpools(t))
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
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: "://invalid"}, memCache, mux, newTestSpools(t))
	assert.Error(t, err)
}

func TestHostHeaders(t *testing.T) {
	var receivedHeaders http.Header
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer backend.Close()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{
		Target:  backend.URL,
		Headers: map[string]string{"Authorization": "Bearer QQ==", "X-Custom": "value"},
	}, memCache, mux, newTestSpools(t))
	assert.NoError(t, err)

	u, _ := url.Parse(backend.URL)
	reqPath := "/" + u.Host + "/test"

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, reqPath, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Bearer QQ==", receivedHeaders.Get("Authorization"))
	assert.Equal(t, "value", receivedHeaders.Get("X-Custom"))
}

func TestHostString(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	host, err := strategy.NewHost(ctx, strategy.HostConfig{Target: "https://example.com/prefix"}, memCache, mux, newTestSpools(t))
	assert.NoError(t, err)

	assert.Equal(t, "host:example.com/prefix", host.String())
}

func TestHostRejectsPathTraversal(t *testing.T) {
	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: backend.URL + "/prefix"}, memCache, mux, newTestSpools(t))
	assert.NoError(t, err)

	u, _ := url.Parse(backend.URL)
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+u.Host+"/prefix/..%2Fsecret", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, callCount)
}

func TestHostFallback(t *testing.T) {
	for _, tt := range []struct {
		name          string
		primaryStatus int
		mirrorStatus  int
		path          string
		wantStatus    int
		wantCalls     int
	}{
		{"rate limit", 429, 200, "/packages/a.jar", 200, 1},
		{"server error", 503, 200, "/packages/a.jar", 200, 1},
		{"transport error", 0, 200, "/packages/a.jar", 200, 1},
		{"authoritative missing", 404, 200, "/packages/a.jar", 404, 0},
		{"forbidden", 403, 200, "/packages/a.jar", 403, 0},
		{"primary success", 200, 200, "/packages/a.jar", 200, 0},
		{"mirror missing", 429, 404, "/packages/a.jar", 404, 1},
		{"exhausted", 503, 502, "/packages/a.jar", 502, 1},
		{"prefix boundary", 429, 200, "/packages-other/a.jar", 429, 0},
		{"escaped path", 429, 200, "/packages/a%20b.jar", 200, 1},
		{"mirror redirect", 429, 302, "/packages/a.jar", 200, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
			c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer c.Close()
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.primaryStatus)
				_, _ = io.WriteString(w, "primary")
			}))
			defer primary.Close()
			if tt.primaryStatus == 0 {
				primary.Close()
			}
			calls := 0
			mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				assert.Equal(t, "custom", r.Header.Get("X-Test"))
				if tt.mirrorStatus == http.StatusFound {
					if r.URL.Path != "/mirror/actual.jar" {
						http.Redirect(w, r, "actual.jar?version=2", http.StatusFound)
						return
					}
					assert.Equal(t, "version=2", r.URL.RawQuery)
					_, _ = io.WriteString(w, "mirror")
					return
				}
				assert.Equal(t, "/mirror"+tt.path[len("/packages"):]+"?version=2", r.URL.RequestURI())
				w.WriteHeader(tt.mirrorStatus)
				_, _ = io.WriteString(w, "mirror")
			}))
			defer mirror.Close()
			mux := http.NewServeMux()
			_, err = strategy.NewHost(ctx, strategy.HostConfig{
				Target:    primary.URL,
				Headers:   map[string]string{"X-Test": "custom"},
				Fallbacks: []strategy.HostFallbackConfig{{Target: mirror.URL + "/mirror", PathPrefix: "/packages"}},
			}, c, mux, newTestSpools(t))
			assert.NoError(t, err)
			u, err := url.Parse(primary.URL)
			assert.NoError(t, err)
			for i := range 2 {
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+u.Host+tt.path+"?version=2", nil))
				assert.Equal(t, tt.wantStatus, w.Code)
				wantBody := "primary"
				if tt.wantCalls > 0 {
					wantBody = "mirror"
				}
				assert.Equal(t, wantBody, w.Body.String())
				wantCalls := tt.wantCalls
				if tt.wantStatus != http.StatusOK {
					wantCalls *= i + 1
				}
				assert.Equal(t, wantCalls, calls)
			}
			reader, _, err := c.Open(ctx, cache.NewKey(primary.URL+tt.path+"?version=2"))
			if tt.wantStatus == http.StatusOK {
				assert.NoError(t, err)
				reader.Close()
			} else {
				assert.True(t, os.IsNotExist(err))
			}
			_, _, err = c.Open(ctx, cache.NewKey(mirror.URL+"/mirror"+tt.path[len("/packages"):]+"?version=2"))
			assert.True(t, os.IsNotExist(err))
		})
	}
}

func TestHostFallbackOrder(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()
	var order []int
	var urls []string
	for i, status := range []int{429, 503, 200} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, i)
			assert.Equal(t, "/a.jar", r.URL.Path)
			w.WriteHeader(status)
			_, _ = io.WriteString(w, strconv.Itoa(i))
		}))
		defer server.Close()
		urls = append(urls, server.URL)
	}
	mux := http.NewServeMux()
	ast, err := hcl.Parse(strings.NewReader(`host "` + urls[0] + `" {
  fallback "` + urls[1] + `" {}
  fallback "` + urls[2] + `" {}
}`))
	assert.NoError(t, err)
	var config strategy.HostConfig
	assert.NoError(t, hcl.UnmarshalBlock(ast.Entries[0].(*hcl.Block), &config))
	_, err = strategy.NewHost(ctx, config, c, mux, newTestSpools(t))
	assert.NoError(t, err)
	u, err := url.Parse(urls[0])
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+u.Host+"/a.jar", nil))
	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "2", w.Body.String())
	assert.Equal(t, []int{0, 1, 2}, order)
}

func TestHostFallbackIncompleteResponse(t *testing.T) {
	for _, cancelRequest := range []bool{false, true} {
		t.Run(strconv.FormatBool(cancelRequest), func(t *testing.T) {
			_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			defer c.Close()
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer primary.Close()
			mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Length", "100")
				_, _ = io.WriteString(w, "partial")
				w.(http.Flusher).Flush()
				if cancelRequest {
					cancel()
				}
			}))
			defer mirror.Close()
			mux := http.NewServeMux()
			_, err = strategy.NewHost(ctx, strategy.HostConfig{Target: primary.URL,
				Fallbacks: []strategy.HostFallbackConfig{{Target: mirror.URL}},
			}, c, mux, newTestSpools(t))
			assert.NoError(t, err)
			u, err := url.Parse(primary.URL)
			assert.NoError(t, err)
			serve := func() {
				mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/"+u.Host+"/a.jar", nil))
			}
			if cancelRequest {
				serve()
			} else {
				assert.Panics(t, serve)
			}
			_, _, err = c.Open(context.Background(), cache.NewKey(primary.URL+"/a.jar"))
			assert.True(t, os.IsNotExist(err))
		})
	}
}

func newTestSpools(t *testing.T) *spool.Registry {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	spools, err := spool.New(ctx, spool.Config{Dir: t.TempDir()})
	assert.NoError(t, err)
	t.Cleanup(spools.Close)
	return spools
}
