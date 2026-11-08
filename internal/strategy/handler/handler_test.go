package handler_test

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
	"github.com/block/cachew/internal/strategy/handler"
)

type testRequest struct {
	url            string
	headers        map[string]string
	expectStatus   int
	expectBody     string
	expectContains string
}

func TestBuilder(t *testing.T) {
	callCounts := make(map[string]int)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCounts[r.URL.Path]++

		switch r.URL.Path {
		case "/simple":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprint(w, "simple response")
		case "/echo-header":
			_, _ = fmt.Fprintf(w, "header: %s", r.Header.Get("X-Custom"))
		case "/conditional":
			if r.Header.Get("X-Private") == "true" {
				_, _ = fmt.Fprint(w, "private")
			} else {
				_, _ = fmt.Fprint(w, "public")
			}
		case "/not-found":
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, "not found")
		case "/stream":
			w.Header().Set("Content-Type", "application/octet-stream")
			for i := range 100 {
				_, _ = fmt.Fprintf(w, "chunk %d\n", i)
			}
		default:
			_, _ = fmt.Fprintf(w, "path: %s", r.URL.Path)
		}
	}))
	defer upstream.Close()

	tests := []struct {
		name                string
		buildHandler        func(cache.Cache) http.Handler
		requests            []testRequest
		expectUpstreamCalls map[string]int
	}{
		{
			name: "BasicFlow",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/simple", nil)
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusOK, expectBody: "simple response"},
			},
			expectUpstreamCalls: map[string]int{"/simple": 1},
		},
		{
			name: "CacheHit",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/simple", nil)
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusOK, expectBody: "simple response"},
				{url: "/test", expectStatus: http.StatusOK, expectBody: "simple response"},
			},
			expectUpstreamCalls: map[string]int{"/simple": 1},
		},
		{
			name: "CustomCacheKey",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					CacheKey(func(_ *http.Request) string {
						return "constant-key"
					}).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/path1", nil)
					})
			},
			requests: []testRequest{
				{url: "/anything1", expectStatus: http.StatusOK, expectBody: "path: /path1"},
				{url: "/anything2", expectStatus: http.StatusOK, expectBody: "path: /path1"},
			},
			expectUpstreamCalls: map[string]int{"/path1": 1},
		},
		{
			name: "Transform",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(r *http.Request) (*http.Request, error) {
						upstreamReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/echo-header", nil)
						if err != nil {
							return nil, err
						}
						upstreamReq.Header.Set("X-Custom", "transformed")
						return upstreamReq, nil
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusOK, expectBody: "header: transformed"},
			},
			expectUpstreamCalls: map[string]int{"/echo-header": 1},
		},
		{
			name: "TransformError",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(_ *http.Request) (*http.Request, error) {
						return nil, httputil.Errorf(http.StatusBadRequest, "transform failed")
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusBadRequest},
			},
			expectUpstreamCalls: map[string]int{},
		},
		{
			name: "ConditionalTransform",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					CacheKey(func(r *http.Request) string {
						return r.URL.String() + ":" + r.Header.Get("X-Private")
					}).
					Transform(func(r *http.Request) (*http.Request, error) {
						upstreamReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/conditional", nil)
						if err != nil {
							return nil, err
						}
						if r.Header.Get("X-Private") == "true" {
							upstreamReq.Header.Set("X-Private", "true")
						}
						return upstreamReq, nil
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusOK, expectBody: "public"},
				{url: "/test", headers: map[string]string{"X-Private": "true"}, expectStatus: http.StatusOK, expectBody: "private"},
			},
			expectUpstreamCalls: map[string]int{"/conditional": 2},
		},
		{
			name: "CustomErrorHandler",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(_ *http.Request) (*http.Request, error) {
						return nil, errors.New("test error")
					}).
					OnError(func(err error, w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusTeapot)
						_, _ = fmt.Fprint(w, "custom error: "+err.Error())
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusTeapot, expectContains: "custom error"},
			},
			expectUpstreamCalls: map[string]int{},
		},
		{
			name: "UpstreamError",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/not-found", nil)
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusNotFound, expectBody: "not found"},
			},
			expectUpstreamCalls: map[string]int{"/not-found": 1},
		},
		{
			name: "CacheKeyWithTransform",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					CacheKey(func(r *http.Request) string {
						return "original:" + r.URL.Path
					}).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/transformed", nil)
					})
			},
			requests: []testRequest{
				{url: "/original", expectStatus: http.StatusOK, expectBody: "path: /transformed"},
				{url: "/original", expectStatus: http.StatusOK, expectBody: "path: /transformed"},
			},
			expectUpstreamCalls: map[string]int{"/transformed": 1},
		},
		{
			name: "StreamingResponse",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/stream", nil)
					})
			},
			requests: []testRequest{
				{url: "/test", expectStatus: http.StatusOK, expectContains: "chunk 0"},
				{url: "/test", expectStatus: http.StatusOK, expectContains: "chunk 99"},
			},
			expectUpstreamCalls: map[string]int{"/stream": 1},
		},
		{
			name: "CustomTTL",
			buildHandler: func(c cache.Cache) http.Handler {
				return handler.New(http.DefaultClient, c, newTestSpools(t)).
					TTL(func(r *http.Request) time.Duration {
						if r.Header.Get("X-Short-Cache") == "true" {
							return 100 * time.Millisecond
						}
						return time.Hour
					}).
					Transform(func(r *http.Request) (*http.Request, error) {
						return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/simple", nil)
					})
			},
			requests: []testRequest{
				{url: "/test", headers: map[string]string{"X-Short-Cache": "true"}, expectStatus: http.StatusOK, expectBody: "simple response"},
			},
			expectUpstreamCalls: map[string]int{"/simple": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for path := range callCounts {
				delete(callCounts, path)
			}

			c := mustNewMemoryCache()
			handler := tt.buildHandler(c)
			ctx := logging.ContextWithLogger(context.Background(), slog.Default())

			for i, req := range tt.requests {
				r := httptest.NewRequest(http.MethodGet, "http://example.com"+req.url, nil)
				r = r.WithContext(ctx)
				for k, v := range req.headers {
					r.Header.Set(k, v)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)

				assert.Equal(t, req.expectStatus, w.Code, "request %d status mismatch", i)
				if req.expectBody != "" {
					assert.Equal(t, req.expectBody, w.Body.String(), "request %d body mismatch", i)
				}
				if req.expectContains != "" {
					assert.True(t, strings.Contains(w.Body.String(), req.expectContains),
						"request %d: expected body to contain %q, got %q", i, req.expectContains, w.Body.String())
				}
			}

			for path, expectedCount := range tt.expectUpstreamCalls {
				assert.Equal(t, expectedCount, callCounts[path], "upstream call count mismatch for %s", path)
			}
		})
	}
}

func TestHeaderForwarding(t *testing.T) {
	var receivedHeaders http.Header
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header.Clone()
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer upstream.Close()

	c := mustNewMemoryCache()
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())

	t.Run("ForwardsOriginalHeaders", func(t *testing.T) {
		h := handler.New(http.DefaultClient, c, newTestSpools(t)).
			CacheKey(func(_ *http.Request) string { return "fwd-test-1" }).
			Transform(func(r *http.Request) (*http.Request, error) {
				return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/test", nil)
			})
		r := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
		r = r.WithContext(ctx)
		r.Header.Set("Accept", "application/json")
		r.Header.Set("X-Custom", "forwarded")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/json", receivedHeaders.Get("Accept"))
		assert.Equal(t, "forwarded", receivedHeaders.Get("X-Custom"))
	})

	t.Run("StripsHopByHopHeaders", func(t *testing.T) {
		h := handler.New(http.DefaultClient, c, newTestSpools(t)).
			CacheKey(func(_ *http.Request) string { return "fwd-test-2" }).
			Transform(func(r *http.Request) (*http.Request, error) {
				return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/test", nil)
			})
		r := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
		r = r.WithContext(ctx)
		r.Header.Set("Connection", "keep-alive")
		r.Header.Set("Keep-Alive", "timeout=5")
		r.Header.Set("Accept", "text/html")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "text/html", receivedHeaders.Get("Accept"))
		assert.Equal(t, "", receivedHeaders.Get("Connection"))
		assert.Equal(t, "", receivedHeaders.Get("Keep-Alive"))
	})

	t.Run("AcceptEncodingVariesCacheKey", func(t *testing.T) {
		callCount := 0
		varyUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callCount++
			w.Header().Set("Vary", "Accept-Encoding")
			_, _ = fmt.Fprintf(w, "call %d ae=%s", callCount, r.Header.Get("Accept-Encoding"))
		}))
		defer varyUpstream.Close()

		varyCache := mustNewMemoryCache()
		h := handler.New(http.DefaultClient, varyCache, newTestSpools(t)).
			Transform(func(r *http.Request) (*http.Request, error) {
				return http.NewRequestWithContext(r.Context(), http.MethodGet, varyUpstream.URL+"/file.zip", nil)
			})

		// Request without Accept-Encoding
		r1 := httptest.NewRequest(http.MethodGet, "http://example.com/file.zip", nil)
		r1 = r1.WithContext(ctx)
		w1 := httptest.NewRecorder()
		h.ServeHTTP(w1, r1)
		assert.Equal(t, http.StatusOK, w1.Code)
		body1 := w1.Body.String()

		// Request with Accept-Encoding: gzip — should be a separate cache entry
		r2 := httptest.NewRequest(http.MethodGet, "http://example.com/file.zip", nil)
		r2 = r2.WithContext(ctx)
		r2.Header.Set("Accept-Encoding", "gzip")
		w2 := httptest.NewRecorder()
		h.ServeHTTP(w2, r2)
		assert.Equal(t, http.StatusOK, w2.Code)
		body2 := w2.Body.String()

		// Both should have hit upstream (different cache keys)
		assert.Equal(t, 2, callCount)
		assert.NotEqual(t, body1, body2)
	})

	t.Run("TransformHeadersTakePrecedence", func(t *testing.T) {
		h := handler.New(http.DefaultClient, c, newTestSpools(t)).
			CacheKey(func(_ *http.Request) string { return "fwd-test-3" }).
			Transform(func(r *http.Request) (*http.Request, error) {
				req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+"/test", nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set("Authorization", "Bearer override")
				return req, nil
			})
		r := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
		r = r.WithContext(ctx)
		r.Header.Set("Authorization", "Bearer original")
		r.Header.Set("X-Custom", "forwarded")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "Bearer override", receivedHeaders.Get("Authorization"))
		assert.Equal(t, "forwarded", receivedHeaders.Get("X-Custom"))
	})
}

func TestHandlerMethodChaining(t *testing.T) {
	c := mustNewMemoryCache()
	client := &http.Client{}

	h := handler.New(client, c, newTestSpools(t))
	result := h.
		CacheKey(func(_ *http.Request) string { return "key" }).
		Transform(func(r *http.Request) (*http.Request, error) { return r, nil }).
		OnError(func(_ error, _ http.ResponseWriter, _ *http.Request) {}).
		TTL(func(_ *http.Request) time.Duration { return time.Hour })

	assert.True(t, h == result, "methods should return the same handler instance")
}

func TestStreamAndCacheAbortsOnUpstreamError(t *testing.T) {
	// Backend that sends partial data then abruptly closes.
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
		}
	}))
	defer backend.Close()

	c := mustNewMemoryCache()
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())

	h := handler.New(http.DefaultClient, c, newTestSpools(t)).
		Transform(func(r *http.Request) (*http.Request, error) {
			return http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL+"/fail", nil)
		})

	r := httptest.NewRequest(http.MethodGet, "http://example.com/test", nil)
	r = r.WithContext(ctx)
	w := httptest.NewRecorder()
	assert.Panics(t, func() { h.ServeHTTP(w, r) })

	// The partial response must not be cached.
	key := cache.NewKey("http://example.com/test")
	_, _, err := c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

type failingWriteCache struct {
	cache.Cache
	createErr error
}

func (c failingWriteCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) (cache.Writer, error) {
	if c.createErr != nil {
		return nil, c.createErr
	}
	w, err := c.Cache.Create(ctx, key, headers, ttl, opts...)
	return &failingWriter{Writer: w}, errors.WithStack(err)
}

type failingWriter struct {
	cache.Writer
	writes int
}

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errors.New("failed to put object: metadata too large")
	}
	return errors.WithStack2(w.Writer.Write(p))
}

func TestCacheWriteFailureDoesNotTruncateResponse(t *testing.T) {
	payload := strings.Repeat("0123456789abcdef", 64<<10)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		_, _ = w.Write([]byte(payload))
	}))
	defer backend.Close()

	for _, tt := range []struct {
		name      string
		createErr error
	}{
		{name: "WriteFails"},
		{name: "CreateFails", createErr: errors.New("headers exceed S3 metadata limit")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := mustNewMemoryCache()
			defer mem.Close()
			ctx := logging.ContextWithLogger(context.Background(), slog.Default())

			h := handler.New(http.DefaultClient, failingWriteCache{Cache: mem, createErr: tt.createErr}, newTestSpools(t)).
				Transform(func(r *http.Request) (*http.Request, error) {
					return http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL, nil)
				})

			r := httptest.NewRequest(http.MethodGet, "http://example.com/big.zip", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, len(payload), w.Body.Len())
			assert.True(t, w.Body.String() == payload, "response body differs from upstream")

			_, _, err := mem.Open(ctx, cache.NewKey("http://example.com/big.zip"))
			assert.IsError(t, err, os.ErrNotExist)
		})
	}
}

func mustNewMemoryCache() cache.Cache {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{
		MaxTTL: time.Hour,
	})
	if err != nil {
		panic(err)
	}
	return c
}

func serve(t *testing.T, h http.Handler, method, url string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	r := httptest.NewRequest(method, url, nil).WithContext(ctx)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func upstreamTransform(url string) func(*http.Request) (*http.Request, error) {
	return func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
	}
}

func TestHeadRequests(t *testing.T) {
	var methods []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.Header().Set("X-Digest", "sha256:abc")
		_, _ = fmt.Fprint(w, "content")
	}))
	defer upstream.Close()
	c := mustNewMemoryCache()
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t)).Transform(upstreamTransform(upstream.URL))

	w := serve(t, h, http.MethodHead, "http://example.com/a", nil)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "sha256:abc", w.Header().Get("X-Digest"))
	assert.Equal(t, "", w.Body.String())

	w = serve(t, h, http.MethodGet, "http://example.com/a", nil)
	assert.Equal(t, "content", w.Body.String())

	w = serve(t, h, http.MethodHead, "http://example.com/a", nil)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "sha256:abc", w.Header().Get("X-Digest"))
	assert.Equal(t, "7", w.Header().Get("Content-Length"))
	assert.Equal(t, "", w.Body.String())

	assert.Equal(t, []string{http.MethodHead, http.MethodGet}, methods)
}

func TestRangeMissCachesFullObject(t *testing.T) {
	const payload = "0123456789"
	tests := []struct {
		name         string
		headers      map[string]string
		expectStatus int
		expectBody   string
		expectRange  string
	}{
		{name: "Partial", headers: map[string]string{"Range": "bytes=2-5"}, expectStatus: http.StatusPartialContent,
			expectBody: "2345", expectRange: "bytes 2-5/10"},
		{name: "Suffix", headers: map[string]string{"Range": "bytes=-3"}, expectStatus: http.StatusPartialContent,
			expectBody: "789", expectRange: "bytes 7-9/10"},
		{name: "NotSatisfiable", headers: map[string]string{"Range": "bytes=20-"},
			expectStatus: http.StatusRequestedRangeNotSatisfiable, expectRange: "bytes */10"},
		{name: "IfRangeMismatch", headers: map[string]string{"Range": "bytes=2-5", "If-Range": `"other"`},
			expectStatus: http.StatusOK, expectBody: payload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var upstreamRanges []string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamRanges = append(upstreamRanges, r.Header.Get("Range"))
				w.Header().Set("ETag", `"upstream"`)
				_, _ = fmt.Fprint(w, payload)
			}))
			defer upstream.Close()
			c := mustNewMemoryCache()
			defer c.Close()
			h := handler.New(http.DefaultClient, c, newTestSpools(t)).Transform(upstreamTransform(upstream.URL))

			w := serve(t, h, http.MethodGet, "http://example.com/obj", tt.headers)
			assert.Equal(t, tt.expectStatus, w.Code)
			assert.Equal(t, tt.expectBody, w.Body.String())
			assert.Equal(t, tt.expectRange, w.Header().Get("Content-Range"))
			assert.Equal(t, []string{""}, upstreamRanges)

			w = serve(t, h, http.MethodGet, "http://example.com/obj", nil)
			assert.Equal(t, payload, w.Body.String())
			assert.Equal(t, 1, len(upstreamRanges))
		})
	}
}

func TestVary(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = fmt.Fprint(w, r.Header.Get("Accept"))
	}))
	defer upstream.Close()
	c := mustNewMemoryCache()
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t)).
		Transform(upstreamTransform(upstream.URL)).
		Vary(func(r *http.Request) map[string]string {
			return map[string]string{"Accept": r.Header.Get("Accept")}
		})

	for range 2 {
		for _, accept := range []string{"a", "b"} {
			w := serve(t, h, http.MethodGet, "http://example.com/v", map[string]string{"Accept": accept})
			assert.Equal(t, accept, w.Body.String())
		}
	}
	assert.Equal(t, 2, calls)
}

type expectedContent struct {
	strings.Builder
	want string
}

func (e *expectedContent) Verify() error {
	if e.String() != e.want {
		return errors.Errorf("got %q, want %q", e.String(), e.want)
	}
	return nil
}

func TestVerify(t *testing.T) {
	tests := []struct {
		name        string
		want        string
		expectCache bool
	}{
		{name: "Match", want: "content", expectCache: true},
		{name: "Mismatch", want: "other", expectCache: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, "content")
			}))
			defer upstream.Close()
			c := mustNewMemoryCache()
			defer c.Close()
			h := handler.New(http.DefaultClient, c, newTestSpools(t)).
				Transform(upstreamTransform(upstream.URL)).
				Verify(func(*http.Request) handler.Verifier { return &expectedContent{want: tt.want} })

			w := serve(t, h, http.MethodGet, "http://example.com/v", nil)
			assert.Equal(t, "content", w.Body.String())

			_, err := c.Stat(context.Background(), cache.NewKey("http://example.com/v"))
			assert.Equal(t, tt.expectCache, err == nil)
		})
	}
}

func TestRevalidate(t *testing.T) {
	tests := []struct {
		name         string
		freshFor     time.Duration
		headStatus   int
		changed      bool
		expectBody   string
		expectHeads  int
		expectGets   int
		expectRetain bool
	}{
		{name: "Fresh", freshFor: time.Hour, headStatus: http.StatusOK, expectBody: "v1", expectGets: 1},
		{name: "Immutable", headStatus: http.StatusOK, changed: true, expectBody: "v1", expectGets: 1},
		{name: "Unchanged", freshFor: time.Nanosecond, headStatus: http.StatusOK, expectBody: "v1", expectHeads: 1,
			expectGets: 1},
		{name: "Changed", freshFor: time.Nanosecond, headStatus: http.StatusOK, changed: true, expectBody: "v2",
			expectHeads: 1, expectGets: 2},
		{name: "UpstreamUnavailable", freshFor: time.Nanosecond, headStatus: http.StatusServiceUnavailable,
			changed: true, expectBody: "v1", expectHeads: 1, expectGets: 1},
		{name: "RateLimited", freshFor: time.Nanosecond, headStatus: http.StatusTooManyRequests, changed: true,
			expectBody: "v1", expectHeads: 1, expectGets: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := "v1"
			heads, gets := 0, 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Digest", version)
				if r.Method == http.MethodHead {
					heads++
					w.WriteHeader(tt.headStatus)
					return
				}
				gets++
				_, _ = fmt.Fprint(w, version)
			}))
			defer upstream.Close()
			c := mustNewMemoryCache()
			defer c.Close()
			h := handler.New(http.DefaultClient, c, newTestSpools(t)).
				Transform(upstreamTransform(upstream.URL)).
				Revalidate("X-Digest", func(*http.Request) time.Duration { return tt.freshFor })

			w := serve(t, h, http.MethodGet, "http://example.com/tag", nil)
			assert.Equal(t, "v1", w.Body.String())
			if tt.changed {
				version = "v2"
			}

			w = serve(t, h, http.MethodGet, "http://example.com/tag", nil)
			assert.Equal(t, tt.expectBody, w.Body.String())
			assert.Equal(t, "", w.Header().Get("X-Cachew-Checked-At"))
			assert.Equal(t, tt.expectHeads, heads)
			assert.Equal(t, tt.expectGets, gets)
		})
	}
}

func TestParallelFetch(t *testing.T) {
	payload := strings.Repeat("0123456789abcdef", 1000)
	tests := []struct {
		name          string
		etag          string
		ignoreRange   bool
		concurrency   int
		expectFetches int
	}{
		{name: "Chunked", etag: `"blob"`, expectFetches: 16},
		{name: "NoETag", expectFetches: 2},
		{name: "RangeIgnored", etag: `"blob"`, ignoreRange: true, expectFetches: 1},
		{name: "SingleStream", etag: `"blob"`, concurrency: 1, expectFetches: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var redirects, fetches atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/blob", func(w http.ResponseWriter, r *http.Request) {
				redirects.Add(1)
				http.Redirect(w, r, "/cdn/blob", http.StatusTemporaryRedirect)
			})
			mux.HandleFunc("/cdn/blob", func(w http.ResponseWriter, r *http.Request) {
				fetches.Add(1)
				if tt.etag != "" {
					w.Header().Set("ETag", tt.etag)
				}
				if tt.ignoreRange {
					r.Header.Del("Range")
				}
				http.ServeContent(w, r, "", time.Time{}, strings.NewReader(payload))
			})
			upstream := httptest.NewServer(mux)
			defer upstream.Close()
			c := mustNewMemoryCache()
			defer c.Close()
			var open atomic.Int32
			client := &http.Client{Transport: countingTransport{open: &open}}
			h := handler.New(client, c, newTestSpools(t)).
				Transform(upstreamTransform(upstream.URL+"/blob")).
				ParallelFetch(1000, cmp.Or(tt.concurrency, 4))

			w := serve(t, h, http.MethodGet, "http://example.com/blob", nil)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, strconv.Itoa(len(payload)), w.Header().Get("Content-Length"))
			assert.True(t, w.Body.String() == payload, "response body differs from upstream")
			assert.Equal(t, int32(1), redirects.Load())
			assert.Equal(t, int32(tt.expectFetches), fetches.Load())
			assert.Equal(t, int32(0), open.Load(), "upstream response bodies left open")

			w = serve(t, h, http.MethodGet, "http://example.com/blob", nil)
			assert.True(t, w.Body.String() == payload, "cached body differs from upstream")
			assert.Equal(t, int32(tt.expectFetches), fetches.Load())
		})
	}
}

// countingTransport tracks upstream response bodies that have not been closed.
type countingTransport struct{ open *atomic.Int32 }

func (t countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	t.open.Add(1)
	resp.Body = &countedBody{ReadCloser: resp.Body, open: t.open}
	return resp, nil
}

type countedBody struct {
	io.ReadCloser
	open *atomic.Int32
	once sync.Once
}

func (b *countedBody) Close() error {
	b.once.Do(func() { b.open.Add(-1) })
	return errors.WithStack(b.ReadCloser.Close())
}

func newTestSpools(t *testing.T) *spool.Registry {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	spools, err := spool.New(ctx, spool.Config{Dir: t.TempDir()}, func(err error) { t.Error(err) })
	assert.NoError(t, err)
	t.Cleanup(spools.Close)
	return spools
}

func TestConcurrentMissesShareOneUpstreamFetch(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, "first ")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "second")
	}))
	defer upstream.Close()
	c := mustNewMemoryCache()
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t)).
		Transform(func(r *http.Request) (*http.Request, error) {
			return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL, nil)
		})
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer server.Close()

	const clients = 5
	responses := make([]*http.Response, clients)
	for i := range clients {
		resp, err := http.Get(server.URL + "/object") //nolint:noctx
		assert.NoError(t, err)
		responses[i] = resp
	}
	close(release)
	for _, resp := range responses {
		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "first second", string(body))
	}
	assert.Equal(t, int32(1), calls.Load())
}

func TestUncoalescableRequestsFetchSeparately(t *testing.T) {
	tests := []struct {
		name   string
		method string
		header http.Header
	}{
		{name: "Head", method: http.MethodHead},
		{name: "Range", method: http.MethodGet, header: http.Header{"Range": {"bytes=0-1"}}},
		{name: "IfNoneMatch", method: http.MethodGet, header: http.Header{"If-None-Match": {`"etag"`}}},
		{name: "IfModifiedSince", method: http.MethodGet, header: http.Header{"If-Modified-Since": {"Mon, 09 Nov 2026 10:00:00 GMT"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrived := make(chan struct{}, 2)
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				arrived <- struct{}{}
				<-release
				_, _ = io.WriteString(w, "body")
			}))
			defer upstream.Close()
			c := mustNewMemoryCache()
			defer c.Close()
			h := handler.New(http.DefaultClient, c, newTestSpools(t)).
				Transform(func(r *http.Request) (*http.Request, error) {
					return http.NewRequestWithContext(r.Context(), r.Method, upstream.URL, nil)
				})
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

			var requests sync.WaitGroup
			defer requests.Wait()
			defer close(release)
			for range 2 {
				requests.Go(func() {
					r := httptest.NewRequestWithContext(ctx, tt.method, "http://example.com/object", nil)
					maps.Copy(r.Header, tt.header)
					h.ServeHTTP(httptest.NewRecorder(), r)
				})
			}
			for range 2 {
				select {
				case <-arrived:
				case <-time.After(5 * time.Second):
					t.Fatal("request was coalesced")
				}
			}
		})
	}
}

func TestCacheResultLabelsServerMetrics(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			close(arrived)
			_, _ = io.WriteString(w, "first ")
			w.(http.Flusher).Flush()
			<-release
		}
		_, _ = io.WriteString(w, "body")
	}))
	defer upstream.Close()
	c := mustNewMemoryCache()
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t)).
		Transform(func(r *http.Request) (*http.Request, error) {
			return http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.URL+r.URL.Path, nil)
		})
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { assert.NoError(t, provider.Shutdown(context.Background())) })
	logger, _ := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	server := httptest.NewServer(otelhttp.NewMiddleware("cachew", otelhttp.WithMeterProvider(provider))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h.ServeHTTP(w, r.WithContext(logging.ContextWithLogger(r.Context(), logger)))
		})))
	defer server.Close()
	open := func(path string, header http.Header) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
		assert.NoError(t, err)
		maps.Copy(req.Header, header)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		return resp
	}
	drain := func(resp *http.Response) {
		t.Helper()
		_, err := io.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
	}

	drain(open("/a", nil))
	drain(open("/a", nil))
	drain(open("/b", http.Header{"Range": {"bytes=0-1"}}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		drain(open("/slow", nil))
	}()
	<-arrived
	joined := open("/slow", nil)
	first := make([]byte, len("first "))
	_, err := io.ReadFull(joined.Body, first)
	assert.NoError(t, err)
	close(release)
	drain(joined)
	<-done

	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &data))
	got := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			assert.True(t, ok)
			for _, point := range histogram.DataPoints {
				result, _ := point.Attributes.Value(handler.CacheResultAttribute)
				got[result.AsString()] += int64(point.Count)
			}
		}
	}
	assert.Equal(t, map[string]int64{handler.CacheHit: 1, handler.CacheMiss: 3, handler.CacheCoalesced: 1}, got)
}

func TestBackoffPausesHandler(t *testing.T) {
	var calls atomic.Int32
	newUpstream := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.URL.Path == "/limited" {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = io.WriteString(w, "body")
		}))
	}
	limited := newUpstream()
	defer limited.Close()
	other := newUpstream()
	defer other.Close()
	client := &http.Client{Transport: httputil.NewBackoffTransport(http.DefaultTransport, time.Minute)}
	c := mustNewMemoryCache()
	defer c.Close()
	newHandler := func() *handler.Handler {
		return handler.New(client, c, newTestSpools(t)).Transform(func(r *http.Request) (*http.Request, error) {
			return http.NewRequestWithContext(r.Context(), http.MethodGet, r.URL.Query().Get("upstream")+r.URL.Path, nil)
		})
	}
	h := newHandler()
	get := func(h http.Handler, upstream, path string) *httptest.ResponseRecorder {
		return serve(t, h, http.MethodGet, "http://example.com"+path+"?upstream="+upstream, nil)
	}

	for _, req := range []struct{ upstream, path string }{
		{limited.URL, "/cached"}, {limited.URL, "/limited"}, {limited.URL, "/cached"}, {limited.URL, "/miss"}, {other.URL, "/miss"},
	} {
		w := get(h, req.upstream, req.path)
		if req.path == "/cached" {
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "body", w.Body.String())
		} else {
			assert.Equal(t, http.StatusTooManyRequests, w.Code)
			assert.Equal(t, "60", w.Header().Get("Retry-After"))
		}
	}
	assert.Equal(t, int32(2), calls.Load())

	w := get(newHandler(), other.URL, "/miss")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "body", w.Body.String())
	assert.Equal(t, int32(3), calls.Load())
}
