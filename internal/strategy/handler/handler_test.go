package handler_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/httputil"
	"github.com/block/sfptc/internal/logging"
	"github.com/block/sfptc/internal/strategy/handler"
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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
				return handler.New(http.DefaultClient, c).
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

func TestHandlerMethodChaining(t *testing.T) {
	c := mustNewMemoryCache()
	client := &http.Client{}

	h := handler.New(client, c)
	result := h.
		CacheKey(func(_ *http.Request) string { return "key" }).
		Transform(func(r *http.Request) (*http.Request, error) { return r, nil }).
		OnError(func(_ error, _ http.ResponseWriter, _ *http.Request) {}).
		TTL(func(_ *http.Request) time.Duration { return time.Hour })

	assert.Equal(t, h, result, "methods should return the same handler instance")
}

func mustNewMemoryCache() cache.Cache {
	c, err := cache.NewMemory(context.Background(), cache.MemoryConfig{
		MaxTTL: time.Hour,
	})
	if err != nil {
		panic(err)
	}
	return c
}
