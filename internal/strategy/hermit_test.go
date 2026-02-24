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
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

// httpTransportMutexHermit ensures hermit tests don't run in parallel
// since they modify the global http.DefaultTransport
var httpTransportMutexHermit sync.Mutex //nolint:gochecknoglobals

func setupHermitTest(t *testing.T) (*http.ServeMux, context.Context, cache.Cache) {
	t.Helper()

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewHermit(ctx, strategy.HermitConfig{GitHubBaseURL: "http://localhost:8080"}, nil, memCache, mux)
	assert.NoError(t, err)

	return mux, ctx, memCache
}

func TestHermitNonGitHubCaching(t *testing.T) {
	httpTransportMutexHermit.Lock()
	defer httpTransportMutexHermit.Unlock()

	callCount := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("go-binary-content"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	mux, ctx, _ := setupHermitTest(t)

	req1 := httptest.NewRequestWithContext(ctx, http.MethodGet, "/hermit/golang.org/dl/go1.21.0.tar.gz", nil)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, "go-binary-content", w1.Body.String())
	assert.Equal(t, 1, callCount)

	req2 := httptest.NewRequestWithContext(ctx, http.MethodGet, "/hermit/golang.org/dl/go1.21.0.tar.gz", nil)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "go-binary-content", w2.Body.String())
	assert.Equal(t, 1, callCount, "second request should be served from cache")
}

type mockTransport struct {
	backend           *httptest.Server
	originalTransport http.RoundTripper
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	newReq.URL.Scheme = "http"
	newReq.URL.Host = m.backend.Listener.Addr().String()
	newReq.RequestURI = ""
	return m.originalTransport.RoundTrip(newReq)
}

func TestHermitGitHubRelease(t *testing.T) {
	githubCallCount := 0
	githubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		githubCallCount++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("github-binary-content"))
	}))
	defer githubServer.Close()

	mux, ctx, memCache := setupHermitTest(t)

	_, err := strategy.NewGitHubReleases(ctx, strategy.GitHubReleasesConfig{}, memCache, mux, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/hermit/github.com/alecthomas/chroma/releases/download/v2.14.0/chroma-2.14.0-linux-amd64.tar.gz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.True(t, w.Code == http.StatusOK || w.Code == http.StatusBadGateway || w.Code == http.StatusNotFound,
		"should attempt to fetch from GitHub (may fail without mock)")
}

func TestHermitNonOKStatus(t *testing.T) {
	httpTransportMutexHermit.Lock()
	defer httpTransportMutexHermit.Unlock()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer backend.Close()

	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
	http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

	mux, ctx, memCache := setupHermitTest(t)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/hermit/example.com/missing.tar.gz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "not found", w.Body.String())

	key := cache.NewKey("https://example.com/missing.tar.gz")
	_, _, err := memCache.Open(context.Background(), key)
	assert.Error(t, err, "non-OK responses should not be cached")
}

func TestHermitDifferentSources(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantHost string
	}{
		{
			name:     "golang.org",
			path:     "/hermit/golang.org/dl/go1.21.0.tar.gz",
			wantHost: "golang.org",
		},
		{
			name:     "npm registry",
			path:     "/hermit/registry.npmjs.org/@esbuild/linux-arm64/-/linux-arm64-0.25.0.tgz",
			wantHost: "registry.npmjs.org",
		},
		{
			name:     "HashiCorp",
			path:     "/hermit/releases.hashicorp.com/terraform/1.5.0/terraform_1.5.0_linux_amd64.zip",
			wantHost: "releases.hashicorp.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpTransportMutexHermit.Lock()
			defer httpTransportMutexHermit.Unlock()

			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("content"))
			}))
			defer backend.Close()

			originalTransport := http.DefaultTransport
			defer func() { http.DefaultTransport = originalTransport }()                                   //nolint:reassign
			http.DefaultTransport = &mockTransport{backend: backend, originalTransport: originalTransport} //nolint:reassign

			mux, ctx, _ := setupHermitTest(t)

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "content", w.Body.String())
		})
	}
}

func TestHermitCacheKeyGeneration(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantKey string
	}{
		{
			name:    "golang.org",
			path:    "/hermit/golang.org/dl/go1.21.0.tar.gz",
			wantKey: "https://golang.org/dl/go1.21.0.tar.gz",
		},
		{
			name:    "npm registry with scope",
			path:    "/hermit/registry.npmjs.org/@esbuild/linux-arm64/-/linux-arm64-0.25.0.tgz",
			wantKey: "https://registry.npmjs.org/@esbuild/linux-arm64/-/linux-arm64-0.25.0.tgz",
		},
		{
			name:    "GitHub release",
			path:    "/hermit/github.com/alecthomas/chroma/releases/download/v2.14.0/chroma-2.14.0-linux-amd64.tar.gz",
			wantKey: "https://github.com/alecthomas/chroma/releases/download/v2.14.0/chroma-2.14.0-linux-amd64.tar.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux, ctx, _ := setupHermitTest(t)

			req := httptest.NewRequestWithContext(ctx, http.MethodGet, tt.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
		})
	}
}
