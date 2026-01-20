package strategy_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

type mockGoModServer struct {
	server       *httptest.Server
	requestCount map[string]int // Track requests by path
	lastPath     string
	responses    map[string]mockResponse
}

type mockResponse struct {
	status  int
	content string
}

func newMockGoModServer() *mockGoModServer {
	m := &mockGoModServer{
		requestCount: make(map[string]int),
		responses:    make(map[string]mockResponse),
	}

	// Set up default responses for common endpoints
	m.responses["/@v/list"] = mockResponse{
		status:  http.StatusOK,
		content: "v1.0.0\nv1.0.1\nv1.1.0\n",
	}
	m.responses["/@v/v1.0.0.info"] = mockResponse{
		status:  http.StatusOK,
		content: `{"Version":"v1.0.0","Time":"2023-01-01T00:00:00Z"}`,
	}
	m.responses["/@v/v1.0.0.mod"] = mockResponse{
		status:  http.StatusOK,
		content: "module github.com/example/test\n\ngo 1.21\n",
	}
	m.responses["/@v/v1.0.0.zip"] = mockResponse{
		status:  http.StatusOK,
		content: "PK\x03\x04...", // Mock zip content
	}
	m.responses["/@latest"] = mockResponse{
		status:  http.StatusOK,
		content: `{"Version":"v1.1.0","Time":"2023-06-01T00:00:00Z"}`,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handleRequest)
	m.server = httptest.NewServer(mux)

	return m
}

func (m *mockGoModServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	m.lastPath = path
	m.requestCount[path]++

	// Find matching response
	var resp mockResponse
	found := false

	// Try exact match first
	if r, ok := m.responses[path]; ok {
		resp = r
		found = true
	} else {
		// Try suffix match for module paths
		for suffix, r := range m.responses {
			if len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix {
				resp = r
				found = true
				break
			}
		}
	}

	// If still not found, try pattern matching for any version
	if !found && strings.Contains(path, "/@v/") {
		switch {
		case strings.HasSuffix(path, ".info"):
			resp = mockResponse{
				status:  http.StatusOK,
				content: `{"Version":"v1.0.0","Time":"2023-01-01T00:00:00Z"}`,
			}
			found = true
		case strings.HasSuffix(path, ".mod"):
			resp = mockResponse{
				status:  http.StatusOK,
				content: "module github.com/example/test\n\ngo 1.21\n",
			}
			found = true
		case strings.HasSuffix(path, ".zip"):
			resp = mockResponse{
				status:  http.StatusOK,
				content: "PK\x03\x04...",
			}
			found = true
		}
	}

	if !found {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
		return
	}

	w.WriteHeader(resp.status)
	_, _ = w.Write([]byte(resp.content))
}

func (m *mockGoModServer) close() {
	m.server.Close()
}

func (m *mockGoModServer) setResponse(path string, status int, content string) {
	m.responses[path] = mockResponse{
		status:  status,
		content: content,
	}
}

func setupGoModTest(t *testing.T) (*mockGoModServer, *http.ServeMux, context.Context) {
	t.Helper()

	mock := newMockGoModServer()
	t.Cleanup(mock.close)

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 24 * time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewGoMod(ctx, strategy.GoModConfig{
		Proxy:        mock.server.URL,
		MutableTTL:   "5m",
		ImmutableTTL: "168h",
	}, memCache, mux)
	assert.NoError(t, err)

	return mock, mux, ctx
}

func TestGoModList(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@v/list", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "v1.0.0\nv1.0.1\nv1.1.0\n", w.Body.String())
	assert.Equal(t, 1, mock.requestCount["/github.com/example/test/@v/list"])
}

func TestGoModInfo(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@v/v1.0.0.info", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `{"Version":"v1.0.0","Time":"2023-01-01T00:00:00Z"}`, w.Body.String())
	assert.Equal(t, 1, mock.requestCount["/github.com/example/test/@v/v1.0.0.info"])
}

func TestGoModMod(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@v/v1.0.0.mod", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "module github.com/example/test\n\ngo 1.21\n", w.Body.String())
	assert.Equal(t, 1, mock.requestCount["/github.com/example/test/@v/v1.0.0.mod"])
}

func TestGoModZip(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@v/v1.0.0.zip", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "PK\x03\x04...", w.Body.String())
	assert.Equal(t, 1, mock.requestCount["/github.com/example/test/@v/v1.0.0.zip"])
}

func TestGoModLatest(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@latest", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `{"Version":"v1.1.0","Time":"2023-06-01T00:00:00Z"}`, w.Body.String())
	assert.Equal(t, 1, mock.requestCount["/github.com/example/test/@latest"])
}

func TestGoModCaching(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/github.com/example/test/@v/v1.0.0.info"

	// First request
	req1 := httptest.NewRequest(http.MethodGet, path, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.requestCount[path])

	// Second request should hit cache
	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 1, mock.requestCount[path], "second request should be served from cache")
}

func TestGoModComplexModulePath(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	// Test module path with multiple slashes
	req := httptest.NewRequest(http.MethodGet, "/golang.org/x/tools/@v/v0.1.0.info", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, mock.requestCount["/golang.org/x/tools/@v/v0.1.0.info"])
}

func TestGoModNonOKResponse(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	// Set up 404 response
	notFoundPath := "/github.com/example/nonexistent/@v/v99.0.0.info"
	mock.setResponse(notFoundPath, http.StatusNotFound, "not found")

	// First request should return 404
	req1 := httptest.NewRequest(http.MethodGet, notFoundPath, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusNotFound, w1.Code)
	assert.Equal(t, 1, mock.requestCount[notFoundPath])

	// Second request should also hit upstream (404s are not cached)
	req2 := httptest.NewRequest(http.MethodGet, notFoundPath, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusNotFound, w2.Code)
	assert.Equal(t, 2, mock.requestCount[notFoundPath], "404 responses should not be cached")
}

func TestGoModMultipleConcurrentRequests(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/github.com/example/test/@v/v1.0.0.zip"

	// Make multiple concurrent requests
	results := make(chan *httptest.ResponseRecorder, 3)
	for range 3 {
		go func() {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req = req.WithContext(ctx)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			results <- w
		}()
	}

	// Collect results
	for range 3 {
		w := <-results
		assert.Equal(t, http.StatusOK, w.Code)
	}

	// First request should have created the cache entry
	// Subsequent requests might hit cache or might be in-flight
	// We just verify all requests succeeded
	assert.True(t, mock.requestCount[path] >= 1, "at least one request should have been made to upstream")
}

func TestGoModDefaultProxy(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 24 * time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()

	// Create strategy with default proxy
	gomod, err := strategy.NewGoMod(ctx, strategy.GoModConfig{
		Proxy:        "https://proxy.golang.org",
		MutableTTL:   "5m",
		ImmutableTTL: "168h",
	}, memCache, mux)
	assert.NoError(t, err)
	assert.Equal(t, "gomod:proxy.golang.org", gomod.String())
}

func TestGoModCustomProxy(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 24 * time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()

	// Create strategy with custom proxy
	gomod, err := strategy.NewGoMod(ctx, strategy.GoModConfig{
		Proxy: "https://goproxy.example.com",
	}, memCache, mux)
	assert.NoError(t, err)
	assert.Equal(t, "gomod:goproxy.example.com", gomod.String())
}

func TestGoModAuthorizationHeader(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	// Create a custom handler to check if Authorization header is passed through
	authReceived := ""
	originalHandler := mock.server.Config.Handler
	mock.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authReceived = r.Header.Get("Authorization")
		originalHandler.ServeHTTP(w, r)
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/example/test/@v/list", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Bearer test-token", authReceived, "Authorization header should be passed to upstream")
}
