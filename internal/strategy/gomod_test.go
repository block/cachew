package strategy_test

import (
	"archive/zip"
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

type mockGoModServer struct {
	server       *httptest.Server
	requestCount map[string]int // Track requests by path
	mu           sync.Mutex     // Protects requestCount
	lastPath     string
	responses    map[string]mockResponse
	t            *testing.T
}

type mockResponse struct {
	status  int
	content string
}

func newMockGoModServer(t *testing.T) *mockGoModServer {
	m := &mockGoModServer{
		requestCount: make(map[string]int),
		responses:    make(map[string]mockResponse),
		t:            t,
	}

	// Set up default responses for common endpoints
	m.responses["/@v/list"] = mockResponse{
		status:  http.StatusOK,
		content: "v1.0.0\nv1.0.1\nv1.1.0",
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

func createModuleZip(t *testing.T, modulePath, version string) string {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)

	prefix := modulePath + "@" + version + "/"

	f, err := w.Create(prefix + "go.mod")
	assert.NoError(t, err)
	_, err = f.Write([]byte("module " + modulePath + "\n\ngo 1.21\n"))
	assert.NoError(t, err)

	f2, err := w.Create(prefix + "main.go")
	assert.NoError(t, err)
	_, err = f2.Write([]byte("package main\n\nfunc main() {}\n"))
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	return buf.String()
}

func (m *mockGoModServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	m.mu.Lock()
	m.lastPath = path
	m.requestCount[path]++
	m.mu.Unlock()

	var resp mockResponse
	found := false

	if r, ok := m.responses[path]; ok {
		resp = r
		found = true
	} else {
		for suffix, r := range m.responses {
			if len(path) >= len(suffix) && path[len(path)-len(suffix):] == suffix {
				resp = r
				found = true
				break
			}
		}
	}

	if !found && strings.Contains(path, "/@v/") {
		parts := strings.Split(path, "/@v/")
		if len(parts) == 2 {
			modulePath := strings.TrimPrefix(parts[0], "/")
			versionPart := parts[1]

			switch {
			case strings.HasSuffix(path, ".info"):
				version := strings.TrimSuffix(versionPart, ".info")
				resp = mockResponse{
					status:  http.StatusOK,
					content: `{"Version":"` + version + `","Time":"2023-01-01T00:00:00Z"}`,
				}
				found = true
			case strings.HasSuffix(path, ".mod"):
				resp = mockResponse{
					status:  http.StatusOK,
					content: "module " + modulePath + "\n\ngo 1.21\n",
				}
				found = true
			case strings.HasSuffix(path, ".zip"):
				version := strings.TrimSuffix(versionPart, ".zip")
				resp = mockResponse{
					status:  http.StatusOK,
					content: createModuleZip(m.t, modulePath, version),
				}
				found = true
			}
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

func (m *mockGoModServer) getRequestCount(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requestCount[path]
}

func setupGoModTest(t *testing.T) (*mockGoModServer, *http.ServeMux, context.Context) {
	t.Helper()

	mock := newMockGoModServer(t)
	t.Cleanup(mock.close)

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 24 * time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewGoMod(ctx, strategy.GoModConfig{
		Proxy: mock.server.URL,
	}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux)
	assert.NoError(t, err)

	return mock, mux, ctx
}

func TestGoModList(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/github.com/example/test/@v/list", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := strings.TrimSpace(w.Body.String())
	assert.True(t, strings.Contains(body, "v1.0.0"), "response should contain v1.0.0")
	assert.True(t, strings.Contains(body, "v1.0.1"), "response should contain v1.0.1")
	assert.True(t, strings.Contains(body, "v1.1.0"), "response should contain v1.1.0")
	assert.Equal(t, 1, mock.getRequestCount("/github.com/example/test/@v/list"))
}

func TestGoModInfo(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/github.com/example/test/@v/v1.0.0.info", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `{"Version":"v1.0.0","Time":"2023-01-01T00:00:00Z"}`, w.Body.String())
	assert.Equal(t, 1, mock.getRequestCount("/github.com/example/test/@v/v1.0.0.info"))
}

func TestGoModMod(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/github.com/example/test/@v/v1.0.0.mod", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "module github.com/example/test\n\ngo 1.21\n", w.Body.String())
	assert.Equal(t, 1, mock.getRequestCount("/github.com/example/test/@v/v1.0.0.mod"))
}

func TestGoModZip(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/github.com/example/test/@v/v1.0.0.zip", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, strings.HasPrefix(w.Body.String(), "PK"), "response should be a valid zip file")
	assert.True(t, mock.getRequestCount("/github.com/example/test/@v/v1.0.0.zip") >= 1, "should have fetched zip")
}

func TestGoModLatest(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/github.com/example/test/@latest", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `{"Version":"v1.1.0","Time":"2023-06-01T00:00:00Z"}`, w.Body.String())
	assert.Equal(t, 1, mock.getRequestCount("/github.com/example/test/@latest"))
}

func TestGoModCaching(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/gomod/github.com/example/test/@v/v1.0.0.info"
	upstreamPath := "/github.com/example/test/@v/v1.0.0.info"

	req1 := httptest.NewRequest(http.MethodGet, path, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.getRequestCount(upstreamPath))

	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 1, mock.getRequestCount(upstreamPath), "second request should be served from cache")
}

func TestGoModComplexModulePath(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	req := httptest.NewRequest(http.MethodGet, "/gomod/golang.org/x/tools/@v/v0.1.0.info", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, mock.getRequestCount("/golang.org/x/tools/@v/v0.1.0.info"))
}

func TestGoModNonOKResponse(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	upstreamPath := "/github.com/example/nonexistent/@v/v99.0.0.info"
	notFoundPath := "/gomod" + upstreamPath
	mock.setResponse(upstreamPath, http.StatusNotFound, "not found")

	req1 := httptest.NewRequest(http.MethodGet, notFoundPath, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusNotFound, w1.Code)
	assert.Equal(t, 1, mock.getRequestCount(upstreamPath))

	req2 := httptest.NewRequest(http.MethodGet, notFoundPath, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusNotFound, w2.Code)
	assert.Equal(t, 2, mock.getRequestCount(upstreamPath), "404 responses should not be cached")
}

func TestGoModMultipleConcurrentRequests(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/gomod/github.com/example/test/@v/v1.0.0.zip"
	upstreamPath := "/github.com/example/test/@v/v1.0.0.zip"

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

	for range 3 {
		w := <-results
		assert.Equal(t, http.StatusOK, w.Code)
	}

	assert.True(t, mock.getRequestCount(upstreamPath) >= 1, "at least one request should have been made to upstream")
}

func TestGoModListNotCached(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/gomod/github.com/example/test/@v/list"
	upstreamPath := "/github.com/example/test/@v/list"

	req1 := httptest.NewRequest(http.MethodGet, path, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.getRequestCount(upstreamPath))

	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 2, mock.getRequestCount(upstreamPath), "/@v/list endpoint should not be cached")
}

func TestGoModLatestNotCached(t *testing.T) {
	mock, mux, ctx := setupGoModTest(t)

	path := "/gomod/github.com/example/test/@latest"
	upstreamPath := "/github.com/example/test/@latest"

	req1 := httptest.NewRequest(http.MethodGet, path, nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.getRequestCount(upstreamPath))

	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 2, mock.getRequestCount(upstreamPath), "/@latest endpoint should not be cached")
}
