package strategy_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

type mockArtifactoryServer struct {
	server          *httptest.Server
	requestCount    int
	lastRequestPath string
	lastHeaders     http.Header
	responseContent string
	responseStatus  int
}

func newMockArtifactoryServer() *mockArtifactoryServer {
	m := &mockArtifactoryServer{
		responseContent: "artifact-content",
		responseStatus:  http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", m.handleRequest)
	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockArtifactoryServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	m.requestCount++
	m.lastRequestPath = r.URL.Path
	m.lastHeaders = r.Header.Clone()

	w.WriteHeader(m.responseStatus)
	_, _ = w.Write([]byte(m.responseContent))
}

func (m *mockArtifactoryServer) close() {
	m.server.Close()
}

func setupArtifactoryTest(t *testing.T, config strategy.ArtifactoryConfig) (*mockArtifactoryServer, *http.ServeMux, context.Context) {
	t.Helper()

	mock := newMockArtifactoryServer()
	t.Cleanup(mock.close)

	// Point config to mock server
	config.Target = mock.server.URL

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: 24 * time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewArtifactory(ctx, jobscheduler.New(ctx, jobscheduler.Config{}), config, memCache, mux)
	assert.NoError(t, err)

	return mock, mux, ctx
}

func TestArtifactoryBasicRequest(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	req := httptest.NewRequest(http.MethodGet, "/"+mock.server.Listener.Addr().String()+"/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("artifact-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)
	assert.Equal(t, "/libs-release/com/example/app/1.0/app-1.0.jar", mock.lastRequestPath)
}

func TestArtifactoryCaching(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	path := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"

	// First request
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("artifact-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)

	// Second request should be served from cache
	req2 := httptest.NewRequest(http.MethodGet, path, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, []byte("artifact-content"), w2.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount, "second request should be served from cache")
}

func TestArtifactoryQueryParamsIncludedInKey(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	basePath := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"

	// First request with query params
	req1 := httptest.NewRequest(http.MethodGet, basePath+"?foo=bar", nil)
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.requestCount)

	// Second request with different query params should NOT hit cache (different cache key)
	req2 := httptest.NewRequest(http.MethodGet, basePath+"?baz=qux", nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 2, mock.requestCount, "different query params should result in different cache keys")
}

func TestArtifactoryXJFrogDownloadRedirectHeader(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	path := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	// Verify X-JFrog-Download-Redirect-To header was set to None
	assert.Equal(t, "None", mock.lastHeaders.Get("X-Jfrog-Download-Redirect-To"))
}

func TestArtifactoryAuthHeaders(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	// Test Authorization header
	path1 := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"
	req := httptest.NewRequest(http.MethodGet, path1, nil)
	req.Header.Set("Authorization", "Bearer test-token-123")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "Bearer test-token-123", mock.lastHeaders.Get("Authorization"))

	// Test X-JFrog-Art-Api header with different path to avoid cache
	path2 := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/2.0/app-2.0.jar"
	req2 := httptest.NewRequest(http.MethodGet, path2, nil)
	req2.Header.Set("X-Jfrog-Art-Api", "api-key-456")
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, "api-key-456", mock.lastHeaders.Get("X-Jfrog-Art-Api"))

	// Test Cookie header with different path to avoid cache
	path3 := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/3.0/app-3.0.jar"
	req3 := httptest.NewRequest(http.MethodGet, path3, nil)
	req3.Header.Set("Cookie", "session=abc123")
	req3 = req3.WithContext(ctx)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)

	assert.Equal(t, http.StatusOK, w3.Code)
	assert.Equal(t, "session=abc123", mock.lastHeaders.Get("Cookie"))
}

func TestArtifactoryNonOKResponse(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{})

	// Configure mock to return 404
	mock.responseStatus = http.StatusNotFound
	mock.responseContent = "Not Found"

	path := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/missing/1.0/missing-1.0.jar"

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, 1, mock.requestCount)
}

func TestArtifactoryString(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	artifactory, err := strategy.NewArtifactory(ctx, jobscheduler.New(ctx, jobscheduler.Config{}), strategy.ArtifactoryConfig{
		Target: "https://ec2.example.jfrog.io",
	}, memCache, mux)
	assert.NoError(t, err)

	assert.Equal(t, "artifactory:ec2.example.jfrog.io", artifactory.String())
}

func TestArtifactoryInvalidTargetURL(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewArtifactory(ctx, jobscheduler.New(ctx, jobscheduler.Config{}), strategy.ArtifactoryConfig{
		Target: "://invalid-url",
	}, memCache, mux)
	assert.Error(t, err)
}

func TestArtifactoryHostBasedRouting(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{
		Hosts: []string{"maven.example.jfrog.io", "npm.example.jfrog.io"},
	})

	// Request using host-based routing
	req := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req.Host = "maven.example.jfrog.io"
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("artifact-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)
	assert.Equal(t, "/libs-release/com/example/app/1.0/app-1.0.jar", mock.lastRequestPath)
}

func TestArtifactoryMultipleHostsSameUpstream(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{
		Hosts: []string{"maven.example.jfrog.io", "npm.example.jfrog.io"},
	})

	// First request via maven host
	req1 := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req1.Host = "maven.example.jfrog.io"
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, mock.requestCount)

	// Second request via npm host for the same artifact - should hit cache
	req2 := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req2.Host = "npm.example.jfrog.io"
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, 1, mock.requestCount, "second request via different host should hit cache")
}

func TestArtifactoryBothRoutingModesSimultaneously(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{
		Hosts: []string{"maven.example.jfrog.io"},
	})

	// First request using host-based routing
	req1 := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req1.Host = "maven.example.jfrog.io"
	req1 = req1.WithContext(ctx)
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, []byte("artifact-content"), w1.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)

	// Second request using path-based routing for same artifact - should hit cache
	pathBasedURL := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"
	req2 := httptest.NewRequest(http.MethodGet, pathBasedURL, nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, []byte("artifact-content"), w2.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount, "path-based request should hit cache from host-based request")

	// Third request using host-based routing again - still should be from cache
	req3 := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req3.Host = "maven.example.jfrog.io"
	req3 = req3.WithContext(ctx)
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)

	assert.Equal(t, http.StatusOK, w3.Code)
	assert.Equal(t, 1, mock.requestCount, "both routing modes should share the same cache")
}

func TestArtifactoryHostBasedWithPort(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{
		Hosts: []string{"maven.example.jfrog.io"},
	})

	// Request with host including port - should match configured host (port is ignored)
	req := httptest.NewRequest(http.MethodGet, "/libs-release/com/example/app/1.0/app-1.0.jar", nil)
	req.Host = "maven.example.jfrog.io:8080"
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("artifact-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)
}

func TestArtifactoryPathBasedOnlyWhenNoHostsConfigured(t *testing.T) {
	mock, mux, ctx := setupArtifactoryTest(t, strategy.ArtifactoryConfig{
		Hosts: []string{}, // Empty hosts - should only use path-based routing
	})

	// Path-based routing should work
	pathBasedURL := "/" + mock.server.Listener.Addr().String() + "/libs-release/com/example/app/1.0/app-1.0.jar"
	req := httptest.NewRequest(http.MethodGet, pathBasedURL, nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("artifact-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.requestCount)
}
