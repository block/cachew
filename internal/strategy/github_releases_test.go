package strategy_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
	"github.com/block/cachew/internal/strategy"
)

// httpTransportMutex ensures GitHub release tests don't run in parallel
// since they modify the global http.DefaultTransport
var httpTransportMutex sync.Mutex //nolint:gochecknoglobals

type mockGitHubServer struct {
	server            *httptest.Server
	apiCallCount      int
	downloadCallCount int
	publicCallCount   int
}

func newMockGitHubServer() *mockGitHubServer {
	m := &mockGitHubServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/{org}/{repo}/releases/tags/{tag...}", m.handleAPIRequest)
	mux.HandleFunc("GET /repos/{org}/{repo}/releases/assets/{assetID}", m.handleAssetDownload)
	mux.HandleFunc("GET /{org}/{repo}/releases/download/{rest...}", m.handlePublicDownload)
	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockGitHubServer) handleAPIRequest(w http.ResponseWriter, r *http.Request) {
	m.apiCallCount++

	if r.Header.Get("Authorization") != "Bearer test-token" {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("Bad credentials"))
		return
	}

	org := r.PathValue("org")
	repo := r.PathValue("repo")
	tag := r.PathValue("tag")

	if tag == "limited" {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	if strings.Contains(r.URL.Path, "missing.tar.gz") {
		releaseInfo := map[string]any{
			"tag_name": tag,
			"assets": []map[string]string{
				{
					"name": "other-file.tar.gz",
					"url":  "https://api.github.com/repos/" + org + "/" + repo + "/releases/assets/12345",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(releaseInfo)
		return
	}

	releaseInfo := map[string]any{
		"tag_name": tag,
		"assets": []map[string]string{
			{
				"name": "binary.tar.gz",
				"url":  "https://api.github.com/repos/" + org + "/" + repo + "/releases/assets/12345",
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(releaseInfo)
}

func (m *mockGitHubServer) handleAssetDownload(w http.ResponseWriter, _ *http.Request) {
	m.downloadCallCount++
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write([]byte("private-binary-content"))
}

func (m *mockGitHubServer) handlePublicDownload(w http.ResponseWriter, r *http.Request) {
	m.publicCallCount++

	if strings.Contains(r.URL.Path, "missing.tar.gz") {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("Not Found"))
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write([]byte("fake-binary-content"))
}

func (m *mockGitHubServer) close() {
	m.server.Close()
}

func setupTest(t *testing.T, config strategy.GitHubReleasesConfig) (*mockGitHubServer, *http.ServeMux, context.Context) {
	t.Helper()

	// Lock to prevent parallel execution since we modify http.DefaultTransport
	httpTransportMutex.Lock()
	t.Cleanup(httpTransportMutex.Unlock)

	mock := newMockGitHubServer()
	t.Cleanup(mock.close)

	originalTransport := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = originalTransport })                                 //nolint:reassign // Required for testing
	http.DefaultTransport = httputil.NewBackoffTransport(metrics.NewOriginTransport(&testTransport{ //nolint:reassign // Required for testing
		backend:           mock.server,
		originalTransport: originalTransport,
	}), time.Minute)

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewGitHubReleases(ctx, config, memCache, mux, func() (*githubapp.TokenManager, error) { return nil, nil }, newTestSpools(t)) //nolint:nilnil
	assert.NoError(t, err)

	return mock, mux, ctx
}

func TestGitHubReleasesPublicRepo(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{})

	req := httptest.NewRequest(http.MethodGet, "/github.com/publicorg/repo/releases/download/v1.0.0/binary.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("fake-binary-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.publicCallCount)

	req2 := httptest.NewRequest(http.MethodGet, "/github.com/publicorg/repo/releases/download/v1.0.0/binary.tar.gz", nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, []byte("fake-binary-content"), w2.Body.Bytes())
	assert.Equal(t, 1, mock.publicCallCount, "second request should be served from cache")
}

func TestGitHubReleasesSlashedReleaseTag(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{})

	req := httptest.NewRequest(http.MethodGet, "/github.com/kubernetes-sigs/kustomize/releases/download/kustomize/v5.8.1/kustomize_v5.8.1_linux_amd64.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("fake-binary-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.publicCallCount)
}

func TestGitHubReleasesPrivateRepo(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "test-token",
		PrivateOrgs: []string{"privateorg"},
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/privateorg/repo/releases/download/v2.0.0/binary.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, []byte("private-binary-content"), w.Body.Bytes())
	assert.Equal(t, 1, mock.apiCallCount)
	assert.Equal(t, 1, mock.downloadCallCount)

	req2 := httptest.NewRequest(http.MethodGet, "/github.com/privateorg/repo/releases/download/v2.0.0/binary.tar.gz", nil)
	req2 = req2.WithContext(ctx)
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Equal(t, []byte("private-binary-content"), w2.Body.Bytes())
	assert.Equal(t, 1, mock.apiCallCount, "second request should be served from cache")
	assert.Equal(t, 1, mock.downloadCallCount, "second request should be served from cache")
}

func TestGitHubReleasesMetadataIsNotOriginTraffic(t *testing.T) {
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); assert.NoError(t, provider.Shutdown(context.Background())) })
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "test-token",
		PrivateOrgs: []string{"privateorg"},
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/privateorg/repo/releases/download/v2.0.0/binary.tar.gz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req.WithContext(ctx))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, mock.apiCallCount)

	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(ctx, &data))
	var origin int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "cachew.origin.requests" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			assert.True(t, ok)
			for _, point := range sum.DataPoints {
				origin += point.Value
			}
		}
	}
	assert.Equal(t, int64(1), origin)
}

func TestGitHubReleasesPrivateRepoAssetNotFound(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "test-token",
		PrivateOrgs: []string{"privateorg"},
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/privateorg/repo/releases/download/v1.0.0/missing.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, 1, mock.apiCallCount)
	assert.Equal(t, 0, mock.downloadCallCount)
}

func TestGitHubReleasesPrivateRepoAPIError(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "bad-token",
		PrivateOrgs: []string{"privateorg"},
	})

	req := httptest.NewRequest(http.MethodGet, "/github.com/privateorg/repo/releases/download/v1.0.0/binary.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, 1, mock.apiCallCount)
	assert.Equal(t, 0, mock.downloadCallCount)
}

func TestGitHubReleasesRateLimitPausesStrategy(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "test-token",
		PrivateOrgs: []string{"privateorg"},
	})
	for _, path := range []string{"privateorg/repo/releases/download/limited/binary.tar.gz", "publicorg/repo/releases/download/v1.0.0/binary.tar.gz"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/github.com/"+path, nil))
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		assert.Equal(t, "30", w.Header().Get("Retry-After"))
	}
	assert.Equal(t, 1, mock.apiCallCount)
	assert.Equal(t, 0, mock.publicCallCount)
}

func TestGitHubReleasesPublicRepoNotFound(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{})

	req := httptest.NewRequest(http.MethodGet, "/github.com/publicorg/repo/releases/download/v1.0.0/missing.tar.gz", nil)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, 1, mock.publicCallCount)

	key := cache.NewKey("https://github.com/publicorg/repo/releases/download/v1.0.0/missing.tar.gz")
	_, ctx2 := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx2, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	_, _, err = memCache.Open(context.Background(), key)
	assert.Error(t, err, "non-OK responses should not be cached")
}

func TestGitHubReleasesNoToken(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	gh, err := strategy.NewGitHubReleases(ctx, strategy.GitHubReleasesConfig{}, memCache, mux, func() (*githubapp.TokenManager, error) { return nil, nil }, newTestSpools(t)) //nolint:nilnil
	assert.NoError(t, err)
	assert.Equal(t, "github-releases", gh.String())
}

func TestGitHubReleasesString(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	gh, err := strategy.NewGitHubReleases(ctx, strategy.GitHubReleasesConfig{
		Token: "test-token",
	}, memCache, mux, func() (*githubapp.TokenManager, error) { return nil, nil }, newTestSpools(t)) //nolint:nilnil
	assert.NoError(t, err)

	assert.Equal(t, "github-releases", gh.String())
}

type testTransport struct {
	backend           *httptest.Server
	originalTransport http.RoundTripper
}

func (t *testTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	newReq.URL.Scheme = "http"
	newReq.URL.Host = t.backend.Listener.Addr().String()
	newReq.RequestURI = ""
	return t.originalTransport.RoundTrip(newReq)
}

func TestGitHubReleasesRejectsPathTraversal(t *testing.T) {
	mock, mux, ctx := setupTest(t, strategy.GitHubReleasesConfig{
		Token:       "test-token",
		PrivateOrgs: []string{"privateorg"},
	})

	for _, target := range []string{
		"/github.com/privateorg/repo/releases/download/..%2F..%2F..%2Fuser/binary.tar.gz",
		"/github.com/privateorg/%2E%2E/releases/download/v1.0.0/binary.tar.gz",
		"/github.com/privateorg/repo/releases/download/v1.0.0/%2E%2E",
	} {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		assert.Equal(t, http.StatusBadRequest, w.Code, target)
	}
	assert.Equal(t, 0, mock.apiCallCount)
	assert.Equal(t, 0, mock.publicCallCount)
}
