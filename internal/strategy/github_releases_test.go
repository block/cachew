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

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

// httpTransportMutex ensures GitHub release tests don't run in parallel
// since they modify the global http.DefaultTransport
var httpTransportMutex sync.Mutex

type mockGitHubServer struct {
	server            *httptest.Server
	apiCallCount      int
	downloadCallCount int
	publicCallCount   int
}

func newMockGitHubServer() *mockGitHubServer {
	m := &mockGitHubServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/{org}/{repo}/releases/tags/{tag}", m.handleAPIRequest)
	mux.HandleFunc("GET /repos/{org}/{repo}/releases/assets/{assetID}", m.handleAssetDownload)
	mux.HandleFunc("GET /{org}/{repo}/releases/download/{release}/{file}", m.handlePublicDownload)
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
	t.Cleanup(func() { http.DefaultTransport = originalTransport }) //nolint:reassign // Required for testing
	http.DefaultTransport = &testTransport{                         //nolint:reassign // Required for testing
		backend:           mock.server,
		originalTransport: originalTransport,
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewGitHubReleases(ctx, config, memCache, mux)
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
	gh, err := strategy.NewGitHubReleases(ctx, strategy.GitHubReleasesConfig{}, memCache, mux)
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
	}, memCache, mux)
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
