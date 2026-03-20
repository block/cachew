package githubapp_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
)

func generateTestKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err)

	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	pemBlock := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}

	path := filepath.Join(t.TempDir(), "test-key.pem")
	f, err := os.Create(path)
	assert.NoError(t, err)
	defer f.Close()

	assert.NoError(t, pem.Encode(f, pemBlock))
	return path
}

func TestNewTokenManagerProvider(t *testing.T) {
	logger := slog.Default()

	t.Run("EmptyConfigs", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider(nil, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.Zero(t, tm)
	})

	t.Run("SkipsEmptyConfigs", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.Zero(t, tm)
	})

	t.Run("ErrorsOnIncompleteConfigs", func(t *testing.T) {
		for _, config := range []githubapp.Config{
			{AppID: "123"},
			{PrivateKeyPath: "/tmp/key.pem"},
		} {
			provider := githubapp.NewTokenManagerProvider([]githubapp.Config{config}, logger)
			_, err := provider()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "incomplete configuration")
		}
	})

	t.Run("SingleApp", func(t *testing.T) {
		keyPath := generateTestKey(t)
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath,
			},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.NotZero(t, tm)
	})

	t.Run("MultipleApps", func(t *testing.T) {
		keyPath1 := generateTestKey(t)
		keyPath2 := generateTestKey(t)
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath1,
			},
			{
				AppID:          "222",
				PrivateKeyPath: keyPath2,
			},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.NotZero(t, tm)
	})
}

func TestGetTokenForOrgDynamicDiscovery(t *testing.T) {
	// Mock GitHub API that returns installation IDs for known orgs
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orgs/squareup/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"id": 12345}) //nolint:errcheck
	})
	mux.HandleFunc("GET /orgs/AfterpayTouch/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{"id": 67890}) //nolint:errcheck
	})
	mux.HandleFunc("GET /orgs/unknown-org/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("GET /users/unknown-org/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("POST /app/installations/12345/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"token": "ghs_squareup_token", "expires_at": "2099-01-01T00:00:00Z"}) //nolint:errcheck
	})
	mux.HandleFunc("POST /app/installations/67890/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"token": "ghs_afterpay_token", "expires_at": "2099-01-01T00:00:00Z"}) //nolint:errcheck
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	keyPath := generateTestKey(t)
	logger := slog.Default()

	tm := githubapp.NewTokenManagerForTest(t, []githubapp.Config{
		{
			AppID:          "111",
			PrivateKeyPath: keyPath,
			FallbackOrg:    "squareup",
		},
	}, logger, server.URL, server.Client())

	ctx := logging.ContextWithLogger(t.Context(), slog.Default())

	t.Run("DiscoverAndCacheInstallation", func(t *testing.T) {
		token, err := tm.GetTokenForOrg(ctx, "squareup")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_squareup_token", token)

		// Second call should use cache
		token, err = tm.GetTokenForOrg(ctx, "squareup")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_squareup_token", token)
	})

	t.Run("DiscoverNewOrg", func(t *testing.T) {
		token, err := tm.GetTokenForOrg(ctx, "AfterpayTouch")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_afterpay_token", token)
	})

	t.Run("FallbackForUnknownOrg", func(t *testing.T) {
		token, err := tm.GetTokenForOrg(ctx, "unknown-org")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_squareup_token", token)
	})
}

func TestGetTokenForOrgNoFallback(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orgs/unknown-org/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("GET /users/unknown-org/installation", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	keyPath := generateTestKey(t)
	logger := slog.Default()

	tm := githubapp.NewTokenManagerForTest(t, []githubapp.Config{
		{
			AppID:          "111",
			PrivateKeyPath: keyPath,
		},
	}, logger, server.URL, server.Client())

	ctx := logging.ContextWithLogger(t.Context(), slog.Default())

	_, err := tm.GetTokenForOrg(ctx, "unknown-org")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no GitHub App installation found for org")
}

func TestGetTokenForOrgNilManager(t *testing.T) {
	var tm *githubapp.TokenManager
	_, err := tm.GetTokenForOrg(t.Context(), "any")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestGetTokenForURLNilManager(t *testing.T) {
	var tm *githubapp.TokenManager
	_, err := tm.GetTokenForURL(t.Context(), "https://github.com/org/repo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}
