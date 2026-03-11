package githubapp_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
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
			{AppID: "123", Installations: map[string]string{"org": "inst"}},
			{PrivateKeyPath: "/tmp/key.pem", Installations: map[string]string{"org": "inst"}},
			{AppID: "123", PrivateKeyPath: "/tmp/key.pem"},
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
				Installations:  map[string]string{"orgA": "inst-a", "orgB": "inst-b"},
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
				Installations:  map[string]string{"orgA": "inst-a"},
			},
			{
				AppID:          "222",
				PrivateKeyPath: keyPath2,
				Installations:  map[string]string{"orgB": "inst-b"},
			},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.NotZero(t, tm)
	})

	t.Run("DuplicateOrgAcrossApps", func(t *testing.T) {
		keyPath1 := generateTestKey(t)
		keyPath2 := generateTestKey(t)
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath1,
				Installations:  map[string]string{"orgA": "inst-a"},
			},
			{
				AppID:          "222",
				PrivateKeyPath: keyPath2,
				Installations:  map[string]string{"orgA": "inst-a2"},
			},
		}, logger)
		_, err := provider()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "org \"orgA\" is configured in both")
	})
}

func TestGetTokenForOrgRouting(t *testing.T) {
	keyPath1 := generateTestKey(t)
	keyPath2 := generateTestKey(t)
	logger := slog.Default()

	provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
		{
			AppID:          "111",
			PrivateKeyPath: keyPath1,
			Installations:  map[string]string{"orgA": "inst-a"},
		},
		{
			AppID:          "222",
			PrivateKeyPath: keyPath2,
			Installations:  map[string]string{"orgB": "inst-b"},
		},
	}, logger)
	tm, err := provider()
	assert.NoError(t, err)

	_, err = tm.GetTokenForOrg(t.Context(), "unknown-org")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no GitHub App configured for org")
}

func TestGetTokenForOrgFallback(t *testing.T) {
	keyPath := generateTestKey(t)
	logger := slog.Default()

	t.Run("FallbackUsedForUnknownOrg", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath,
				Installations:  map[string]string{"squareup": "inst-sq"},
				FallbackOrg:    "squareup",
			},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)

		ctx := logging.ContextWithLogger(t.Context(), slog.Default())
		// Unknown org should not error when fallback is configured
		// (will fail at the HTTP level but not at the routing level)
		_, err = tm.GetTokenForOrg(ctx, "cashapp")
		// Error is expected here because we don't have a real GitHub API,
		// but it should NOT be "no GitHub App configured for org"
		assert.Error(t, err)
		assert.NotContains(t, err.Error(), "no GitHub App configured for org")
	})

	t.Run("FallbackOrgNotInInstallations", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath,
				Installations:  map[string]string{"squareup": "inst-sq"},
				FallbackOrg:    "nonexistent",
			},
		}, logger)
		_, err := provider()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fallback-org \"nonexistent\" is not in the installations map")
	})

	t.Run("NoFallbackStillErrorsForUnknownOrg", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				AppID:          "111",
				PrivateKeyPath: keyPath,
				Installations:  map[string]string{"squareup": "inst-sq"},
			},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)

		_, err = tm.GetTokenForOrg(t.Context(), "unknown-org")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no GitHub App configured for org")
	})
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
