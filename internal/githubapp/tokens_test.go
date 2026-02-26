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

	t.Run("SkipsIncompleteConfigs", func(t *testing.T) {
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{Name: "missing-key", AppID: "123", Installations: map[string]string{"org": "inst"}},
			{Name: "missing-id", PrivateKeyPath: "/tmp/key.pem", Installations: map[string]string{"org": "inst"}},
			{Name: "missing-installations", AppID: "123", PrivateKeyPath: "/tmp/key.pem"},
		}, logger)
		tm, err := provider()
		assert.NoError(t, err)
		assert.Zero(t, tm)
	})

	t.Run("SingleApp", func(t *testing.T) {
		keyPath := generateTestKey(t)
		provider := githubapp.NewTokenManagerProvider([]githubapp.Config{
			{
				Name:           "app1",
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
				Name:           "app1",
				AppID:          "111",
				PrivateKeyPath: keyPath1,
				Installations:  map[string]string{"orgA": "inst-a"},
			},
			{
				Name:           "app2",
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
				Name:           "app1",
				AppID:          "111",
				PrivateKeyPath: keyPath1,
				Installations:  map[string]string{"orgA": "inst-a"},
			},
			{
				Name:           "app2",
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
			Name:           "app1",
			AppID:          "111",
			PrivateKeyPath: keyPath1,
			Installations:  map[string]string{"orgA": "inst-a"},
		},
		{
			Name:           "app2",
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
