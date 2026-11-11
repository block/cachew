package githubapp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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

func installationsJSON(t *testing.T, owners map[int]string) string {
	t.Helper()
	installations := []map[string]any{}
	for id, login := range owners {
		installations = append(installations, map[string]any{"id": id, "account": map[string]string{"login": login}})
	}
	body, err := json.Marshal(installations)
	assert.NoError(t, err)
	return string(body)
}

func rejectInstallationLookups(t *testing.T, mux *http.ServeMux) {
	t.Helper()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	})
}

func TestGetTokenForOrgDynamicDiscovery(t *testing.T) {
	mux := http.NewServeMux()
	rejectInstallationLookups(t, mux)
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		switch r.URL.Query().Get("page") {
		case "1":
			owners := map[int]string{12345: "block"}
			for i := range 99 {
				owners[i+1] = fmt.Sprintf("filler-%d", i)
			}
			_, _ = fmt.Fprint(w, installationsJSON(t, owners))
		case "2":
			_, _ = fmt.Fprint(w, installationsJSON(t, map[int]string{67890: "AfterpayTouch"}))
		default:
			t.Errorf("unexpected page %q", r.URL.Query().Get("page"))
		}
	})
	mux.HandleFunc("POST /app/installations/12345/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"token": "ghs_block_token", "expires_at": "2099-01-01T00:00:00Z"}) //nolint:errcheck
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
		{},
		{
			AppID:          "111",
			PrivateKeyPath: keyPath,
			FallbackOrg:    "block",
		},
	}, logger, server.URL, server.Client())

	ctx := logging.ContextWithLogger(t.Context(), slog.Default())

	t.Run("FallbackBeforeDiscovery", func(t *testing.T) {
		token, installation, err := tm.GetTokenForOrgWithInstallation(ctx, "unknown-org")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_block_token", token)
		assert.Equal(t, githubapp.Installation{ID: "12345", Owner: "block"}, installation)
	})

	t.Run("DiscoverAndCacheInstallation", func(t *testing.T) {
		token, installation, err := tm.GetTokenForOrgWithInstallation(ctx, "block")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_block_token", token)
		assert.Equal(t, githubapp.Installation{ID: "12345", Owner: "block"}, installation)

		// Second call should use cache
		token, err = tm.GetTokenForOrg(ctx, "block")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_block_token", token)
	})

	t.Run("DiscoverInstallationOnLaterPage", func(t *testing.T) {
		token, installation, err := tm.GetTokenForOrgWithInstallation(ctx, "AfterpayTouch")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_afterpay_token", token)
		assert.Equal(t, githubapp.Installation{ID: "67890", Owner: "AfterpayTouch"}, installation)
	})

	t.Run("FallbackForUnknownOrg", func(t *testing.T) {
		token, err := tm.GetTokenForOrg(ctx, "unknown-org")
		assert.NoError(t, err)
		assert.Equal(t, "ghs_block_token", token)
	})
}

func TestGetTokenForOrgNoFallback(t *testing.T) {
	mux := http.NewServeMux()
	rejectInstallationLookups(t, mux)
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[]`)
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

func TestGetTokenForOrgMatchesOwnerCaseInsensitively(t *testing.T) {
	var mints atomic.Int64
	mux := http.NewServeMux()
	rejectInstallationLookups(t, mux)
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, installationsJSON(t, map[int]string{123: "Example-Org"}))
	})
	mux.HandleFunc("POST /app/installations/123/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		mints.Add(1)
		w.WriteHeader(http.StatusCreated)
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"token": "test-token", "expires_at": "2099-01-01T00:00:00Z"}))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	manager := githubapp.NewTokenManagerForTest(t, []githubapp.Config{{AppID: "111", PrivateKeyPath: generateTestKey(t)}},
		slog.Default(), server.URL, server.Client())
	ctx := logging.ContextWithLogger(t.Context(), slog.Default())
	for _, alias := range []string{"example-org", "EXAMPLE-ORG", "Example-Org"} {
		token, installation, err := manager.GetTokenForOrgWithInstallation(ctx, alias)
		assert.NoError(t, err)
		assert.Equal(t, "test-token", token)
		assert.Equal(t, githubapp.Installation{ID: "123", Owner: "Example-Org"}, installation)
	}
	assert.Equal(t, int64(1), mints.Load())
}

func TestRefreshInstallationsFindsNewInstallations(t *testing.T) {
	var listings atomic.Int64
	var listingStatus atomic.Int64
	listingStatus.Store(http.StatusOK)
	var listing atomic.Value
	listing.Store(installationsJSON(t, map[int]string{1: "block"}))
	mux := http.NewServeMux()
	rejectInstallationLookups(t, mux)
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, _ *http.Request) {
		listings.Add(1)
		w.WriteHeader(int(listingStatus.Load()))
		_, _ = fmt.Fprint(w, listing.Load())
	})
	mux.HandleFunc("POST /app/installations/{id}/access_tokens", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"token":"token-%s","expires_at":"2099-01-01T00:00:00Z"}`, r.PathValue("id"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	manager := githubapp.NewTokenManagerForTest(t, []githubapp.Config{{
		AppID: "111", PrivateKeyPath: generateTestKey(t), FallbackOrg: "block",
	}}, slog.Default(), server.URL, server.Client())
	ctx := logging.ContextWithLogger(t.Context(), slog.Default())

	for range 3 {
		token, installation, err := manager.GetTokenForOrgWithInstallation(ctx, "cashapp")
		assert.NoError(t, err)
		assert.Equal(t, "token-1", token)
		assert.Equal(t, githubapp.Installation{ID: "1", Owner: "block"}, installation)
	}
	assert.Equal(t, int64(1), listings.Load())

	listing.Store(installationsJSON(t, map[int]string{1: "block", 2: "CashApp"}))
	assert.NoError(t, manager.RefreshInstallations(ctx))
	token, installation, err := manager.GetTokenForOrgWithInstallation(ctx, "cashapp")
	assert.NoError(t, err)
	assert.Equal(t, "token-2", token)
	assert.Equal(t, githubapp.Installation{ID: "2", Owner: "CashApp"}, installation)

	listingStatus.Store(http.StatusInternalServerError)
	assert.Error(t, manager.RefreshInstallations(ctx))
	token, err = manager.GetTokenForOrg(ctx, "cashapp")
	assert.NoError(t, err)
	assert.Equal(t, "token-2", token)
	assert.Equal(t, int64(3), listings.Load())
}

func TestGetTokenForOrgNilManager(t *testing.T) {
	var tm *githubapp.TokenManager
	_, err := tm.GetTokenForOrg(t.Context(), "any")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestMissingInstallationsMakeNoPerOrgCalls(t *testing.T) {
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		assert.NoError(t, provider.Shutdown(context.Background()))
	})
	mux := http.NewServeMux()
	rejectInstallationLookups(t, mux)
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[]`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	manager := githubapp.NewTokenManagerForTest(t, []githubapp.Config{{AppID: "111", PrivateKeyPath: generateTestKey(t)}},
		slog.Default(), server.URL, server.Client())
	ctx := logging.ContextWithLogger(t.Context(), slog.Default())
	for i := range 100 {
		_, err := manager.GetTokenForOrg(ctx, fmt.Sprintf("nonexistent-org-%d", i%10))
		assert.Error(t, err)
	}
	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(ctx, &data))
	var points []metricdata.DataPoint[int64]
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "github.api_calls" {
				continue
			}
			counter, ok := m.Data.(metricdata.Sum[int64])
			assert.True(t, ok)
			points = append(points, counter.DataPoints...)
		}
	}
	assert.Equal(t, 1, len(points))
	assert.Equal(t, int64(1), points[0].Value)
	assert.Equal(t, attribute.NewSet(attribute.String("owner", "unknown"),
		attribute.String("operation", "list_installations")), points[0].Attributes)
}

func TestGetTokenForURLNilManager(t *testing.T) {
	var tm *githubapp.TokenManager
	_, err := tm.GetTokenForURL(t.Context(), "https://github.com/org/repo")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}
