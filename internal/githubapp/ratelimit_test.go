package githubapp //nolint:testpackage // Exercise poll results and installation cache snapshots directly.

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
)

func pollerContext(t *testing.T) context.Context {
	t.Helper()
	return logging.ContextWithLogger(t.Context(), slog.Default())
}

func newMetricReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previous)
		assert.NoError(t, provider.Shutdown(context.Background()))
	})
	return reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string][]metricdata.DataPoint[int64] {
	t.Helper()
	var result metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &result))
	points := map[string][]metricdata.DataPoint[int64]{}
	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				points[m.Name] = data.DataPoints
			case metricdata.Sum[int64]:
				points[m.Name] = data.DataPoints
			}
		}
	}
	return points
}

func cachedTestApp(server *httptest.Server, appID, owner, installationID string) *appState {
	return &appState{
		appID: appID, apiBase: server.URL, httpClient: NewAPIClient(server.Client()),
		cacheConfig: DefaultTokenCacheConfig(), installationCache: map[string]string{owner: installationID},
		tokens: map[string]*cachedToken{owner: {token: "token-" + appID, expiresAt: time.Now().Add(time.Hour)}},
	}
}

func TestRateLimitPollerSamplesInstallations(t *testing.T) {
	reader := newMetricReader(t)
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/rate_limit", r.URL.Path)
		assert.Equal(t, "application/vnd.github+json", r.Header.Get("Accept"))
		authorization := r.Header.Get("Authorization")
		assert.True(t, authorization == "Bearer token-111" || authorization == "Bearer token-222")
		calls.Add(1)
		_, _ = fmt.Fprint(w, `{"resources":{"core":{"remaining":4900,"limit":5000,"used":100},"search":{"remaining":30,"limit":30,"used":0}}}`)
	}))
	defer server.Close()
	app := cachedTestApp(server, "111", "squareup", "123")
	app.installationCache["alias"] = "123"
	app.tokens["alias"] = app.tokens["squareup"]
	otherApp := cachedTestApp(server, "222", "block", "456")
	manager := &TokenManager{apps: []*appState{app, otherApp}, fallbackApp: app, fallbackOrg: "squareup"}
	assert.NoError(t, NewRateLimitPoller(manager, 0).poll(pollerContext(t)))
	assert.Equal(t, int64(2), calls.Load())
	data := collectMetrics(t, reader)
	for name, values := range map[string]map[string]int64{
		"github.ratelimit_remaining": {"core": 4900, "search": 30},
		"github.ratelimit_limit":     {"core": 5000, "search": 30},
		"github.ratelimit_used":      {"core": 100, "search": 0},
	} {
		assert.Equal(t, 4, len(data[name]))
		for _, point := range data[name] {
			resource, _ := point.Attributes.Value("resource")
			assert.Equal(t, values[resource.AsString()], point.Value)
			appID, _ := point.Attributes.Value("app")
			installation, _ := point.Attributes.Value("installation_id")
			if appID.AsString() == "111" {
				assert.Equal(t, "123", installation.AsString())
			} else {
				assert.Equal(t, "222", appID.AsString())
				assert.Equal(t, "456", installation.AsString())
			}
		}
	}
}

func TestRateLimitPollerFindsNewInstallations(t *testing.T) {
	reader := newMetricReader(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"resources":{"core":{"remaining":50,"limit":100,"used":50}}}`)
	}))
	defer server.Close()
	app := cachedTestApp(server, "111", "squareup", "123")
	poller := NewRateLimitPoller(&TokenManager{apps: []*appState{app}}, 0)
	assert.NoError(t, poller.poll(pollerContext(t)))
	assert.Equal(t, 1, len(collectMetrics(t, reader)["github.ratelimit_remaining"]))
	app.installationMu.Lock()
	app.installationCache["block"] = "456"
	app.installationMu.Unlock()
	app.mu.Lock()
	app.tokens["block"] = &cachedToken{token: "later-token", expiresAt: time.Now().Add(time.Hour)}
	app.mu.Unlock()
	assert.NoError(t, poller.poll(pollerContext(t)))
	assert.Equal(t, 2, len(collectMetrics(t, reader)["github.ratelimit_remaining"]))
}

func TestRateLimitPrometheusExport(t *testing.T) {
	previous := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	client, err := metrics.New(pollerContext(t), metrics.Config{ServiceName: "cachew", EnablePrometheus: true})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, client.Close()) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"resources":{"core":{"remaining":42,"limit":100,"used":58}}}`)
	}))
	defer server.Close()
	app := cachedTestApp(server, "111", "squareup", "123")
	assert.NoError(t, NewRateLimitPoller(&TokenManager{apps: []*appState{app}}, 0).poll(pollerContext(t)))
	response := httptest.NewRecorder()
	client.Handler().ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Contains(t, response.Body.String(), `github_ratelimit_remaining{`)
	assert.Contains(t, response.Body.String(), `app="111"`)
	assert.Contains(t, response.Body.String(), `installation_id="123"`)
	assert.Contains(t, response.Body.String(), `owner="squareup"`)
	assert.Contains(t, response.Body.String(), `resource="core"`)
}

func TestRateLimitPollerErrorsDoNotEmitZeroes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"Forbidden", http.StatusForbidden, `{"message":"forbidden"}`},
		{"Malformed", http.StatusOK, `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newMetricReader(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			app := cachedTestApp(server, "111", "squareup", "123")
			poller := NewRateLimitPoller(&TokenManager{apps: []*appState{app}}, 0)
			assert.Error(t, poller.poll(pollerContext(t)))
			assert.Zero(t, len(collectMetrics(t, reader)["github.ratelimit_remaining"]))
		})
	}
}

func TestRateLimitPollerContinuesAfterFailure(t *testing.T) {
	reader := newMetricReader(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer token-111" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = fmt.Fprint(w, `{"resources":{"core":{"remaining":42,"limit":100,"used":58}}}`)
	}))
	defer server.Close()
	manager := &TokenManager{apps: []*appState{
		cachedTestApp(server, "111", "squareup", "123"), cachedTestApp(server, "222", "block", "456"),
	}}
	assert.Error(t, NewRateLimitPoller(manager, 0).poll(pollerContext(t)))
	points := collectMetrics(t, reader)["github.ratelimit_remaining"]
	assert.Equal(t, 1, len(points))
	assert.Equal(t, int64(42), points[0].Value)
	assert.Equal(t, attribute.NewSet(attribute.String("app", "222"), attribute.String("owner", "block"),
		attribute.String("installation_id", "456"), attribute.String("resource", "core")), points[0].Attributes)
}

func TestRateLimitPollerRun(t *testing.T) {
	newMetricReader(t)
	var calls atomic.Int64
	repeated := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"resources":{}}`)
		if calls.Add(1) == 2 {
			close(repeated)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(pollerContext(t), time.Second)
	defer cancel()
	poller := NewRateLimitPoller(&TokenManager{apps: []*appState{cachedTestApp(server, "111", "squareup", "123")}}, time.Millisecond)
	done := make(chan struct{})
	go func() {
		poller.Run(ctx)
		close(done)
	}()
	select {
	case <-repeated:
	case <-ctx.Done():
		t.Fatal("poller did not repeat")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller did not stop")
	}
}

func TestRateLimitPollerCancelsRequest(t *testing.T) {
	newMetricReader(t)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(pollerContext(t))
	defer cancel()
	poller := NewRateLimitPoller(&TokenManager{apps: []*appState{cachedTestApp(server, "111", "squareup", "123")}}, 0)
	done := make(chan struct{})
	go func() {
		poller.Run(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("poller did not start immediately")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("in-flight poll was not cancelled")
	}
}

func TestRateLimitPollerWithoutApps(t *testing.T) {
	NewRateLimitPoller(nil, 0).Run(t.Context())
	assert.NoError(t, NewRateLimitPoller(&TokenManager{}, 0).poll(t.Context()))
}
