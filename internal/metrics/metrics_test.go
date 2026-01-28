package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
)

func TestMetricsClient(t *testing.T) {
	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, logging.Config{})
	_ = logger

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName: "cachew",
		Port:        9102,
	})
	assert.NoError(t, err)

	// Handler should return metrics
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	client.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	assert.NoError(t, client.Close())
}

func TestMetricsDedicatedServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger, ctx := logging.Configure(ctx, logging.Config{})
	_ = logger

	client, err := metrics.New(ctx, metrics.Config{
		ServiceName: "cachew-test",
		Port:        9103,
	})
	assert.NoError(t, err)
	defer client.Close()

	// ServeMetrics uses configured port
	err = client.ServeMetrics(ctx)
	assert.NoError(t, err)
}
