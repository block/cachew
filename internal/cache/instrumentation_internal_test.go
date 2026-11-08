package cache

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
)

func TestObservedBackfillEarlyClose(t *testing.T) {
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	defer func() { otel.SetMeterProvider(previous); assert.NoError(t, provider.Shutdown(context.Background())) }()
	_, ctx := logging.Configure(t.Context(), logging.Config{})
	ctx = metrics.NewArtifactMetrics().WithRequest(ctx, "host", "registry.example", true)
	ctx = metrics.WithCachePurpose(ctx, "backfill")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c, err := NewMemory(ctx, MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()
	key := NewKey("artifact")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	w = observeWriter(ctx, c, 0, w, nil)
	r := newBackfillReadCloser(ctx, io.NopCloser(strings.NewReader("artifact")), w, cancel)
	p := make([]byte, 2)
	_, err = r.Read(p)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	_, _, err = c.Open(t.Context(), key)
	assert.IsError(t, err, os.ErrNotExist)
	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(t.Context(), &data))
	for _, name := range []string{"cachew.cache.fills", "cachew.cache.tier_writes"} {
		count := int64(0)
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != name {
					continue
				}
				sum := m.Data.(metricdata.Sum[int64])
				for _, point := range sum.DataPoints {
					result, _ := point.Attributes.Value("result")
					assert.Equal(t, "canceled", result.AsString())
					count += point.Value
				}
			}
		}
		assert.Equal(t, int64(1), count)
	}
}
