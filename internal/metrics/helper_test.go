package metrics_test

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/metrics"
)

func TestNewMetric(t *testing.T) {
	meter := otel.Meter("cachew.scheduler")
	queueDepth := metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.queue_depth", "{jobs}",
		"Number of jobs waiting in the scheduler queue")
	queueDepth.Record(t.Context(), 10)
}
