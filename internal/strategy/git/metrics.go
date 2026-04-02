package git

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/metrics"
)

type gitMetrics struct {
	operationDuration metric.Float64Histogram
	operationTotal    metric.Int64Counter
	requestTotal      metric.Int64Counter
}

func newGitMetrics() *gitMetrics {
	meter := otel.Meter("cachew.git")
	return &gitMetrics{
		operationDuration: metrics.NewMetric[metric.Float64Histogram](meter, "cachew.git.operation_duration_seconds", "s", "Duration of git operations (clone, fetch, repack, snapshot)"),
		operationTotal:    metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.operations_total", "{operations}", "Total number of git operations"),
		requestTotal:      metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.requests_total", "{requests}", "Total number of git HTTP requests by type"),
	}
}

// recordOperation records the duration and outcome of a git operation (clone, fetch, repack, snapshot).
func (m *gitMetrics) recordOperation(ctx context.Context, operation, status string, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("status", status),
	)
	m.operationTotal.Add(ctx, 1, attrs)
	m.operationDuration.Record(ctx, duration.Seconds(), attrs)
}

func (m *gitMetrics) recordRequest(ctx context.Context, requestType string) {
	m.requestTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("type", requestType)))
}
