package git

import (
	"context"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type gitMetrics struct {
	operationDuration    metric.Float64Histogram
	operationTotal       metric.Int64Counter
	requestTotal         metric.Int64Counter
	cloneRejectionsTotal metric.Int64Counter
}

func newGitMetrics() (*gitMetrics, error) {
	meter := otel.Meter("cachew.git")
	m := &gitMetrics{}
	var err error

	if m.operationDuration, err = meter.Float64Histogram("cachew.git.operation_duration_seconds",
		metric.WithDescription("Duration of git operations (clone, fetch, repack, snapshot)"),
		metric.WithUnit("s")); err != nil {
		return nil, errors.Wrap(err, "create operation_duration histogram")
	}

	if m.operationTotal, err = meter.Int64Counter("cachew.git.operations_total",
		metric.WithDescription("Total number of git operations"),
		metric.WithUnit("{operations}")); err != nil {
		return nil, errors.Wrap(err, "create operations_total counter")
	}

	if m.requestTotal, err = meter.Int64Counter("cachew.git.requests_total",
		metric.WithDescription("Total number of git HTTP requests by type"),
		metric.WithUnit("{requests}")); err != nil {
		return nil, errors.Wrap(err, "create requests_total counter")
	}

	if m.cloneRejectionsTotal, err = meter.Int64Counter("cachew.git.clone_rejections_total",
		metric.WithDescription("Clone triggers rejected by per-client rate limiting"),
		metric.WithUnit("{rejections}")); err != nil {
		return nil, errors.Wrap(err, "create clone_rejections_total counter")
	}

	return m, nil
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

func (m *gitMetrics) recordCloneRejection(ctx context.Context, clientIP string) {
	m.cloneRejectionsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("client", clientIP)))
}
