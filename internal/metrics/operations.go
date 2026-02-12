package metrics

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OperationMetrics provides a generic way to record any operation's metrics
// without needing to create separate structs for each operation type.
// Just call RecordOperation() with the operation name, duration, and custom attributes.
type OperationMetrics struct {
	duration metric.Float64Histogram
	count    metric.Int64Counter
}

// NewOperationMetrics creates a generic operation metrics recorder.
func NewOperationMetrics() (*OperationMetrics, error) {
	meter := otel.Meter("cachew")

	duration, err := meter.Float64Histogram(
		"cachew.operation.duration",
		metric.WithDescription("Duration of cachew operations (git clone, fetch, hermit download, etc.)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create duration histogram: %w", err)
	}

	count, err := meter.Int64Counter(
		"cachew.operation.count",
		metric.WithDescription("Count of cachew operations by type and result"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create count counter: %w", err)
	}

	return &OperationMetrics{
		duration: duration,
		count:    count,
	}, nil
}

// RecordOperation records any operation with custom attributes.
//
// Examples:
//
//	// Git clone
//	ops.RecordOperation(ctx, "git.clone", "success", cloneDuration,
//	    attribute.String("repository_url", repoURL))
//
//	// Git fetch
//	ops.RecordOperation(ctx, "git.fetch", "failure", fetchDuration,
//	    attribute.String("repository_url", repoURL),
//	    attribute.String("error", "timeout"))
//
//	// Hermit download
//	ops.RecordOperation(ctx, "hermit.download", "success", downloadDuration,
//	    attribute.String("package", "hermit"),
//	    attribute.String("version", "1.2.3"))
//
//	// Snapshot generation
//	ops.RecordOperation(ctx, "snapshot.generate", "success", duration,
//	    attribute.String("repository", "blox"),
//	    attribute.Int64("size_bytes", 1234567))
func (m *OperationMetrics) RecordOperation(ctx context.Context, operation, result string, duration time.Duration, customAttrs ...attribute.KeyValue) {
	if m == nil {
		return
	}

	// Base attributes that every operation has
	baseAttrs := []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("result", result),
	}

	// Combine base and custom attributes
	allAttrs := baseAttrs
	allAttrs = append(allAttrs, customAttrs...)

	// Record duration
	m.duration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(allAttrs...))

	// Increment count
	m.count.Add(ctx, 1,
		metric.WithAttributes(allAttrs...))
}

// RecordCount records a count metric without duration.
// Useful for cache hits/misses, request counts, etc.
//
// Examples:
//
//	// Cache hit
//	ops.RecordCount(ctx, "cache.hit", 1,
//	    attribute.String("strategy", "git"))
//
//	// Cache miss
//	ops.RecordCount(ctx, "cache.miss", 1,
//	    attribute.String("strategy", "git"))
//
//	// Batch operation
//	ops.RecordCount(ctx, "git.refs.synced", 42,
//	    attribute.String("repository_url", repoURL))
func (m *OperationMetrics) RecordCount(ctx context.Context, operation string, value int64, customAttrs ...attribute.KeyValue) {
	if m == nil {
		return
	}

	baseAttrs := []attribute.KeyValue{
		attribute.String("operation", operation),
	}

	allAttrs := baseAttrs
	allAttrs = append(allAttrs, customAttrs...)

	m.count.Add(ctx, value,
		metric.WithAttributes(allAttrs...))
}

// Context helpers

type contextKey struct{}

// ContextWithOperations adds OperationMetrics to the context.
func ContextWithOperations(ctx context.Context, ops *OperationMetrics) context.Context {
	return context.WithValue(ctx, contextKey{}, ops)
}

// FromContext extracts OperationMetrics from the context. Returns nil if not found.
func FromContext(ctx context.Context) *OperationMetrics {
	ops, _ := ctx.Value(contextKey{}).(*OperationMetrics)
	return ops
}
