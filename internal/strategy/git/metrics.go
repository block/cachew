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
	operationDuration      metric.Float64Histogram
	operationTotal         metric.Int64Counter
	requestTotal           metric.Int64Counter
	snapshotServeTotal     metric.Int64Counter
	snapshotServeSize      metric.Float64Histogram
	snapshotServeDuration  metric.Float64Histogram
	bundleServeTotal       metric.Int64Counter
	bundleServeSize        metric.Float64Histogram
	bundleServeDuration    metric.Float64Histogram
	ensureRefsTotal        metric.Int64Counter
	ensureRefsDuration     metric.Float64Histogram
	spoolWriterDuration    metric.Float64Histogram
	spoolFollowerWaitTotal metric.Int64Counter
	spoolFollowerWait      metric.Float64Histogram
	repackPackCount        metric.Float64Histogram
	prewarmRepoDuration    metric.Float64Histogram
	prewarmPassDuration    metric.Float64Histogram
	prewarmPassTotal       metric.Int64Counter
}

func newGitMetrics() *gitMetrics {
	meter := otel.Meter("cachew.git")
	return &gitMetrics{
		operationDuration:      metrics.NewHistogram(meter, "cachew.git.operation_duration_seconds", "s", "Duration of git operations (clone, fetch, repack, snapshot)", metrics.LatencyBuckets()),
		operationTotal:         metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.operations_total", "{operations}", "Total number of git operations"),
		requestTotal:           metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.requests_total", "{requests}", "Total number of git HTTP requests by type"),
		snapshotServeTotal:     metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.snapshot_serves_total", "{serves}", "Snapshot serve events by source (cache, spool, cold_cache, generated) and repository"),
		snapshotServeSize:      metrics.NewHistogram(meter, "cachew.git.snapshot_serve_bytes", "By", "Size of served snapshots in bytes", metrics.ByteBuckets()),
		snapshotServeDuration:  metrics.NewHistogram(meter, "cachew.git.snapshot_serve_duration_seconds", "s", "Wall-clock duration of snapshot serves, from handler entry to last byte sent", metrics.LatencyBuckets()),
		bundleServeTotal:       metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.bundle_serves_total", "{serves}", "Bundle serve events by source (cache, generated, miss) and repository"),
		bundleServeSize:        metrics.NewHistogram(meter, "cachew.git.bundle_serve_bytes", "By", "Size of served bundles in bytes", metrics.ByteBuckets()),
		bundleServeDuration:    metrics.NewHistogram(meter, "cachew.git.bundle_serve_duration_seconds", "s", "Wall-clock duration of bundle serves, including any on-demand generation", metrics.LatencyBuckets()),
		ensureRefsTotal:        metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.ensure_refs_total", "{requests}", "EnsureRefs requests by fetched and status"),
		ensureRefsDuration:     metrics.NewHistogram(meter, "cachew.git.ensure_refs_duration_seconds", "s", "Duration of EnsureRefs requests, including any upstream fetch", metrics.FastLatencyBuckets()),
		spoolWriterDuration:    metrics.NewHistogram(meter, "cachew.git.spool_writer_duration_seconds", "s", "Time the snapshot spool writer spent producing the stream", metrics.LatencyBuckets()),
		spoolFollowerWaitTotal: metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.spool_follower_waits_total", "{waits}", "Snapshot spool follower events, by outcome (served, writer_failed)"),
		spoolFollowerWait:      metrics.NewHistogram(meter, "cachew.git.spool_follower_wait_seconds", "s", "Time a snapshot spool follower spent waiting for the writer to publish headers", metrics.FastLatencyBuckets()),
		repackPackCount:        metrics.NewHistogram(meter, "cachew.git.repack_pack_count", "{packs}", "Pack file count observed before and after repack, by stage (before, after)", metrics.SmallCountBuckets()),
		prewarmRepoDuration:    metrics.NewHistogram(meter, "cachew.git.prewarm_repo_duration_seconds", "s", "Duration of per-repo background prewarm, by path (fetch, restore) and status", metrics.LatencyBuckets()),
		prewarmPassDuration:    metrics.NewHistogram(meter, "cachew.git.prewarm_pass_duration_seconds", "s", "Total duration of the background prewarm pass, by outcome", metrics.LatencyBuckets()),
		prewarmPassTotal:       metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.prewarm_passes_total", "{passes}", "Background prewarm passes completed, by outcome (complete, empty_histogram)"),
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

// recordSnapshotServe records a snapshot serve event with its source, repository, size and wall-clock duration.
// Source is one of: "cache", "cold_cache", "spool", "generated".
func (m *gitMetrics) recordSnapshotServe(ctx context.Context, source, repo string, sizeBytes int64, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("repository", repo),
	)
	m.snapshotServeTotal.Add(ctx, 1, attrs)
	if sizeBytes > 0 {
		m.snapshotServeSize.Record(ctx, float64(sizeBytes), attrs)
	}
	if duration > 0 {
		m.snapshotServeDuration.Record(ctx, duration.Seconds(), attrs)
	}
}

// recordBundleServe records a bundle serve event. Source is one of:
// "cache" (served from object cache), "generated" (created on demand from the
// local mirror), or "miss" (no bundle could be produced).
func (m *gitMetrics) recordBundleServe(ctx context.Context, source, repo string, sizeBytes int64, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("repository", repo),
	)
	m.bundleServeTotal.Add(ctx, 1, attrs)
	if sizeBytes > 0 {
		m.bundleServeSize.Record(ctx, float64(sizeBytes), attrs)
	}
	if duration > 0 {
		m.bundleServeDuration.Record(ctx, duration.Seconds(), attrs)
	}
}

// recordEnsureRefs records an EnsureRefs request, including whether it
// triggered an upstream fetch.
func (m *gitMetrics) recordEnsureRefs(ctx context.Context, status string, fetched bool, repo string, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("status", status),
		attribute.Bool("fetched", fetched),
		attribute.String("repository", repo),
	)
	m.ensureRefsTotal.Add(ctx, 1, attrs)
	m.ensureRefsDuration.Record(ctx, duration.Seconds(), attrs)
}

// recordSpoolWriter records how long the snapshot-spool writer goroutine
// spent producing the stream from cloneForSnapshot through MarkComplete.
func (m *gitMetrics) recordSpoolWriter(ctx context.Context, repo, status string, duration time.Duration) {
	m.spoolWriterDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("repository", repo),
		attribute.String("status", status),
	))
}

// recordSpoolFollowerWait records how long a follower waited on the writer
// to publish spool headers, and the outcome of the follower's serve attempt.
func (m *gitMetrics) recordSpoolFollowerWait(ctx context.Context, repo, outcome string, wait time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("repository", repo),
		attribute.String("outcome", outcome),
	)
	m.spoolFollowerWaitTotal.Add(ctx, 1, attrs)
	m.spoolFollowerWait.Record(ctx, wait.Seconds(), attrs)
}

// recordRepackPackCount records the pack-file count observed on a mirror at
// a given stage of a repack. Stage is "before" or "after".
func (m *gitMetrics) recordRepackPackCount(ctx context.Context, repo, stage string, count int) {
	m.repackPackCount.Record(ctx, float64(count), metric.WithAttributes(
		attribute.String("repository", repo),
		attribute.String("stage", stage),
	))
}

// recordPrewarmRepo records the wall-clock cost of warming a single repo
// during the background prewarm pass. path is "fetch" (mirror already on
// disk) or "restore" (mirror snapshot pulled from S3 or full clone). status
// is "success" or "error".
func (m *gitMetrics) recordPrewarmRepo(ctx context.Context, path, status string, duration time.Duration) {
	m.prewarmRepoDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("path", path),
		attribute.String("status", status),
	))
}

// recordPrewarmPass records the total prewarm-pass wall time and completion
// outcome. outcome is "complete" or "empty_histogram".
func (m *gitMetrics) recordPrewarmPass(ctx context.Context, outcome string, duration time.Duration) {
	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	m.prewarmPassTotal.Add(ctx, 1, attrs)
	m.prewarmPassDuration.Record(ctx, duration.Seconds(), attrs)
}
