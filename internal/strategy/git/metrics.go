package git

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/internal/metrics"
)

// backendNone labels serves whose bytes did not come from the object cache
// (e.g. on-demand generation), so the "backend" dimension is always populated.
const backendNone = "none"

type gitMetrics struct {
	operationDuration        metric.Float64Histogram
	operationTotal           metric.Int64Counter
	requestTotal             metric.Int64Counter
	snapshotServeTotal       metric.Int64Counter
	snapshotServeSize        metric.Float64Histogram
	snapshotServeDuration    metric.Float64Histogram
	snapshotServeTTFB        metric.Float64Histogram
	snapshotCacheOpenLatency metric.Float64Histogram
	bundleServeTotal         metric.Int64Counter
	bundleServeSize          metric.Float64Histogram
	bundleServeDuration      metric.Float64Histogram
	ensureRefsTotal          metric.Int64Counter
	ensureRefsDuration       metric.Float64Histogram
	spoolWriterDuration      metric.Float64Histogram
	spoolFollowerWaitTotal   metric.Int64Counter
	spoolFollowerWait        metric.Float64Histogram
	repackPackCount          metric.Float64Histogram
	snapshotServeBandwidth   metric.Float64Histogram
	lfsPhaseDuration         metric.Float64Histogram
	lfsPhaseBytes            metric.Float64Histogram
}

func newGitMetrics() *gitMetrics {
	meter := otel.Meter("cachew.git")
	return &gitMetrics{
		operationDuration:        metrics.NewHistogram(meter, "cachew.git.operation_duration_seconds", "s", "Duration of git operations (clone, fetch, repack, snapshot)", metrics.LatencyBuckets()),
		operationTotal:           metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.operations_total", "{operations}", "Total number of git operations"),
		requestTotal:             metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.requests_total", "{requests}", "Total number of git HTTP requests by type"),
		snapshotServeTotal:       metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.snapshot_serves_total", "{serves}", "Snapshot serve events by source (cache, spool, cold_cache, generated) and repository"),
		snapshotServeSize:        metrics.NewHistogram(meter, "cachew.git.snapshot_serve_bytes", "By", "Size of served snapshots in bytes", metrics.ByteBuckets()),
		snapshotServeDuration:    metrics.NewHistogram(meter, "cachew.git.snapshot_serve_duration_seconds", "s", "Wall-clock duration of snapshot serves, from handler entry to last byte sent", metrics.LatencyBuckets()),
		snapshotServeTTFB:        metrics.NewHistogram(meter, "cachew.git.snapshot_serve_ttfb_seconds", "s", "Server-side time-to-first-byte for snapshot serves, from handler entry to the first response byte, by source, backend and repository", metrics.LatencyBuckets()),
		snapshotCacheOpenLatency: metrics.NewHistogram(meter, "cachew.git.snapshot_cache_open_duration_seconds", "s", "Duration of the snapshot cache Open (lookup/metadata/reader creation) before streaming, by backend, status and repository", metrics.LatencyBuckets()),
		bundleServeTotal:         metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.bundle_serves_total", "{serves}", "Bundle serve events by source (cache, generated, miss) and repository"),
		bundleServeSize:          metrics.NewHistogram(meter, "cachew.git.bundle_serve_bytes", "By", "Size of served bundles in bytes", metrics.ByteBuckets()),
		bundleServeDuration:      metrics.NewHistogram(meter, "cachew.git.bundle_serve_duration_seconds", "s", "Wall-clock duration of bundle serves, including any on-demand generation", metrics.LatencyBuckets()),
		ensureRefsTotal:          metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.ensure_refs_total", "{requests}", "EnsureRefs requests by fetched and status"),
		ensureRefsDuration:       metrics.NewHistogram(meter, "cachew.git.ensure_refs_duration_seconds", "s", "Duration of EnsureRefs requests, including any upstream fetch", metrics.FastLatencyBuckets()),
		spoolWriterDuration:      metrics.NewHistogram(meter, "cachew.git.spool_writer_duration_seconds", "s", "Time the snapshot spool writer spent producing the stream", metrics.LatencyBuckets()),
		spoolFollowerWaitTotal:   metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.spool_follower_waits_total", "{waits}", "Snapshot spool follower events, by outcome (served, writer_failed)"),
		spoolFollowerWait:        metrics.NewHistogram(meter, "cachew.git.spool_follower_wait_seconds", "s", "Time a snapshot spool follower spent waiting for the writer to publish headers", metrics.FastLatencyBuckets()),
		repackPackCount:          metrics.NewHistogram(meter, "cachew.git.repack_pack_count", "{packs}", "Pack file count observed before and after repack, by stage (before, after)", metrics.SmallCountBuckets()),
		snapshotServeBandwidth:   metrics.NewHistogram(meter, "cachew.git.snapshot_serve_bandwidth_mbps", "MiBy/s", "Per-request snapshot serve throughput in MiB/s, by source and repository", metrics.BandwidthMbpsBuckets()),
		lfsPhaseDuration:         metrics.NewHistogram(meter, "cachew.git.lfs_phase_duration_seconds", "s", "Duration of an LFS-snapshot generation phase (discover, clone, fetch, archive_upload), by status and repository", metrics.LatencyBuckets()),
		lfsPhaseBytes:            metrics.NewHistogram(meter, "cachew.git.lfs_phase_bytes", "By", "Bytes processed in an LFS-snapshot generation phase, by phase and repository (e.g. .git/lfs size after fetch)", metrics.ByteBuckets()),
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

// recordSnapshotServe records a snapshot serve event with its source,
// repository, size and wall-clock duration. Also records per-request
// throughput (cachew.git.snapshot_serve_bandwidth_mbps) for non-empty,
// non-zero-duration serves so we can see the distribution of MiB/s instead
// of relying on aggregate-over-time of bytes/duration sums.
//
// Source is one of: "cache", "cold_cache", "spool", "generated".
//
// Backend is the cache tier that produced the bytes ("disk", "s3", ...) for
// cache-backed serves, or "" / "none" when not applicable (e.g. on-demand
// generation that never read from the object cache).
func (m *gitMetrics) recordSnapshotServe(ctx context.Context, source, backend, repo string, sizeBytes int64, duration time.Duration) {
	if backend == "" {
		backend = backendNone
	}
	attrs := metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("backend", backend),
		attribute.String("repository", repo),
	)
	m.snapshotServeTotal.Add(ctx, 1, attrs)
	if sizeBytes > 0 {
		m.snapshotServeSize.Record(ctx, float64(sizeBytes), attrs)
	}
	if duration > 0 {
		m.snapshotServeDuration.Record(ctx, duration.Seconds(), attrs)
	}
	if sizeBytes > 0 && duration > 0 {
		mbps := float64(sizeBytes) / (1 << 20) / duration.Seconds()
		m.snapshotServeBandwidth.Record(ctx, mbps, attrs)
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Float64("cachew.snapshot.bandwidth_mbps", mbps),
		)
	}
}

// recordSnapshotTTFB records server-side time-to-first-byte for a snapshot
// serve: the wall time from handler entry until the first response byte. For
// cache-backed serves this includes cache Open plus the latency until the
// backend (e.g. an S3 range reader) yields its first chunk, which is what a
// client observes as its snapshot "lookup" latency before the download begins.
func (m *gitMetrics) recordSnapshotTTFB(ctx context.Context, source, backend, repo string, ttfb time.Duration) {
	if ttfb <= 0 {
		return
	}
	if backend == "" {
		backend = backendNone
	}
	m.snapshotServeTTFB.Record(ctx, ttfb.Seconds(), metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("backend", backend),
		attribute.String("repository", repo),
	))
}

// recordSnapshotCacheOpen records the duration of the snapshot cache Open
// (lookup, metadata read and reader creation) that precedes streaming. Status
// is "hit", "miss" or "error". Backend is the serving tier on a hit.
func (m *gitMetrics) recordSnapshotCacheOpen(ctx context.Context, backend, repo, status string, duration time.Duration) {
	if backend == "" {
		backend = backendNone
	}
	m.snapshotCacheOpenLatency.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("backend", backend),
		attribute.String("repository", repo),
		attribute.String("status", status),
	))
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

// recordLFSPhase records the duration of one phase of LFS-snapshot
// generation. Phase is one of "discover", "clone", "fetch",
// "archive_upload". Status is "success" or "error".
func (m *gitMetrics) recordLFSPhase(ctx context.Context, repo, phase, status string, duration time.Duration) {
	m.lfsPhaseDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("repository", repo),
		attribute.String("phase", phase),
		attribute.String("status", status),
	))
}

// recordLFSPhaseBytes records the byte size associated with one phase of
// LFS-snapshot generation (e.g. .git/lfs total size observed after a
// fetch).
func (m *gitMetrics) recordLFSPhaseBytes(ctx context.Context, repo, phase string, sizeBytes int64) {
	if sizeBytes <= 0 {
		return
	}
	m.lfsPhaseBytes.Record(ctx, float64(sizeBytes), metric.WithAttributes(
		attribute.String("repository", repo),
		attribute.String("phase", phase),
	))
}
