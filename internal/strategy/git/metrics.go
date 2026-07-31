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

type gitMetrics struct {
	operationDuration        metric.Float64Histogram
	operationTotal           metric.Int64Counter
	requestTotal             metric.Int64Counter
	snapshotServeTotal       metric.Int64Counter
	snapshotServeSize        metric.Float64Histogram
	snapshotServeDuration    metric.Float64Histogram
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
	incrementalServeTotal    metric.Int64Counter
	incrementalFetchDuration metric.Float64Histogram
	incrementalEligibleTotal metric.Int64Counter
}

func newGitMetrics() *gitMetrics {
	meter := otel.Meter("cachew.git")
	return &gitMetrics{
		operationDuration:        metrics.NewHistogram(meter, "cachew.git.operation_duration_seconds", "s", "Duration of git operations (clone, fetch, repack, snapshot) by operation, status and trigger (background|incremental)", metrics.LatencyBuckets()),
		operationTotal:           metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.operations_total", "{operations}", "Total number of git operations by operation, status and trigger (background|incremental; incremental marks fetches issued synchronously on the request path by incremental pull-through)"),
		requestTotal:             metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.requests_total", "{requests}", "Total number of git HTTP requests by type"),
		snapshotServeTotal:       metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.snapshot_serves_total", "{serves}", "Snapshot serve events by source (cache, spool, cold_cache, generated) and repository"),
		snapshotServeSize:        metrics.NewHistogram(meter, "cachew.git.snapshot_serve_bytes", "By", "Size of served snapshots in bytes", metrics.ByteBuckets()),
		snapshotServeDuration:    metrics.NewHistogram(meter, "cachew.git.snapshot_serve_duration_seconds", "s", "Wall-clock duration of snapshot serves, from handler entry to last byte sent", metrics.LatencyBuckets()),
		bundleServeTotal:         metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.bundle_serves_total", "{serves}", "Bundle serve events by source (cache, generated, up_to_date, miss_bad_base, miss) and repository"),
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
		incrementalServeTotal:    metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.incremental_serves_total", "{serves}", "Incremental pull-through upload-pack serve events by outcome (local_hit, fetched, fallback_fetch_failed, fallback_missing, fallback_local_error, client_gone, client_gone_after_fetch, fallback_not_our_ref) and repository"),
		incrementalFetchDuration: metrics.NewHistogram(meter, "cachew.git.incremental_fetch_duration_seconds", "s", "Wall-clock time from upload-pack handler entry through making wanted objects available for an incremental pull-through serve, by outcome and repository", metrics.LatencyBuckets()),
		incrementalEligibleTotal: metrics.NewMetric[metric.Int64Counter](meter, "cachew.git.incremental_eligible_total", "{requests}", "Upload-pack POSTs against a ready mirror reaching the incremental pull-through decision, by engaged and repository; engaged=true matches an incremental_serves_total increment when the feature is enabled, except for a body mixing want and want-ref, which is forwarded before any serve is recorded; engaged=false means the request bypassed the incremental path (ls-refs, want-ref only, no wants, parse failure)"),
	}
}

// recordOperation records the duration and outcome of a git operation (clone,
// fetch, repack, snapshot). trigger is "background" for scheduled-job work and
// "incremental" for fetches issued on the request path by incremental
// pull-through, so the fetch SLO can separate the two.
func (m *gitMetrics) recordOperation(ctx context.Context, operation, status, trigger string, duration time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("operation", operation),
		attribute.String("status", status),
		attribute.String("trigger", trigger),
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
	if sizeBytes > 0 && duration > 0 {
		mbps := float64(sizeBytes) / (1 << 20) / duration.Seconds()
		m.snapshotServeBandwidth.Record(ctx, mbps, attrs)
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Float64("cachew.snapshot.bandwidth_mbps", mbps),
		)
	}
}

// recordBundleServe records a bundle serve event. Source is one of:
// "cache" (served from object cache), "generated" (created on demand from the
// local mirror), "up_to_date" (base already matches upstream HEAD, nothing to
// bundle), "miss_bad_base" (base unknown even after freshening the mirror),
// "miss_stale" (mirror freshen failed, so up-to-date could not be verified), or
// "miss" (no bundle could be produced for any other reason).
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

// recordIncrementalServe records an incremental pull-through upload-pack serve event.
// Outcome is one of: "local_hit", "fetched", "fallback_fetch_failed",
// "fallback_missing", "fallback_local_error", "client_gone",
// "client_gone_after_fetch", "fallback_not_our_ref". It is called once the serve
// outcome is known, so a local serve that ended in an upstream passthrough is not
// counted as a hit.
func (m *gitMetrics) recordIncrementalServe(ctx context.Context, outcome, repo string) {
	m.incrementalServeTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("repository", repo),
	))
}

// recordIncrementalEligible records an upload-pack POST that reached the incremental
// pull-through decision point. Engaged is true when wants were extracted, so
// the incremental path applies; false when the request bypassed it (ls-refs,
// want-ref, no wants, parse failure). Recorded even when the feature is
// config-disabled, so the ratio shows how much traffic would take the path
// independently of the kill switch.
func (m *gitMetrics) recordIncrementalEligible(ctx context.Context, engaged bool, repo string) {
	m.incrementalEligibleTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Bool("engaged", engaged),
		attribute.String("repository", repo),
	))
}

// recordIncrementalFetchDuration records how long the incremental pull-through
// path took, from upload-pack handler entry through making the wanted objects
// available. Recorded for every outcome, so local_hit gives the baseline for the
// no-fetch-needed case.
func (m *gitMetrics) recordIncrementalFetchDuration(ctx context.Context, outcome, repo string, duration time.Duration) {
	m.incrementalFetchDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("outcome", outcome),
		attribute.String("repository", repo),
	))
}
