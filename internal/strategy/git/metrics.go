package git

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var meter = otel.Meter("cachew/git")

// Clone metrics track how repositories are initially populated on a cachew pod.
// The "source" attribute distinguishes between:
//   - "local":    already present on local disk from a previous pod lifecycle
//   - "mirror":   restored from a cached mirror snapshot (e.g. S3)
//   - "upstream": full clone from upstream (e.g. GitHub)
//
// Mirror restores also include a "size_class" attribute (small/medium/large/huge)
// to allow segmented analysis without per-repo cardinality.
var (
	cloneTotal      metric.Int64Counter
	cloneDuration   metric.Float64Histogram
	cloneBytes      metric.Int64Histogram
	cloneThroughput metric.Float64Histogram
)

// Clone outcome counters track success/failure independently from source.
// The "error_class" attribute on failures provides low-cardinality bucketing
// (e.g. "timeout", "auth", "network", "unknown").
var (
	cloneSuccess metric.Int64Counter
	cloneFailure metric.Int64Counter
)

// Snapshot cache metrics track whether the pre-built snapshot artifact was
// found in the cache layer when a workstation requested it.
var (
	snapshotCacheHit  metric.Int64Counter
	snapshotCacheMiss metric.Int64Counter
)

// Snapshot serve metrics track how snapshot.tar.zst requests to workstations
// are fulfilled. The "source" attribute distinguishes between:
//   - "cache": served from a pre-built snapshot in the cache layer
//   - "live":  generated on-the-fly from the local mirror (cache miss)
var (
	snapshotServeTotal      metric.Int64Counter
	snapshotServeDuration   metric.Float64Histogram
	snapshotServeBytes      metric.Int64Histogram
	snapshotServeThroughput metric.Float64Histogram
)

func init() {
	var err error

	cloneTotal, err = meter.Int64Counter("cachew.git.clone.total",
		metric.WithDescription("Number of repository clones by source."))
	if err != nil {
		panic(err)
	}

	cloneDuration, err = meter.Float64Histogram("cachew.git.clone.duration_ms",
		metric.WithDescription("Time to clone/restore a repository in milliseconds."),
		metric.WithExplicitBucketBoundaries(100, 500, 1000, 5000, 10000, 30000, 60000, 120000, 300000))
	if err != nil {
		panic(err)
	}

	cloneBytes, err = meter.Int64Histogram("cachew.git.clone.bytes",
		metric.WithDescription("Compressed bytes read during mirror snapshot restore."),
		metric.WithExplicitBucketBoundaries(1e6, 10e6, 100e6, 500e6, 1e9, 5e9, 10e9, 20e9))
	if err != nil {
		panic(err)
	}

	// S3 single-stream is ~100 MB/s, parallel range downloads up to ~250 MB/s.
	// Local serves can reach 400-500 MB/s.
	cloneThroughput, err = meter.Float64Histogram("cachew.git.clone.throughput_mbps",
		metric.WithDescription("Effective download throughput in MB/s during mirror snapshot restore."),
		metric.WithUnit("MB/s"),
		metric.WithExplicitBucketBoundaries(10, 25, 50, 75, 100, 150, 200, 250, 350, 500))
	if err != nil {
		panic(err)
	}

	cloneSuccess, err = meter.Int64Counter("cachew.git.clone.success",
		metric.WithDescription("Number of successful repository clones/restores."))
	if err != nil {
		panic(err)
	}

	cloneFailure, err = meter.Int64Counter("cachew.git.clone.failure",
		metric.WithDescription("Number of failed repository clones/restores, by error class."))
	if err != nil {
		panic(err)
	}

	snapshotCacheHit, err = meter.Int64Counter("cachew.git.snapshot.cache.hit",
		metric.WithDescription("Number of snapshot requests served from cache."))
	if err != nil {
		panic(err)
	}

	snapshotCacheMiss, err = meter.Int64Counter("cachew.git.snapshot.cache.miss",
		metric.WithDescription("Number of snapshot requests that required live generation."))
	if err != nil {
		panic(err)
	}

	snapshotServeTotal, err = meter.Int64Counter("cachew.git.snapshot.serve.total",
		metric.WithDescription("Number of snapshot requests served to workstations, by source (cache, live)."))
	if err != nil {
		panic(err)
	}

	snapshotServeDuration, err = meter.Float64Histogram("cachew.git.snapshot.serve.duration_ms",
		metric.WithDescription("Time to serve a snapshot to a workstation in milliseconds."),
		metric.WithExplicitBucketBoundaries(100, 500, 1000, 5000, 10000, 30000, 60000, 120000, 300000))
	if err != nil {
		panic(err)
	}

	snapshotServeBytes, err = meter.Int64Histogram("cachew.git.snapshot.serve.bytes",
		metric.WithDescription("Bytes served to a workstation for a snapshot request."),
		metric.WithExplicitBucketBoundaries(1e6, 10e6, 100e6, 500e6, 1e9, 5e9, 10e9, 20e9))
	if err != nil {
		panic(err)
	}

	// Local serves can sustain 400-500 MB/s. Upstream is typically much slower.
	snapshotServeThroughput, err = meter.Float64Histogram("cachew.git.snapshot.serve.throughput_mbps",
		metric.WithDescription("Effective throughput in MB/s serving a snapshot to a workstation."),
		metric.WithUnit("MB/s"),
		metric.WithExplicitBucketBoundaries(10, 25, 50, 75, 100, 150, 200, 250, 350, 500))
	if err != nil {
		panic(err)
	}
}

// sizeClass returns a low-cardinality bucket label for a byte count.
//
//   - "small":  < 100 MB
//   - "medium": 100 MB – 1 GB
//   - "large":  1 GB – 5 GB
//   - "huge":   > 5 GB
func sizeClass(bytes int64) string {
	switch {
	case bytes < 100*1e6:
		return "small"
	case bytes < 1e9:
		return "medium"
	case bytes < 5*1e9:
		return "large"
	default:
		return "huge"
	}
}

// recordCloneMetrics records clone/restore metrics with the given source label.
// bytesRead should be > 0 only for mirror restores; it is skipped for local/upstream.
func recordCloneMetrics(ctx context.Context, source string, duration time.Duration, bytesRead int64) {
	attrs := []attribute.KeyValue{attribute.String("source", source)}

	if bytesRead > 0 {
		attrs = append(attrs, attribute.String("size_class", sizeClass(bytesRead)))
	}

	opt := metric.WithAttributes(attrs...)
	cloneTotal.Add(ctx, 1, opt)
	cloneDuration.Record(ctx, float64(duration.Milliseconds()), opt)

	if bytesRead > 0 {
		cloneBytes.Record(ctx, bytesRead, opt)

		if secs := duration.Seconds(); secs > 0 {
			mbps := float64(bytesRead) / 1e6 / secs
			cloneThroughput.Record(ctx, mbps, opt)
		}
	}
}

// recordCloneSuccess increments the clone success counter with the given source.
func recordCloneSuccess(ctx context.Context, source string) {
	cloneSuccess.Add(ctx, 1, metric.WithAttributes(attribute.String("source", source)))
}

// recordCloneFailure increments the clone failure counter with an error class.
func recordCloneFailure(ctx context.Context, source string, err error) {
	cloneFailure.Add(ctx, 1, metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("error_class", classifyError(err)),
	))
}

// classifyError returns a low-cardinality error class for metrics tagging.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	msg := err.Error()
	switch {
	case contains(msg, "timeout", "deadline exceeded", "context deadline"):
		return "timeout"
	case contains(msg, "context canceled"):
		return "canceled"
	case contains(msg, "authentication", "authorization", "403", "401"):
		return "auth"
	case contains(msg, "connection refused", "no such host", "network", "dial"):
		return "network"
	default:
		return "unknown"
	}
}

// contains checks if s contains any of the substrings (case-insensitive).
func contains(s string, substrs ...string) bool {
	lower := strings.ToLower(s)
	for _, sub := range substrs {
		if strings.Contains(lower, sub) {
			return true
		}
	}
	return false
}

// recordSnapshotCacheResult records whether a snapshot was found in cache.
func recordSnapshotCacheResult(ctx context.Context, hit bool) {
	if hit {
		snapshotCacheHit.Add(ctx, 1)
	} else {
		snapshotCacheMiss.Add(ctx, 1)
	}
}

// recordSnapshotServe records metrics for a snapshot served to a workstation.
// bytesWritten and duration are optional (zero values are skipped).
func recordSnapshotServe(ctx context.Context, source string, duration time.Duration, bytesWritten int64) {
	attrs := []attribute.KeyValue{attribute.String("source", source)}

	if bytesWritten > 0 {
		attrs = append(attrs, attribute.String("size_class", sizeClass(bytesWritten)))
	}

	opt := metric.WithAttributes(attrs...)
	snapshotServeTotal.Add(ctx, 1, opt)

	if duration > 0 {
		snapshotServeDuration.Record(ctx, float64(duration.Milliseconds()), opt)
	}

	if bytesWritten > 0 {
		snapshotServeBytes.Record(ctx, bytesWritten, opt)

		if secs := duration.Seconds(); secs > 0 {
			mbps := float64(bytesWritten) / 1e6 / secs
			snapshotServeThroughput.Record(ctx, mbps, opt)
		}
	}
}
