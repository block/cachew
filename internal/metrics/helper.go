package metrics

import (
	"reflect"

	"go.opentelemetry.io/otel/metric"
)

// LatencyBuckets covers operations that span sub-millisecond cache hits all
// the way to multi-minute clone/repack/generation work. The default OTel
// histogram boundaries (0..10 seconds) compress everything beyond 10s into
// the +Inf bucket, which makes p50/p90/avg unusable for cachew's git path.
func LatencyBuckets() []float64 {
	return []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30, 60, 120, 300, 600,
	}
}

// FastLatencyBuckets is for operations expected to complete in well under
// a minute (spool follower waits, ensure-refs round trips).
func FastLatencyBuckets() []float64 {
	return []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
		1, 2.5, 5, 10, 30,
	}
}

// ByteBuckets covers cachew payload sizes, which range from tiny bundles
// (a few KiB) to multi-GiB snapshot tarballs.
func ByteBuckets() []float64 {
	return []float64{
		1 << 10, // 1 KiB
		1 << 14, // 16 KiB
		1 << 16, // 64 KiB
		1 << 18, // 256 KiB
		1 << 20, // 1 MiB
		1 << 22, // 4 MiB
		1 << 24, // 16 MiB
		1 << 26, // 64 MiB
		1 << 28, // 256 MiB
		1 << 30, // 1 GiB
		1 << 32, // 4 GiB
		1 << 33, // 8 GiB
		1 << 34, // 16 GiB
		1 << 35, // 32 GiB
	}
}

// SmallCountBuckets is for low-cardinality integer counts (e.g. number of
// pack files on a mirror), where the values of interest are mostly in the
// 1–100 range with a long tail.
func SmallCountBuckets() []float64 {
	return []float64{
		1, 2, 5, 10, 25, 50, 100, 250, 500, 1000,
	}
}

// BandwidthMbpsBuckets is for per-request throughput in MiB/s, covering
// everything from slow long-tail clients (a few MiB/s) up through saturated
// 10 GbE links (~1.2 GiB/s) and the parallel-stream-on-localhost ceiling.
func BandwidthMbpsBuckets() []float64 {
	return []float64{
		1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000,
	}
}

// NewHistogram creates a Float64Histogram with explicit bucket boundaries.
// Prefer this over NewMetric for histograms: the OTel SDK default boundaries
// only go up to 10 seconds, which is far too narrow for most cachew metrics.
func NewHistogram(meter metric.Meter, path, unit, description string, buckets []float64) metric.Float64Histogram {
	h, err := meter.Float64Histogram(
		path,
		metric.WithUnit(unit),
		metric.WithDescription(description),
		metric.WithExplicitBucketBoundaries(buckets...),
	)
	if err != nil {
		panic(err)
	}
	return h
}

func NewMetric[OM any](meter metric.Meter, path, unit, description string) OM {
	u := metric.WithUnit(unit)
	d := metric.WithDescription(description)
	switch reflect.TypeFor[OM]().Name() {
	case "Int64Gauge":
		om, err := meter.Int64Gauge(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Int64Counter":
		om, err := meter.Int64Counter(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Float64Histogram":
		om, err := meter.Float64Histogram(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)

	case "Float64Counter":
		om, err := meter.Float64Counter(path, u, d)
		if err != nil {
			panic(err)
		}
		return om.(OM)
	}
	panic("unsupported metric type")
}
