package metrics

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ArtifactMetrics separates cache decisions, origin exchanges, and cache publication.
type ArtifactMetrics struct {
	resolutions metric.Int64Counter
	origin      metric.Int64Counter
	duration    metric.Float64Histogram
	fills       metric.Int64Counter
	writes      metric.Int64Counter
}

// NewArtifactMetrics uses the same provider as the server's HTTP metrics.
func NewArtifactMetrics() *ArtifactMetrics {
	m := otel.Meter("cachew.artifact")
	return &ArtifactMetrics{
		resolutions: NewMetric[metric.Int64Counter](m, "cachew.cache.resolutions", "{request}", "Final cache lookup decisions for artifact requests"),
		origin:      NewMetric[metric.Int64Counter](m, "cachew.origin.requests", "{request}", "Outbound artifact HTTP exchanges, including redirects and transport failures"),
		duration:    NewHistogram(m, "cachew.origin.request_duration_seconds", "s", "Time to origin response headers or transport failure", FastLatencyBuckets()),
		fills:       NewMetric[metric.Int64Counter](m, "cachew.cache.fills", "{fill}", "Terminal artifact cache fill and backfill outcomes"),
		writes:      NewMetric[metric.Int64Counter](m, "cachew.cache.tier_writes", "{write}", "Terminal cache write outcomes by tier"),
	}
}

const noValue = "none"

type artifactKey struct{}
type purposeKey struct{}

type cacheSource struct {
	backend string
	tier    int
}

type artifactRequest struct {
	metrics  *ArtifactMetrics
	attrs    []attribute.KeyValue
	external bool
	source   atomic.Pointer[cacheSource]
}

// WithRequest scopes observations to a configured strategy and registry, never an artifact URL.
func (m *ArtifactMetrics) WithRequest(ctx context.Context, strategy, registry string, external bool) context.Context {
	return context.WithValue(ctx, artifactKey{}, &artifactRequest{
		metrics: m, external: external,
		attrs: []attribute.KeyValue{attribute.String("strategy", strategy), attribute.String("registry", registry)},
	})
}

// ContinueArtifactRequest retains metric attribution without inheriting a request's cancellation.
func ContinueArtifactRequest(ctx, request context.Context) context.Context {
	if r, ok := request.Value(artifactKey{}).(*artifactRequest); ok {
		return context.WithValue(ctx, artifactKey{}, r)
	}
	return ctx
}

// WithCachePurpose distinguishes origin fills from background cache replication.
func WithCachePurpose(ctx context.Context, purpose string) context.Context {
	return context.WithValue(ctx, purposeKey{}, purpose)
}

// ObserveCacheSource reports the accepted tier, after stale and conditional probes are resolved.
func ObserveCacheSource(ctx context.Context, backend string, tier int) {
	if r, ok := ctx.Value(artifactKey{}).(*artifactRequest); ok {
		r.source.CompareAndSwap(nil, &cacheSource{backend: backend, tier: tier})
	}
}

// RecordCacheResolution records one decision, independently of subsequent body transfer.
func RecordCacheResolution(ctx context.Context, result, fallback string) {
	r, ok := ctx.Value(artifactKey{}).(*artifactRequest)
	if !ok {
		return
	}
	source, tier := noValue, -1
	if result == "hit" || result == "conditional" {
		source, tier = fallback, 0
		if s := r.source.Load(); s != nil {
			source, tier = s.backend, s.tier
		}
	}
	r.metrics.resolutions.Add(ctx, 1, r.options(attribute.String("result", result), attribute.String("source", source), attribute.Int("tier", tier)))
}

// RecordCacheFill records completion rather than assuming an origin 200 was cached.
func RecordCacheFill(ctx context.Context, result string) {
	if r, ok := ctx.Value(artifactKey{}).(*artifactRequest); ok {
		r.metrics.fills.Add(ctx, 1, r.options(attribute.String("purpose", cachePurpose(ctx)), attribute.String("result", result)))
	}
}

// RecordCacheWrite records a terminal outcome for one backend writer.
func RecordCacheWrite(ctx context.Context, backend string, tier int, result string) {
	if r, ok := ctx.Value(artifactKey{}).(*artifactRequest); ok {
		r.metrics.writes.Add(ctx, 1, r.options(attribute.String("backend", backend), attribute.Int("tier", tier),
			attribute.String("purpose", cachePurpose(ctx)), attribute.String("result", result)))
	}
}

// HasArtifactRequest avoids allocating observation wrappers for unrelated cache operations.
func HasArtifactRequest(ctx context.Context) bool {
	_, ok := ctx.Value(artifactKey{}).(*artifactRequest)
	return ok
}

func cachePurpose(ctx context.Context) string {
	if p, ok := ctx.Value(purposeKey{}).(string); ok {
		return p
	}
	return "origin"
}

func (r *artifactRequest) options(extra ...attribute.KeyValue) metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, len(r.attrs)+len(extra))
	attrs = append(attrs, r.attrs...)
	return metric.WithAttributes(append(attrs, extra...)...)
}

// ArtifactClient preserves client settings and measures individual HTTP exchanges without global transport changes.
func ArtifactClient(base *http.Client) *http.Client {
	client := *base
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	if _, ok := transport.(*artifactTransport); !ok {
		client.Transport = &artifactTransport{base: transport}
	}
	return &client
}

type artifactTransport struct {
	base http.RoundTripper
}

func (t *artifactTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r, ok := req.Context().Value(artifactKey{}).(*artifactRequest)
	if !ok || !r.external {
		return t.base.RoundTrip(req) //nolint:wrapcheck // Preserve the transport contract.
	}
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	status, errorType := noValue, noValue
	if resp != nil {
		status = strconv.Itoa(resp.StatusCode)
	}
	switch {
	case errors.Is(err, context.Canceled):
		errorType = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		errorType = "timeout"
	case err != nil:
		errorType = "transport"
		if n, isNetError := errors.AsType[net.Error](err); isNetError && n.Timeout() {
			errorType = "timeout"
		}
	}
	attrs := r.options(attribute.String("status_code", status), attribute.String("error_type", errorType))
	r.metrics.origin.Add(req.Context(), 1, attrs)
	r.metrics.duration.Record(req.Context(), time.Since(start).Seconds(), attrs)
	return resp, err //nolint:wrapcheck // Preserve the transport error for http.Client.
}

func (t *artifactTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}
