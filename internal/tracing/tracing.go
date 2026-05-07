// Package tracing wires up the OpenTelemetry tracer provider so callers
// can use the standard otel.Tracer(...) API to create spans.
//
// The provider is backed by dd-trace-go, which serializes spans into
// Datadog's native msgpack format and ships them to the local Datadog
// Agent on port 8126. The OpenTelemetry-ness lives entirely in the API
// surface inside this binary — no OTLP exporter and no extra collector
// is involved.
//
// To switch backends later (e.g. to OTLP/Tempo) only New() needs to
// change; instrumentation sites that call otel.Tracer(...).Start(...)
// are unaffected.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	ddotel "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/opentelemetry"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Config holds tracing configuration.
type Config struct {
	Enabled bool `hcl:"enabled" help:"Enable distributed tracing." default:"false"`
}

// New registers a Datadog-backed OpenTelemetry tracer provider on the
// global otel package. Returns a stop function that flushes pending
// spans and stops the underlying tracer.
//
// When cfg.Enabled is false this is a no-op and the global tracer
// provider remains the OpenTelemetry default (a no-op provider), so
// callers can use otel.Tracer(...) safely either way.
func New(_ context.Context, cfg Config) (stop func(), err error) {
	if !cfg.Enabled {
		return func() {}, nil
	}

	// ddotel.NewTracerProvider starts the dd-trace-go tracer internally
	// and returns an OpenTelemetry TracerProvider that translates spans
	// into dd-trace-go's representation. DD_* env vars (DD_AGENT_HOST,
	// DD_SERVICE, DD_ENV, DD_VERSION) are picked up automatically.
	provider := ddotel.NewTracerProvider(
		tracer.WithRuntimeMetrics(),
	)
	otel.SetTracerProvider(provider)

	return func() {
		_ = provider.Shutdown() //nolint:errcheck // shutdown errors are not actionable
	}, nil
}
