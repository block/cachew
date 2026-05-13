// Package tracing wires up the OpenTelemetry tracer provider so callers
// can use the standard otel.Tracer(...) API to create spans.
//
// Callers do not need to know how spans are exported. When tracing is
// disabled or no exporter is configured, the global tracer provider
// stays a no-op so otel.Tracer(...) calls remain safe.
package tracing

import (
	"context"
	"os"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Config holds tracing configuration.
type Config struct {
	Enabled bool `hcl:"enabled" help:"Enable distributed tracing." default:"false"`
}

// New registers an OpenTelemetry tracer provider on the global otel
// package and returns a stop function that flushes pending spans.
//
// New is a no-op when cfg.Enabled is false or when no exporter
// destination is configured.
func New(ctx context.Context, cfg Config) (stop func(), err error) {
	// The OTel SDK reads endpoint/protocol/TLS from OTEL_EXPORTER_OTLP_*
	// env vars; if no endpoint is set there is nowhere to ship spans.
	if !cfg.Enabled || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func() {}, nil
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, errors.Errorf("creating trace exporter: %w", err)
	}

	provider := trace.NewTracerProvider(trace.WithBatcher(exporter))
	otel.SetTracerProvider(provider)

	return func() {
		_ = provider.Shutdown(context.Background()) //nolint:errcheck // shutdown errors are not actionable
	}, nil
}
