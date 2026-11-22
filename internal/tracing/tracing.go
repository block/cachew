// Package tracing installs the OpenTelemetry tracer provider used by the standard otel.Tracer(...) API.
//
// When tracing is disabled or has no exporter, the global provider stays a no-op so otel.Tracer calls are safe.
package tracing

import (
	"context"
	"os"
	"strings"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

// Config holds tracing configuration.
type Config struct {
	Enabled bool `hcl:"enabled" help:"Enable distributed tracing." default:"false"`
}

// ErrUnsupportedProtocol marks an OTLP protocol value that New ignored, as the OTel spec requires.
var ErrUnsupportedProtocol = errors.New("unsupported OTLP protocol")

// New registers a global OpenTelemetry tracer provider and returns a stop function that flushes pending spans.
//
// It is a no-op when cfg.Enabled is false or no exporter destination is configured. An ignored protocol value
// returns a working stop function with an error wrapping ErrUnsupportedProtocol, for the caller to report.
func New(ctx context.Context, cfg Config) (stop func(), err error) {
	// The OTel SDK reads its endpoint from OTEL_EXPORTER_OTLP_*; without one there is nowhere to ship spans.
	if !cfg.Enabled || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func() {}, nil
	}

	protocol, ignored := tracesProtocol()
	var exporter trace.SpanExporter
	if protocol == "http/protobuf" {
		exporter, err = otlptracehttp.New(ctx)
	} else {
		exporter, err = otlptracegrpc.New(ctx)
	}
	if err != nil {
		return nil, errors.Errorf("creating %s trace exporter: %w", protocol, err)
	}

	provider := trace.NewTracerProvider(trace.WithBatcher(exporter))
	otel.SetTracerProvider(provider)

	// Register a W3C trace context and baggage propagator so trace IDs cross HTTP boundaries.
	//
	// otelhttp and otelconnect use the global propagator; without it every service starts a new trace.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func() {
		_ = provider.Shutdown(context.Background()) //nolint:errcheck // shutdown errors are not actionable
	}, ignored
}

// tracesProtocol returns the OTLP transport for traces, defaulting to grpc for compatibility with earlier releases.
//
// The traces-specific variable takes precedence. Per the OTel spec, values are case-insensitive and an unsupported
// value is ignored with a warning rather than failing startup, so ignored reports each one.
func tracesProtocol() (protocol string, ignored error) { //nolint:privatedoc // explains why
	var unsupported []error
	for _, name := range []string{"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "OTEL_EXPORTER_OTLP_PROTOCOL"} {
		value := os.Getenv(name)
		switch lower := strings.ToLower(value); lower {
		case "":
		case "grpc", "http/protobuf":
			return lower, errors.Join(unsupported...)
		default:
			unsupported = append(unsupported, errors.Errorf("%w: %s=%q", ErrUnsupportedProtocol, name, value))
		}
	}
	return "grpc", errors.Join(unsupported...)
}
