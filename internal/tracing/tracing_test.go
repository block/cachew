package tracing_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"

	"github.com/block/cachew/internal/tracing"
)

// New is a no-op when disabled and must return a non-nil stop function.
func TestNewDisabled(t *testing.T) {
	stop, err := tracing.New(context.Background(), tracing.Config{Enabled: false})
	if err != nil {
		t.Fatalf("New(disabled) returned error: %v", err)
	}
	if stop == nil {
		t.Fatal("New(disabled) returned nil stop func")
	}
	stop()
}

func TestNewExportsHTTPTraces(t *testing.T) {
	for _, test := range []struct {
		name           string
		protocol       string
		tracesProtocol string
		ignored        error
	}{
		{name: "GeneralProtocol", protocol: "http/protobuf"},
		{name: "TracesProtocolTakesPrecedence", protocol: "grpc", tracesProtocol: "http/protobuf"},
		{name: "CaseInsensitive", protocol: "HTTP/PROTOBUF"},
		{name: "UnsupportedTracesProtocolIgnored", protocol: "http/protobuf", tracesProtocol: "http/json",
			ignored: tracing.ErrUnsupportedProtocol},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests, err := exportSpan(t, test.protocol, test.tracesProtocol, "1000")
			if !errors.Is(err, test.ignored) {
				t.Fatalf("New returned %v, want %v", err, test.ignored)
			}
			select {
			case path := <-requests:
				if path != "/v1/traces" {
					t.Fatalf("trace path = %q, want /v1/traces", path)
				}
			default:
				t.Fatal("no HTTP trace request received")
			}
		})
	}
}

func TestNewFallsBackToGRPCForUnsupportedProtocol(t *testing.T) {
	requests, err := exportSpan(t, "http/json", "", "200")
	ignored := `OTEL_EXPORTER_OTLP_PROTOCOL="http/json"`
	if !errors.Is(err, tracing.ErrUnsupportedProtocol) || !strings.Contains(err.Error(), ignored) {
		t.Fatalf("New returned %v, want the ignored OTEL_EXPORTER_OTLP_PROTOCOL value", err)
	}
	select {
	case path := <-requests:
		t.Fatalf("unexpected HTTP trace request to %q", path)
	default:
	}
}

func exportSpan(t *testing.T, protocol, tracesProtocol, timeout string) (<-chan string, error) {
	t.Helper()
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requests <- r.URL.Path
		w.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", protocol)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", tracesProtocol)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", server.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", timeout)

	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	ctx := context.Background()
	stop, err := tracing.New(ctx, tracing.Config{Enabled: true})
	if stop == nil {
		t.Fatalf("New returned no stop function: %v", err)
	}
	_, span := otel.Tracer("cachew-test").Start(ctx, "restore")
	span.End()
	stop()
	return requests, err
}
