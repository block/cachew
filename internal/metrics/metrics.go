package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/block/cachew/internal/logging"
)

// Config holds metrics configuration.
type Config struct {
	ServiceName        string `hcl:"service-name" help:"Service name for metrics." default:"cachew"`
	Port               int    `hcl:"port" help:"Port for Prometheus metrics server." default:"9102"`
	EnablePrometheus   bool   `hcl:"enable-prometheus" help:"Enable Prometheus exporter." default:"true"`
	EnableOTLP         bool   `hcl:"enable-otlp" help:"Enable OTLP exporter." default:"false"`
	OTLPEndpoint       string `hcl:"otlp-endpoint" help:"OTLP endpoint URL." default:"http://localhost:4318"`
	OTLPInsecure       bool   `hcl:"otlp-insecure" help:"Use insecure connection for OTLP." default:"false"`
	OTLPExportInterval int    `hcl:"otlp-export-interval" help:"OTLP export interval in seconds." default:"60"`
}

// Client provides OpenTelemetry metrics with configurable exporters.
type Client struct {
	provider          metric.MeterProvider
	prometheusEnabled bool
	exporter          *prometheusexporter.Exporter
	registry          *prometheus.Registry
	serviceName       string
	port              int
	operations        *OperationMetrics
}

// New creates a new OpenTelemetry metrics client with configurable exporters.
func New(ctx context.Context, cfg Config) (*Client, error) {
	logger := logging.FromContext(ctx)

	// Validate that at least one exporter is enabled
	if !cfg.EnablePrometheus && !cfg.EnableOTLP {
		return nil, errors.New("at least one exporter (Prometheus or OTLP) must be enabled")
	}

	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithProcess(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var readers []sdkmetric.Reader
	var registry *prometheus.Registry
	var prometheusExporter *prometheusexporter.Exporter
	exporters := []string{}

	// Configure Prometheus exporter if enabled
	if cfg.EnablePrometheus {
		registry = prometheus.NewRegistry()
		prometheusExporter, err = prometheusexporter.New(prometheusexporter.WithRegisterer(registry))
		if err != nil {
			return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
		}
		readers = append(readers, prometheusExporter)
		exporters = append(exporters, "prometheus")
	}

	// Configure OTLP exporter if enabled
	if cfg.EnableOTLP {
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpointURL(cfg.OTLPEndpoint),
		}
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}

		otlpExporter, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}

		// Create periodic reader for OTLP
		reader := sdkmetric.NewPeriodicReader(
			otlpExporter,
			sdkmetric.WithInterval(time.Duration(cfg.OTLPExportInterval)*time.Second),
		)
		readers = append(readers, reader)
		exporters = append(exporters, "otlp")
	}

	// Create meter provider with all configured readers
	providerOpts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
	}
	for _, reader := range readers {
		providerOpts = append(providerOpts, sdkmetric.WithReader(reader))
	}

	provider := sdkmetric.NewMeterProvider(providerOpts...)
	otel.SetMeterProvider(provider)

	// Initialize generic operation metrics for any operation (git, hermit, http, etc.)
	operations, err := NewOperationMetrics()
	if err != nil {
		logger.WarnContext(ctx, "Failed to initialize operation metrics, continuing without them", "error", err)
	}

	client := &Client{
		provider:          provider,
		prometheusEnabled: cfg.EnablePrometheus,
		exporter:          prometheusExporter,
		registry:          registry,
		serviceName:       cfg.ServiceName,
		port:              cfg.Port,
		operations:        operations,
	}

	logger.InfoContext(ctx, "OpenTelemetry metrics initialized",
		"service", cfg.ServiceName,
		"exporters", exporters,
		"prometheus_port", cfg.Port,
		"otlp_endpoint", cfg.OTLPEndpoint,
		"operation_metrics_enabled", operations != nil,
	)

	return client, nil
}

// Close shuts down the meter provider.
func (c *Client) Close() error {
	if c.provider == nil {
		return nil
	}
	if provider, ok := c.provider.(*sdkmetric.MeterProvider); ok {
		if err := provider.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("failed to shutdown meter provider: %w", err)
		}
	}
	return nil
}

// Handler returns the HTTP handler for the /metrics endpoint.
func (c *Client) Handler() http.Handler {
	if c.registry == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})
	}
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Operations returns the generic operation metrics recorder.
// Use this to record any operation (git clone, fetch, hermit download, etc.)
// without needing to create separate metric structs.
// Returns nil if operation metrics were not initialized.
func (c *Client) Operations() *OperationMetrics {
	if c == nil {
		return nil
	}
	return c.operations
}

// ServeMetrics starts a dedicated HTTP server for Prometheus metrics scraping.
// This is only started if Prometheus exporter is enabled.
func (c *Client) ServeMetrics(ctx context.Context) error {
	// Only start metrics server if Prometheus is enabled
	if !c.prometheusEnabled {
		return nil
	}

	logger := logging.FromContext(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", c.Handler())

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", c.port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.InfoContext(ctx, "Starting Prometheus metrics server", "port", c.port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.ErrorContext(ctx, "Metrics server error", "error", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.ErrorContext(shutdownCtx, "Metrics server shutdown error", "error", err)
		}
	}()

	return nil
}
