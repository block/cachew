package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/block/cachew/internal/logging"
)

// Config holds metrics configuration.
type Config struct {
	ServiceName string `help:"Service name for metrics." default:"cachew"`
	Port        int    `help:"Port for metrics server." default:"9102"`
}

// Client provides OpenTelemetry metrics with Prometheus exporter.
type Client struct {
	provider    metric.MeterProvider
	exporter    *prometheusexporter.Exporter
	registry    *prometheus.Registry
	serviceName string
	port        int
}

// New creates a new OpenTelemetry metrics client with Prometheus exporter.
func New(ctx context.Context, cfg Config) (*Client, error) {
	logger := logging.FromContext(ctx)

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

	registry := prometheus.NewRegistry()

	exporter, err := prometheusexporter.New(prometheusexporter.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)

	otel.SetMeterProvider(provider)

	client := &Client{
		provider:    provider,
		exporter:    exporter,
		registry:    registry,
		serviceName: cfg.ServiceName,
		port:        cfg.Port,
	}

	logger.InfoContext(ctx, "OpenTelemetry metrics initialized with Prometheus exporter",
		"service", cfg.ServiceName,
		"port", cfg.Port,
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

// ServeMetrics starts a dedicated HTTP server for Prometheus metrics scraping.
func (c *Client) ServeMetrics(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", c.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			logger.ErrorContext(ctx, "failed to write health check response", "error", err)
		}
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", c.port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.InfoContext(ctx, "Starting metrics server", "port", c.port)
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
