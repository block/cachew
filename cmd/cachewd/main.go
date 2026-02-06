package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/alecthomas/hcl/v2"
	"github.com/alecthomas/kong"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/config"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
	"github.com/block/cachew/internal/strategy"
	"github.com/block/cachew/internal/strategy/git"
	"github.com/block/cachew/internal/strategy/gomod"
)

type GlobalConfig struct {
	Bind            string              `hcl:"bind" default:"127.0.0.1:8080" help:"Bind address for the server."`
	URL             string              `hcl:"url" default:"http://127.0.0.1:8080/" help:"Base URL for cachewd."`
	SchedulerConfig jobscheduler.Config `embed:"" hcl:"scheduler,block" prefix:"scheduler-"`
	LoggingConfig   logging.Config      `embed:"" hcl:"logging,block" prefix:"log-"`
	MetricsConfig   metrics.Config      `embed:"" hcl:"metrics,block" prefix:"metrics-"`
}

var cli struct {
	Schema bool `help:"Print the configuration file schema." xor:"command"`

	Config *os.File `hcl:"-" help:"Configuration file path." placeholder:"PATH" required:"" default:"cachew.hcl"`

	// GlobalConfig accepts command-line, but can also be parsed from HCL.
	GlobalConfig
}

func main() {
	kctx := kong.Parse(&cli, kong.DefaultEnvars("CACHEW"))

	ast, err := hcl.Parse(cli.Config)
	kctx.FatalIfErrorf(err)

	globalConfig, providersConfig := config.Split[GlobalConfig](ast)

	err = hcl.UnmarshalAST(globalConfig, &cli.GlobalConfig)
	kctx.FatalIfErrorf(err)

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, cli.LoggingConfig)

	scheduler := jobscheduler.New(ctx, cli.SchedulerConfig)

	cr := cache.NewRegistry()
	cache.RegisterMemory(cr)
	cache.RegisterDisk(cr)
	cache.RegisterS3(cr)

	sr := strategy.NewRegistry()
	strategy.RegisterAPIV1(sr)
	strategy.RegisterArtifactory(sr)
	strategy.RegisterGitHubReleases(sr)
	strategy.RegisterHermit(sr, cli.URL)
	strategy.RegisterHost(sr)
	git.Register(sr, scheduler)
	gomod.Register(sr)

	// Commands
	switch { //nolint:gocritic
	case cli.Schema:
		schema := config.Schema[GlobalConfig](cr, sr)
		slices.SortStableFunc(schema.Entries, func(a, b hcl.Entry) int {
			return strings.Compare(a.EntryKey(), b.EntryKey())
		})
		text, err := hcl.MarshalAST(schema)
		kctx.FatalIfErrorf(err)

		if fileInfo, err := os.Stdout.Stat(); err == nil && (fileInfo.Mode()&os.ModeCharDevice) != 0 {
			err = quick.Highlight(os.Stdout, string(text), "terraform", "terminal256", "solarized")
			kctx.FatalIfErrorf(err)
		} else {
			fmt.Printf("%s\n", text) //nolint:forbidigo
		}
		return
	}

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /_liveness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	mux.HandleFunc("GET /_readiness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	err = config.Load(ctx, cr, sr, providersConfig, mux, parseEnvars())
	kctx.FatalIfErrorf(err)

	metricsClient, err := metrics.New(ctx, cli.MetricsConfig)
	kctx.FatalIfErrorf(err, "failed to create metrics client")
	defer func() {
		if err := metricsClient.Close(); err != nil {
			logger.ErrorContext(ctx, "failed to close metrics client", "error", err)
		}
	}()

	if err := metricsClient.ServeMetrics(ctx); err != nil {
		kctx.FatalIfErrorf(err, "failed to start metrics server")
	}

	logger.InfoContext(ctx, "Starting cachewd", slog.String("bind", cli.Bind))

	var handler http.Handler = mux

	handler = otelhttp.NewMiddleware(cli.MetricsConfig.ServiceName,
		otelhttp.WithMeterProvider(otel.GetMeterProvider()),
		otelhttp.WithTracerProvider(otel.GetTracerProvider()),
	)(handler)

	handler = httputil.LoggingMiddleware(handler)

	server := &http.Server{
		Addr:              cli.Bind,
		Handler:           handler,
		ReadTimeout:       30 * time.Minute,
		WriteTimeout:      30 * time.Minute,
		ReadHeaderTimeout: 30 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return logging.ContextWithLogger(ctx, logger.With("client", c.RemoteAddr().String()))
		},
	}

	err = server.ListenAndServe()
	kctx.FatalIfErrorf(err)
}

func parseEnvars() map[string]string {
	envars := map[string]string{}
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok {
			envars[key] = value
		}
	}
	return envars
}
