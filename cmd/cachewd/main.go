package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/alecthomas/hcl/v2"
	"github.com/alecthomas/kong"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/config"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
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
	SchedulerConfig jobscheduler.Config `hcl:"scheduler,block"`
	LoggingConfig   logging.Config      `hcl:"log,block"`
	MetricsConfig   metrics.Config      `hcl:"metrics,block"`
	GitCloneConfig  gitclone.Config     `hcl:"git-clone,block"`
	GithubAppConfig githubapp.Config    `embed:"" hcl:"github-app,block,optional" prefix:"github-app-"`
}

type CLI struct {
	Schema bool `help:"Print the configuration file schema." xor:"command"`

	Config *os.File `hcl:"-" help:"Configuration file path." required:"" default:"cachew.hcl"`
}

func main() {
	var cli CLI
	kctx := kong.Parse(&cli, kong.DefaultEnvars("CACHEW"))

	defer cli.Config.Close()
	ast, err := hcl.Parse(cli.Config)
	kctx.FatalIfErrorf(err)

	globalConfigHCL, providersConfigHCL := config.Split[GlobalConfig](ast)

	// Load global config.
	var globalConfig GlobalConfig
	globalSchema, err := hcl.Schema(&globalConfig)
	kctx.FatalIfErrorf(err)
	config.InjectEnvars(globalSchema, globalConfigHCL, "CACHEW", parseEnvars())
	err = hcl.UnmarshalAST(globalConfigHCL, &globalConfig, hcl.HydratedImplicitBlocks(true))
	kctx.FatalIfErrorf(err)

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, globalConfig.LoggingConfig)

	// Start initialising
	tokenManagerProvider := githubapp.NewTokenManagerProvider(globalConfig.GithubAppConfig, logger)
	managerProvider := gitclone.NewManagerProvider(ctx, globalConfig.GitCloneConfig, func() (gitclone.CredentialProvider, error) {
		return tokenManagerProvider()
	})

	scheduler := jobscheduler.New(ctx, globalConfig.SchedulerConfig)

	cr, sr := newRegistries(scheduler, managerProvider, tokenManagerProvider)

	// Commands
	switch { //nolint:gocritic
	case cli.Schema:
		printSchema(kctx, cr, sr)
		return
	}

	mux, err := newMux(ctx, cr, sr, providersConfigHCL)
	kctx.FatalIfErrorf(err)

	metricsClient, err := metrics.New(ctx, globalConfig.MetricsConfig)
	kctx.FatalIfErrorf(err, "failed to create metrics client")
	defer func() {
		if err := metricsClient.Close(); err != nil {
			logger.ErrorContext(ctx, "failed to close metrics client", "error", err)
		}
	}()

	if err := metricsClient.ServeMetrics(ctx); err != nil {
		kctx.FatalIfErrorf(err, "failed to start metrics server")
	}

	logger.InfoContext(ctx, "Starting cachewd", slog.String("bind", globalConfig.Bind))

	server := newServer(ctx, mux, globalConfig.Bind, globalConfig.MetricsConfig)
	err = server.ListenAndServe()
	kctx.FatalIfErrorf(err)
}

func newRegistries(scheduler jobscheduler.Scheduler, cloneManagerProvider gitclone.ManagerProvider, tokenManagerProvider githubapp.TokenManagerProvider) (*cache.Registry, *strategy.Registry) {
	cr := cache.NewRegistry()
	cache.RegisterMemory(cr)
	cache.RegisterDisk(cr)
	cache.RegisterS3(cr)

	sr := strategy.NewRegistry()
	strategy.RegisterAPIV1(sr)
	strategy.RegisterArtifactory(sr)
	strategy.RegisterGitHubReleases(sr, tokenManagerProvider)
	strategy.RegisterHermit(sr)
	strategy.RegisterHost(sr)
	git.Register(sr, scheduler, cloneManagerProvider, tokenManagerProvider)
	gomod.Register(sr, cloneManagerProvider)

	return cr, sr
}

func printSchema(kctx *kong.Context, cr *cache.Registry, sr *strategy.Registry) {
	schema := config.Schema[GlobalConfig](cr, sr)
	text, err := hcl.MarshalAST(schema)
	kctx.FatalIfErrorf(err)

	if fileInfo, err := os.Stdout.Stat(); err == nil && (fileInfo.Mode()&os.ModeCharDevice) != 0 {
		err = quick.Highlight(os.Stdout, string(text), "terraform", "terminal256", "solarized")
		kctx.FatalIfErrorf(err)
	} else {
		fmt.Printf("%s\n", text) //nolint:forbidigo
	}
}

func newMux(ctx context.Context, cr *cache.Registry, sr *strategy.Registry, providersConfigHCL *hcl.AST) (*http.ServeMux, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /_liveness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	mux.HandleFunc("GET /_readiness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) //nolint:errcheck
	})

	if err := config.Load(ctx, cr, sr, providersConfigHCL, mux, parseEnvars()); err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	return mux, nil
}

// extractPathPrefix extracts the strategy name, path prefix from a request path.
// Examples: /git/... -> "git", /gomod/... -> "gomod", /api/v1/... -> "api".
func extractPathPrefix(path string) string {
	if path == "" || path == "/" {
		return ""
	}
	trimmed := strings.TrimPrefix(path, "/")
	prefix, _, _ := strings.Cut(trimmed, "/")
	return prefix
}

func newServer(ctx context.Context, mux *http.ServeMux, bind string, metricsConfig metrics.Config) *http.Server {
	logger := logging.FromContext(ctx)

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labeler, _ := otelhttp.LabelerFromContext(r.Context())
		labeler.Add(attribute.String("cachew.http.path.prefix", extractPathPrefix(r.URL.Path)))
		mux.ServeHTTP(w, r)
	})

	// Add standard otelhttp middleware
	handler = otelhttp.NewMiddleware(metricsConfig.ServiceName,
		otelhttp.WithMeterProvider(otel.GetMeterProvider()),
		otelhttp.WithTracerProvider(otel.GetTracerProvider()),
	)(handler)

	handler = httputil.LoggingMiddleware(handler)

	return &http.Server{
		Addr:              bind,
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
