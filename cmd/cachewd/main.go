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

	"github.com/block/cachew/internal/config"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

var cli struct {
	Schema bool `help:"Print the configuration file schema." xor:"command"`

	Config          *os.File            `hcl:"-" help:"Configuration file path." placeholder:"PATH" required:"" default:"cachew.hcl"`
	Bind            string              `hcl:"bind" default:"127.0.0.1:8080" help:"Bind address for the server."`
	SchedulerConfig jobscheduler.Config `embed:"" prefix:"scheduler-"`
	LoggingConfig   logging.Config      `embed:"" prefix:"log-"`
}

func main() {
	kctx := kong.Parse(&cli, kong.DefaultEnvars("CACHEW"))

	ctx := context.Background()
	logger, ctx := logging.Configure(ctx, cli.LoggingConfig)

	switch {
	case cli.Schema:
		schema := config.Schema()
		slices.SortStableFunc(schema.Entries, func(a, b hcl.Entry) int {
			return strings.Compare(a.EntryKey(), b.EntryKey())
		})
		text, err := hcl.MarshalAST(schema)
		kctx.FatalIfErrorf(err)

		if fileInfo, err := os.Stdout.Stat(); err == nil && (fileInfo.Mode()&os.ModeCharDevice) != 0 {
			err = quick.Highlight(os.Stdout, string(text), "terraform", "terminal256", "monokai")
			kctx.FatalIfErrorf(err)
		} else {
			fmt.Printf("%s\n", text)
		}
		return
	}

	mux := http.NewServeMux()

	scheduler := jobscheduler.New(ctx, cli.SchedulerConfig)

	err := config.Load(ctx, cli.Config, scheduler, mux, parseEnvars())
	kctx.FatalIfErrorf(err)

	logger.InfoContext(ctx, "Starting cachewd", slog.String("bind", cli.Bind))

	server := &http.Server{
		Addr:              cli.Bind,
		Handler:           httputil.LoggingMiddleware(mux),
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
