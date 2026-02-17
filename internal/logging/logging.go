// Package logging provides logging configuration and utility functions.
package logging

import (
	"context"
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

type Config struct {
	JSON  bool              `hcl:"json,optional" help:"Enable JSON logging."`
	Level slog.Level        `hcl:"level" help:"Set the logging level." default:"info"`
	Remap map[string]string `hcl:"remap,optional" help:"Remap field names from old to new (e.g., msg=message, time=timestamp)."`
}

type logKey struct{}

func Configure(ctx context.Context, config Config) (*slog.Logger, context.Context) {
	var handler slog.Handler
	if config.JSON {
		options := &slog.HandlerOptions{Level: config.Level}
		if len(config.Remap) > 0 {
			options.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) > 0 {
					return a
				}
				if newName, ok := config.Remap[a.Key]; ok {
					a.Key = newName
				}
				return a
			}
		}
		handler = &messageHandler{inner: slog.NewJSONHandler(os.Stdout, options)}
	} else {
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level: config.Level,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey && len(groups) == 0 {
					return slog.Attr{}
				}
				return a
			},
		})
	}
	logger := slog.New(handler)
	return logger, context.WithValue(ctx, logKey{}, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	logger, ok := ctx.Value(logKey{}).(*slog.Logger)
	if !ok {
		panic("no logger in context")
	}
	return logger
}

// ContextWithLogger returns a new context with the given logger.
func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, logKey{}, logger)
}
