package logs

import (
	"context"
	"log/slog"
	"net/http"
)

var global = slog.Default()

func defaultLogger() {
	slog.Info("message") // want `default logger should not be used`
}

func globalLogger() {
	global.Info("message") // want `global logger should not be used`
}

func withContext(ctx context.Context, logger *slog.Logger) {
	logger.Info("message") // want `InfoContext should be used instead`
	logger.InfoContext(ctx, "message")
	func() {
		logger.Warn("message") // want `WarnContext should be used instead`
	}()
}

func withRequest(r *http.Request, logger *slog.Logger) {
	logger.Error("message") // want `ErrorContext should be used instead`
}

func withoutContext(logger *slog.Logger) {
	logger.Info("message")
	logger.With("key", "value").Debug("message")
}
