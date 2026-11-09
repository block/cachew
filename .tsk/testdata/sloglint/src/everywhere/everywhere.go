package everywhere

import "log/slog"

func logs(logger *slog.Logger) {
	logger.Info("message") // want `InfoContext should be used instead`
	slog.Debug("message")  // want `DebugContext should be used instead`
	logger.Log(nil, slog.LevelInfo, "message")
}
