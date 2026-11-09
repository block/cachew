package defaultonly

import "log/slog"

var global = slog.Default()

func logs() {
	slog.Info("message") // want `default logger should not be used`
	global.Info("message")
}
