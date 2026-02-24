// Package reaper provides a background zombie process reaper.
//
// When a Go process runs as PID 1 (e.g. inside a container), it inherits
// orphaned child processes. If those children exit without being waited on,
// they accumulate as zombies. This package periodically calls waitpid(-1)
// with WNOHANG to reap any such zombies.
package reaper

import (
	"context"
	"log/slog"
	"os"
	"syscall"
	"time"

	"github.com/block/cachew/internal/logging"
)

// Start launches a background goroutine that reaps zombie child processes.
// It only activates when the current process is PID 1. The goroutine exits
// when ctx is cancelled.
func Start(ctx context.Context) {
	logger := logging.FromContext(ctx)
	if os.Getpid() != 1 {
		logger.DebugContext(ctx, "Zombie reaper not needed, not running as PID 1")
		return
	}
	logger.InfoContext(ctx, "Running as PID 1, starting zombie reaper")
	go run(ctx)
}

// StartForTest is like Start but skips the PID 1 check.
func StartForTest(ctx context.Context) {
	go run(ctx)
}

// Reap collects all currently-zombie child processes without blocking.
func Reap(ctx context.Context) {
	logger := logging.FromContext(ctx)
	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
		logger.DebugContext(ctx, "Reaped zombie process", slog.Int("pid", pid), slog.Int("status", status.ExitStatus()))
	}
}

func run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			Reap(ctx)
		}
	}
}
