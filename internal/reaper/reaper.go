// Package reaper provides a background zombie process reaper.
//
// A Go process running as PID 1, e.g. in a container, inherits orphans that become zombies if not waited on.
// This package reaps them by periodically calling waitpid(-1) with WNOHANG.
package reaper

import (
	"context"
	"os"
	"syscall"
	"time"

	"github.com/block/cachew/internal/logging"
)

// Start launches a goroutine that reaps zombie child processes until ctx is cancelled.
//
// It only activates when the current process is PID 1.
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
		logger.DebugContext(ctx, "Reaped zombie process", "pid", pid, "status", status.ExitStatus())
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
