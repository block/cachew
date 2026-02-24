package reaper_test

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/reaper"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	return ctx
}

func TestStartReapsZombies(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("zombie reaping not applicable on Windows")
	}
	if os.Getpid() == 1 {
		t.Skip("test assumes we are not PID 1")
	}

	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()

	reaper.StartForTest(ctx)

	// Create a child process that exits immediately. We deliberately
	// don't call cmd.Wait(), so the exited process becomes a zombie
	// that the reaper should collect.
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err := cmd.Start()
	assert.NoError(t, err)

	pid := cmd.Process.Pid

	// Give the child time to exit and then reap it.
	time.Sleep(200 * time.Millisecond)
	reaper.Reap(ctx)

	// Verify the zombie was reaped: waitpid should return nothing.
	var status syscall.WaitStatus
	wpid, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
	assert.True(t, wpid <= 0 || err != nil, "expected zombie to have been reaped")
}

func TestStartSkipsWhenNotPID1(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("unexpectedly running as PID 1")
	}

	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()

	// Should return immediately without starting a goroutine.
	reaper.Start(ctx)
}
