package snapshot_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

// TestPipeLifecycleNoDeadlock verifies the pipe pattern used by Create,
// StreamTo, and Extract does not deadlock when the downstream process exits
// while the upstream is still producing data.
//
// Background: exec.Cmd.StdoutPipe() retains the pipe read end in the parent
// until Wait() runs closeAfterWait. If the downstream exits early, the
// upstream cannot receive SIGPIPE (the parent still holds the read end), so
// it blocks on pipe write and Wait() deadlocks.
//
// The fix (used in Create/StreamTo/Extract): use os.Pipe() manually and close
// both ends in the parent immediately after starting child processes. This
// ensures that when the downstream exits, the upstream receives SIGPIPE.
func TestPipeLifecycleNoDeadlock(t *testing.T) {
	t.Run("StdoutPipeDeadlocks", func(t *testing.T) {
		// Demonstrates the broken pattern: StdoutPipe holds the read end,
		// preventing SIGPIPE delivery to the upstream.
		upstream := exec.Command("yes")
		downstream := exec.Command("head", "-c", "100")

		pipeRead, _ := upstream.StdoutPipe()
		downstream.Stdin = pipeRead

		assert.NoError(t, upstream.Start())
		assert.NoError(t, downstream.Start())

		done := make(chan struct{})
		go func() {
			_ = upstream.Wait()
			_ = downstream.Wait()
			close(done)
		}()

		select {
		case <-done:
			t.Fatal("expected StdoutPipe pattern to deadlock, but it completed")
		case <-time.After(2 * time.Second):
			// Deadlock confirmed — clean up.
			upstream.Process.Kill()   //nolint:errcheck
			downstream.Process.Kill() //nolint:errcheck
			<-done
		}
	})

	t.Run("ManualPipeWorks", func(t *testing.T) {
		// The fixed pattern: parent closes both pipe ends after starting
		// children, so the upstream gets SIGPIPE when downstream exits.
		upstream := exec.Command("yes")
		downstream := exec.Command("head", "-c", "100")

		pr, pw, err := os.Pipe()
		assert.NoError(t, err)

		upstream.Stdout = pw
		downstream.Stdin = pr

		assert.NoError(t, upstream.Start())
		pw.Close() //nolint:errcheck,gosec

		assert.NoError(t, downstream.Start())
		pr.Close() //nolint:errcheck,gosec

		done := make(chan struct{})
		go func() {
			_ = upstream.Wait()
			_ = downstream.Wait()
			close(done)
		}()

		select {
		case <-done:
			// OK: no deadlock.
		case <-time.After(5 * time.Second):
			upstream.Process.Kill()   //nolint:errcheck
			downstream.Process.Kill() //nolint:errcheck
			<-done
			t.Fatal("manual pipe pattern deadlocked unexpectedly")
		}
	})
}
