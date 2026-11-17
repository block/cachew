package snapshot_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

// TestPipeLifecycleNoDeadlock verifies the os.Pipe pattern in Create, StreamTo and Extract can't deadlock.
//
// StdoutPipe keeps the read end in the parent until Wait, so an early-exiting downstream never SIGPIPEs the
// upstream. Closing both os.Pipe ends in the parent after starting the children lets SIGPIPE through.
func TestPipeLifecycleNoDeadlock(t *testing.T) {
	t.Run("StdoutPipeDeadlocks", func(t *testing.T) {
		// The broken pattern: StdoutPipe holds the read end, so the upstream never gets SIGPIPE.
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
		// The fixed pattern: the parent closes both ends after starting children, so the upstream gets SIGPIPE.
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
