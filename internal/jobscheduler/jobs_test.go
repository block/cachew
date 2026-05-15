package jobscheduler_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func newTestScheduler(ctx context.Context, t *testing.T, config jobscheduler.Config) jobscheduler.Scheduler {
	t.Helper()
	s, err := jobscheduler.New(ctx, config)
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	msg := "condition not met within timeout"
	if len(msgAndArgs) > 0 {
		if format, ok := msgAndArgs[0].(string); ok {
			msg = fmt.Sprintf(format, msgAndArgs[1:]...)
		}
	}
	t.Fatal(msg)
}

func TestJobSchedulerBasic(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var executed atomic.Bool
	scheduler.Submit("queue1", "job1", func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "job should execute")
}

func TestJobSchedulerConcurrency(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	concurrency := 4
	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: concurrency})

	var (
		running       atomic.Int32
		maxConcurrent atomic.Int32
		jobsCompleted atomic.Int32
	)

	jobCount := 20
	for i := range jobCount {
		queueID := fmt.Sprintf("queue%d", i)
		jobID := fmt.Sprintf("job%d", i)
		scheduler.Submit(queueID, jobID, func(_ context.Context) error {
			current := running.Add(1)
			defer running.Add(-1)

			for {
				maxVal := maxConcurrent.Load()
				if current <= maxVal {
					break
				}
				if maxConcurrent.CompareAndSwap(maxVal, current) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
			jobsCompleted.Add(1)
			return nil
		})
	}

	eventually(t, 5*time.Second, func() bool {
		return jobsCompleted.Load() == int32(jobCount)
	}, "all jobs should complete")

	assert.True(t, maxConcurrent.Load() <= int32(concurrency),
		"max concurrent jobs (%d) should not exceed configured concurrency (%d)",
		maxConcurrent.Load(), concurrency)
}

func TestJobSchedulerQueueIsolation(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 4})

	var (
		queue1Running       atomic.Int32
		queue2Running       atomic.Int32
		maxQueue1Concurrent atomic.Int32
		maxQueue2Concurrent atomic.Int32
	)

	updateMax := func(running *atomic.Int32, maxVal *atomic.Int32) {
		current := running.Load()
		for {
			maxCurrent := maxVal.Load()
			if current <= maxCurrent {
				break
			}
			if maxVal.CompareAndSwap(maxCurrent, current) {
				break
			}
		}
	}

	jobID := "job1"
	scheduler.Submit("queue1", jobID, func(_ context.Context) error {
		queue1Running.Add(1)
		defer queue1Running.Add(-1)
		updateMax(&queue1Running, &maxQueue1Concurrent)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	scheduler.Submit("queue2", jobID, func(_ context.Context) error {
		queue2Running.Add(1)
		defer queue2Running.Add(-1)
		updateMax(&queue2Running, &maxQueue2Concurrent)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), maxQueue1Concurrent.Load(),
		"queue1 should never run more than 1 job concurrently")
	assert.Equal(t, int32(1), maxQueue2Concurrent.Load(),
		"queue2 should never run more than 1 job concurrently")
}

func TestJobSchedulerJobOrdering(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 4})

	var (
		mu    sync.Mutex
		order []int
	)

	for i := range 5 {
		queueID := fmt.Sprintf("queue%d", i)
		jobID := fmt.Sprintf("job%d", i)
		scheduler.Submit(queueID, jobID, func(_ context.Context) error {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			return nil
		})
	}

	eventually(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 5
	}, "all jobs should complete")

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 5, len(order), "should have 5 jobs executed")
	for i := range 5 {
		found := slices.Contains(order, i)
		assert.True(t, found, "should contain job %d", i)
	}
}

func TestJobSchedulerErrorHandling(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var (
		failingJobExecuted atomic.Bool
		job2Executed       atomic.Bool
	)

	scheduler.Submit("queue1", "failing-job", func(_ context.Context) error {
		failingJobExecuted.Store(true)
		return errors.New("intentional error")
	})

	scheduler.Submit("queue2", "job2", func(_ context.Context) error {
		job2Executed.Store(true)
		return nil
	})

	eventually(t, time.Second, func() bool {
		return failingJobExecuted.Load() && job2Executed.Load()
	}, "jobs should execute despite errors")
}

func TestJobSchedulerContextCancellation(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var jobStarted atomic.Bool

	scheduler.Submit("queue1", "job1", func(ctx context.Context) error {
		jobStarted.Store(true)
		<-ctx.Done()
		return ctx.Err()
	})

	eventually(t, time.Second, jobStarted.Load)

	cancel()

	time.Sleep(100 * time.Millisecond)
}

func TestJobSchedulerPeriodicJob(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var executionCount atomic.Int32

	scheduler.SubmitPeriodicJob("queue1", "periodic", 50*time.Millisecond, func(_ context.Context) error {
		executionCount.Add(1)
		return nil
	})

	eventually(t, 2*time.Second, func() bool {
		return executionCount.Load() >= 3
	}, "periodic job should execute multiple times")
}

func TestJobSchedulerPeriodicJobWithError(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var executionCount atomic.Int32

	scheduler.SubmitPeriodicJob("queue1", "periodic-error", 50*time.Millisecond, func(_ context.Context) error {
		executionCount.Add(1)
		return errors.New("intentional error")
	})

	eventually(t, 2*time.Second, func() bool {
		return executionCount.Load() >= 3
	}, "periodic job should continue executing even after errors")
}

// TestJobSchedulerPeriodicJobStopsOnCancel verifies that the periodic re-arm
// goroutine exits cleanly when the scheduler's context is cancelled. Without
// this, a SIGTERM would leave the re-arm goroutine to wake and submit to a
// dead scheduler — and on a real pod that goroutine dies with the process,
// causing the periodic job to skip a full interval after every restart.
func TestJobSchedulerPeriodicJobStopsOnCancel(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var executions atomic.Int32
	scheduler.SubmitPeriodicJob("queue1", "periodic", 50*time.Millisecond, func(_ context.Context) error {
		executions.Add(1)
		return nil
	})

	eventually(t, time.Second, func() bool { return executions.Load() >= 2 },
		"periodic job should fire at least twice before cancel")

	cancel()
	// Give workers and the re-arm goroutine time to observe the cancel.
	time.Sleep(150 * time.Millisecond)
	before := executions.Load()
	// Wait several intervals; no further executions should occur.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, before, executions.Load(),
		"periodic job should not fire after scheduler context is cancelled")
}

// TestJobSchedulerSubmitDroppedAfterShutdown verifies that submissions made
// after the scheduler has been shut down are silently dropped rather than
// accumulating in the queue (which would leak the closure capture forever).
func TestJobSchedulerSubmitDroppedAfterShutdown(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	cancel()
	// Wait for the scheduler to observe the cancel and set q.done.
	time.Sleep(50 * time.Millisecond)

	var executed atomic.Bool
	scheduler.Submit("queue1", "post-shutdown", func(_ context.Context) error {
		executed.Store(true)
		return nil
	})
	time.Sleep(100 * time.Millisecond)
	assert.False(t, executed.Load(), "submissions after shutdown should be dropped")
}

func TestJobSchedulerMultipleQueues(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 3})

	queues := []string{"queue1", "queue2", "queue3", "queue4", "queue5"}
	totalJobs := len(queues)

	var (
		completed       atomic.Int32
		mu              sync.Mutex
		queueExecutions = make(map[string]bool)
	)

	// Pre-populate the map before any Submit so the worker goroutines that
	// later write to it under mu don't race with the main goroutine on map
	// writes. scheduler.Submit synchronises internally (sync.Cond+sync.Mutex),
	// which establishes happens-before from these writes to the eventual
	// worker reads/writes.
	for _, queue := range queues {
		queueExecutions[queue] = false
	}

	for _, queue := range queues {
		jobID := "job1"
		scheduler.Submit(queue, jobID, func(_ context.Context) error {
			mu.Lock()
			queueExecutions[queue] = true
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			completed.Add(1)
			return nil
		})
	}

	eventually(t, 5*time.Second, func() bool {
		return completed.Load() == int32(totalJobs)
	}, "all queue jobs should complete")

	mu.Lock()
	defer mu.Unlock()
	for queue, executed := range queueExecutions {
		assert.True(t, executed, "queue %s should have executed", queue)
	}
}

func TestJobSchedulerHighConcurrency(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: 50})

	jobCount := 100
	var completed atomic.Int32

	for i := range jobCount {
		queueID := fmt.Sprintf("queue%d", i)
		jobID := fmt.Sprintf("job%d", i)
		scheduler.Submit(queueID, jobID, func(_ context.Context) error {
			time.Sleep(time.Millisecond)
			completed.Add(1)
			return nil
		})
	}

	eventually(t, 5*time.Second, func() bool {
		return completed.Load() == int32(jobCount)
	}, "all jobs should complete")
}

func FuzzJobScheduler(f *testing.F) {
	// Seed corpus with various combinations
	f.Add(uint8(1), uint8(5), uint8(3))
	f.Add(uint8(4), uint8(10), uint8(5))
	f.Add(uint8(10), uint8(20), uint8(8))
	f.Add(uint8(2), uint8(100), uint8(10))

	f.Fuzz(func(t *testing.T, concurrency uint8, jobCount uint8, queueCount uint8) {
		// Constrain inputs to reasonable ranges
		if concurrency == 0 {
			concurrency = 1
		}
		if concurrency > 20 {
			concurrency = 20
		}
		if jobCount == 0 {
			return
		}
		if jobCount > 200 {
			jobCount = 200
		}
		if queueCount == 0 {
			queueCount = 1
		}
		if queueCount > 50 {
			queueCount = 50
		}

		_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		scheduler := newTestScheduler(ctx, t, jobscheduler.Config{Concurrency: int(concurrency)})

		var (
			completed     atomic.Int32
			maxConcurrent atomic.Int32
			running       atomic.Int32
			mu            sync.Mutex
			executions    = make(map[string][]int)
		)

		for i := range int(jobCount) {
			queueID := fmt.Sprintf("queue%d", i%int(queueCount))
			jobID := fmt.Sprintf("job%d", i)
			jobIdx := i

			mu.Lock()
			if executions[queueID] == nil {
				executions[queueID] = []int{}
			}
			mu.Unlock()

			scheduler.Submit(queueID, jobID, func(_ context.Context) error {
				current := running.Add(1)
				defer running.Add(-1)

				// Track max concurrency
				for {
					maxVal := maxConcurrent.Load()
					if current <= maxVal {
						break
					}
					if maxConcurrent.CompareAndSwap(maxVal, current) {
						break
					}
				}

				// Record execution order for this queue
				mu.Lock()
				executions[queueID] = append(executions[queueID], jobIdx)
				mu.Unlock()

				// Simulate some work with variable duration
				time.Sleep(time.Microsecond * time.Duration(jobIdx%10))

				completed.Add(1)
				return nil
			})
		}

		// Wait for all jobs to complete
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if completed.Load() == int32(jobCount) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Verify all jobs completed
		assert.Equal(t, int32(jobCount), completed.Load(), "all jobs should complete")

		// Verify concurrency limit was respected
		assert.True(t, maxConcurrent.Load() <= int32(concurrency),
			"concurrency limit exceeded: max %d, limit %d", maxConcurrent.Load(), concurrency)

		// Verify queue isolation: jobs within same queue ran in order
		mu.Lock()
		defer mu.Unlock()
		for queue, jobs := range executions {
			if len(jobs) <= 1 {
				continue
			}
			// Check that jobs are in strictly increasing order
			for i := 1; i < len(jobs); i++ {
				assert.True(t, jobs[i] > jobs[i-1],
					"queue %s: jobs out of order at position %d: %v", queue, i, jobs)
			}
		}
	})
}

func TestJobSchedulerCloneConcurrencyLimit(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newTestScheduler(ctx, t, jobscheduler.Config{
		Concurrency:         8,
		MaxCloneConcurrency: 2,
	})

	var (
		cloneRunning       atomic.Int32
		maxCloneConcurrent atomic.Int32
		fetchCompleted     atomic.Int32
		cloneCompleted     atomic.Int32
		allClonesBlocking  sync.WaitGroup
	)

	// Submit 4 clone jobs that block until released.
	allClonesBlocking.Add(1)
	for i := range 4 {
		queue := fmt.Sprintf("repo%d", i)
		scheduler.Submit(queue, "git-clone", func(_ context.Context) error {
			current := cloneRunning.Add(1)
			defer cloneRunning.Add(-1)
			for {
				maxVal := maxCloneConcurrent.Load()
				if current <= maxVal {
					break
				}
				if maxCloneConcurrent.CompareAndSwap(maxVal, current) {
					break
				}
			}
			allClonesBlocking.Wait()
			cloneCompleted.Add(1)
			return nil
		})
	}

	// Submit fetch jobs on different queues — these should NOT be blocked by clone limit.
	for i := range 4 {
		queue := fmt.Sprintf("fetch-repo%d", i)
		scheduler.Submit(queue, "git-fetch", func(_ context.Context) error {
			fetchCompleted.Add(1)
			return nil
		})
	}

	// Fetch jobs should complete even while clone jobs are blocking workers.
	eventually(t, 2*time.Second, func() bool {
		return fetchCompleted.Load() == 4
	}, "fetch jobs should complete without being blocked by clone limit")

	// Clone concurrency should be capped at 2.
	assert.True(t, maxCloneConcurrent.Load() <= 2,
		"max concurrent clones (%d) should not exceed MaxCloneConcurrency (2)",
		maxCloneConcurrent.Load())

	// Release clone jobs.
	allClonesBlocking.Done()

	eventually(t, 2*time.Second, func() bool {
		return cloneCompleted.Load() == 4
	}, "all clone jobs should eventually complete")
}
