package jobscheduler_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func newAdapterScheduler(ctx context.Context, t *testing.T, config jobscheduler.Config) jobscheduler.Scheduler {
	t.Helper()
	s, err := jobscheduler.NewAdapter(ctx, config)
	assert.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAdapterBasicSubmit(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newAdapterScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var executed atomic.Bool
	s.Submit("queue1", "job1", func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "job should execute")
}

func TestAdapterQueueIsolation(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newAdapterScheduler(ctx, t, jobscheduler.Config{Concurrency: 4})

	var running atomic.Int32
	var violation atomic.Bool
	blocker := make(chan struct{})

	s.Submit("queue1", "job1", func(_ context.Context) error {
		running.Add(1)
		defer running.Add(-1)
		<-blocker
		return nil
	})
	// Same queue — should not run concurrently.
	s.Submit("queue1", "job2", func(_ context.Context) error {
		if running.Load() > 0 {
			violation.Store(true)
		}
		return nil
	})

	time.Sleep(100 * time.Millisecond)
	close(blocker)
	time.Sleep(100 * time.Millisecond)

	assert.False(t, violation.Load(), "same-queue jobs ran concurrently")
}

func TestAdapterWithQueuePrefix(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newAdapterScheduler(ctx, t, jobscheduler.Config{Concurrency: 4})
	prefixed := s.WithQueuePrefix("git")

	var executed atomic.Bool
	prefixed.Submit("repo1", "clone", func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "prefixed job should execute")
}

func TestAdapterCloneConcurrencyLimit(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newAdapterScheduler(ctx, t, jobscheduler.Config{Concurrency: 4, MaxCloneConcurrency: 2})

	var (
		running       atomic.Int32
		maxConcurrent atomic.Int32
		done          sync.WaitGroup
	)

	for i := range 6 {
		done.Add(1)
		queue := fmt.Sprintf("repo%d", i)
		s.Submit(queue, "clone", func(_ context.Context) error {
			defer done.Done()
			cur := running.Add(1)
			defer running.Add(-1)
			for {
				maxVal := maxConcurrent.Load()
				if cur <= maxVal {
					break
				}
				if maxConcurrent.CompareAndSwap(maxVal, cur) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}

	done.Wait()
	assert.True(t, maxConcurrent.Load() <= 2,
		"max concurrent clone jobs (%d) should not exceed 2", maxConcurrent.Load())
}

func TestAdapterPeriodicJob(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s := newAdapterScheduler(ctx, t, jobscheduler.Config{Concurrency: 2})

	var count atomic.Int32
	s.SubmitPeriodicJob("queue1", "periodic", 50*time.Millisecond, func(_ context.Context) error {
		count.Add(1)
		return nil
	})

	eventually(t, time.Second, func() bool { return count.Load() >= 3 },
		"periodic job should run at least 3 times")
}
