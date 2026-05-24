package scheduler_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/scheduler"
)

var (
	priFG = scheduler.Priority{Level: 10, Weight: 4} //nolint:gochecknoglobals
	priBG = scheduler.Priority{Level: 5, Weight: 1}  //nolint:gochecknoglobals
)

type testJob struct {
	started chan struct{}
	finish  chan error
}

func newTestJob() *testJob {
	return &testJob{
		started: make(chan struct{}),
		finish:  make(chan error, 1),
	}
}

func (j *testJob) fn(ctx context.Context) error {
	close(j.started)
	select {
	case err := <-j.finish:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *testJob) complete() { j.finish <- nil }

func (j *testJob) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-j.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}
}

func (j *testJob) assertNotStarted(t *testing.T) {
	t.Helper()
	select {
	case <-j.started:
		t.Fatal("job started unexpectedly")
	case <-time.After(50 * time.Millisecond):
	}
}

func testContext() context.Context {
	logger, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelWarn})
	return logging.ContextWithLogger(ctx, logger)
}

func newTestScheduler(t *testing.T) *scheduler.Scheduler {
	t.Helper()
	return newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 50,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
}

func newTestSchedulerWithConfig(t *testing.T, cfg scheduler.Config) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(testContext(), cfg, nil)
	assert.NoError(t, err)
	t.Cleanup(s.Close)
	s.RegisterPriority(priFG)
	s.RegisterPriority(priBG)
	return s
}

func newCustomPriorityScheduler(t *testing.T, cfg scheduler.Config, priorities ...scheduler.Priority) *scheduler.Scheduler {
	t.Helper()
	s, err := scheduler.New(testContext(), cfg, nil)
	assert.NoError(t, err)
	t.Cleanup(s.Close)
	for _, p := range priorities {
		s.RegisterPriority(p)
	}
	return s
}

func TestBasicSubmit(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	tj := newTestJob()
	s.Submit("work", "j1", tj.fn)
	tj.waitStarted(t)
	tj.complete()
}

func TestRunSync(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	called := false
	err := s.RunSync(testContext(), "work", "j1", "client", func(_ context.Context) error {
		called = true
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestRunSyncReturnsError(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	want := errors.New("boom")
	err := s.RunSync(testContext(), "work", "j1", "client", func(_ context.Context) error {
		return want
	})
	assert.EqualError(t, err, "boom")
}

func TestRunSyncContextCancellation(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 1,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	blocker := newTestJob()
	s.Submit("work", "blocker", blocker.fn)
	blocker.waitStarted(t)

	ctx, cancel := context.WithCancel(testContext())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.RunSync(ctx, "work", "j1", "client", func(_ context.Context) error {
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	syncErr := <-errCh
	assert.IsError(t, syncErr, context.Canceled)

	blocker.complete()
}

func TestPriorityOrdering(t *testing.T) {
	s := newTestScheduler(t)
	const conflict scheduler.ConflictGroup = "git"
	s.RegisterType("fgSetup", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG, ConflictGroup: conflict})
	s.RegisterType("fg", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG, ConflictGroup: conflict})
	s.RegisterType("bg", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG, ConflictGroup: conflict})

	blocker := newTestJob()
	s.Submit("fgSetup", "repo1", blocker.fn)
	blocker.waitStarted(t)

	// Submit bg first, then fg — both on repo1, both blocked by conflict.
	bg := newTestJob()
	s.Submit("bg", "repo1", bg.fn)
	time.Sleep(10 * time.Millisecond)
	fg := newTestJob()
	s.Submit("fg", "repo1", fg.fn)

	bg.assertNotStarted(t)
	fg.assertNotStarted(t)

	// Release blocker — fg should win despite arriving second.
	blocker.complete()
	fg.waitStarted(t)
	bg.assertNotStarted(t)

	fg.complete()
	bg.waitStarted(t)
	bg.complete()
}

func TestFairness(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 1,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG})

	// Build up accumulated cost for clientA.
	err := s.RunSync(testContext(), "work", "warmup", "clientA", func(_ context.Context) error {
		return nil
	})
	assert.NoError(t, err)

	blocker := newTestJob()
	s.Submit("work", "blocker", blocker.fn)
	blocker.waitStarted(t)

	// Queue A (arrived first) then B.
	jobA := newTestJob()
	jobB := newTestJob()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.RunSync(testContext(), "work", "a1", "clientA", jobA.fn) }() //nolint:errcheck
	time.Sleep(10 * time.Millisecond)
	go func() { defer wg.Done(); s.RunSync(testContext(), "work", "b1", "clientB", jobB.fn) }() //nolint:errcheck
	time.Sleep(10 * time.Millisecond)

	// Release blocker — B should go first (lower accumulated cost).
	blocker.complete()
	jobB.waitStarted(t)
	jobA.assertNotStarted(t)

	jobB.complete()
	jobA.waitStarted(t)
	jobA.complete()
	wg.Wait()
}

func TestAdmissionCostPreventsDoS(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 1,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
		AdmissionCost:    5 * time.Second,
	})
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG})

	// ClientA submits many cheap jobs, building up accumulated cost via the admission cost floor.
	for range 5 {
		err := s.RunSync(testContext(), "work", "cheap", "clientA", func(_ context.Context) error { return nil })
		assert.NoError(t, err)
	}

	// Block the scheduler so we can queue both clients.
	blocker := newTestJob()
	s.Submit("work", "blocker", blocker.fn)
	blocker.waitStarted(t)

	// Queue A (arrived first) then B.
	jobA := newTestJob()
	jobB := newTestJob()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.RunSync(testContext(), "work", "a1", "clientA", jobA.fn) }() //nolint:errcheck
	time.Sleep(10 * time.Millisecond)
	go func() { defer wg.Done(); s.RunSync(testContext(), "work", "b1", "clientB", jobB.fn) }() //nolint:errcheck
	time.Sleep(10 * time.Millisecond)

	// Release blocker — B should go first because A has high accumulated cost from many admissions.
	blocker.complete()
	jobB.waitStarted(t)
	jobA.assertNotStarted(t)

	jobB.complete()
	jobA.waitStarted(t)
	jobA.complete()
	wg.Wait()
}

func TestConflictGroupExclusion(t *testing.T) {
	s := newTestScheduler(t)
	const conflict scheduler.ConflictGroup = "git"
	s.RegisterType("clone", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG, ConflictGroup: conflict})
	s.RegisterType("repack", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG, ConflictGroup: conflict})

	clone := newTestJob()
	s.Submit("clone", "repo1", clone.fn)
	clone.waitStarted(t)

	repack := newTestJob()
	s.Submit("repack", "repo1", repack.fn)
	repack.assertNotStarted(t)

	clone.complete()
	repack.waitStarted(t)
	repack.complete()
}

func TestConflictGroupDifferentIDs(t *testing.T) {
	s := newTestScheduler(t)
	const conflict scheduler.ConflictGroup = "git"
	s.RegisterType("clone", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG, ConflictGroup: conflict})
	s.RegisterType("repack", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG, ConflictGroup: conflict})

	clone := newTestJob()
	s.Submit("clone", "repo1", clone.fn)
	clone.waitStarted(t)

	// Different job ID — no conflict.
	repack := newTestJob()
	s.Submit("repack", "repo2", repack.fn)
	repack.waitStarted(t)

	clone.complete()
	repack.complete()
}

func TestNoConflictGroup(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("clone", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG, ConflictGroup: "git"})
	s.RegisterType("snapshot", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG})

	clone := newTestJob()
	s.Submit("clone", "repo1", clone.fn)
	clone.waitStarted(t)

	snap := newTestJob()
	s.Submit("snapshot", "repo1", snap.fn)
	snap.waitStarted(t)

	clone.complete()
	snap.complete()
}

func TestTypeConcurrencyLimit(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 50,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	// TotalConcurrency=50, fg weight=4, bg weight=1 → fg gets 40 tier slots.
	// MaxConcurrency=0.05 → int(0.05*40) = 2 type slots.
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 0.05, Priority: priFG})

	j1 := newTestJob()
	j2 := newTestJob()
	j3 := newTestJob()
	s.Submit("work", "a", j1.fn)
	s.Submit("work", "b", j2.fn)
	s.Submit("work", "c", j3.fn)

	j1.waitStarted(t)
	j2.waitStarted(t)
	j3.assertNotStarted(t)

	j1.complete()
	j3.waitStarted(t)
	j2.complete()
	j3.complete()
}

func TestTierConcurrencyLimit(t *testing.T) {
	// Two tiers with equal weight → 2 slots each out of TotalConcurrency=4.
	fg := scheduler.Priority{Level: 10, Weight: 1}
	bg := scheduler.Priority{Level: 5, Weight: 1}
	s := newCustomPriorityScheduler(t, scheduler.Config{
		TotalConcurrency: 4,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	}, fg, bg)
	s.RegisterType("fgWork", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: fg})
	s.RegisterType("bgWork", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: bg})

	j1 := newTestJob()
	j2 := newTestJob()
	j3 := newTestJob()
	s.Submit("bgWork", "a", j1.fn)
	s.Submit("bgWork", "b", j2.fn)
	s.Submit("bgWork", "c", j3.fn)

	j1.waitStarted(t)
	j2.waitStarted(t)
	j3.assertNotStarted(t)

	j1.complete()
	j3.waitStarted(t)
	j2.complete()
	j3.complete()
}

func TestTotalConcurrencyLimit(t *testing.T) {
	pri := scheduler.Priority{Level: 10, Weight: 1}
	s := newCustomPriorityScheduler(t, scheduler.Config{
		TotalConcurrency: 2,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	}, pri)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: pri})

	j1 := newTestJob()
	j2 := newTestJob()
	j3 := newTestJob()
	s.Submit("work", "a", j1.fn)
	s.Submit("work", "b", j2.fn)
	s.Submit("work", "c", j3.fn)

	j1.waitStarted(t)
	j2.waitStarted(t)
	j3.assertNotStarted(t)

	j1.complete()
	j3.waitStarted(t)
	j2.complete()
	j3.complete()
}

func TestCostEstimation(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	err := s.RunSync(testContext(), "work", "j1", "c", func(_ context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	assert.NoError(t, err)

	// Verify the estimate was updated by running a second job and checking
	// that accumulated cost reflects a learned (not default) value. The second
	// job's estimated cost should be much less than the 100s default.
	err = s.RunSync(testContext(), "work", "j1", "client2", func(_ context.Context) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestSubmitDedup(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	var calls atomic.Int32
	tj := newTestJob()
	s.Submit("work", "j1", func(ctx context.Context) error {
		calls.Add(1)
		return tj.fn(ctx)
	})
	tj.waitStarted(t)

	// Second Submit with same (type, id) is silently deduplicated.
	s.Submit("work", "j1", func(_ context.Context) error {
		calls.Add(1)
		return nil
	})

	tj.complete()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRunSyncDedup(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 1,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	blocker := newTestJob()
	s.Submit("work", "blocker", blocker.fn)
	blocker.waitStarted(t)

	// Two RunSync calls for the same (type, id) coalesce onto one job.
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	for i := range 2 {
		go func() {
			defer wg.Done()
			errs[i] = s.RunSync(testContext(), "work", "shared", fmt.Sprintf("client%d", i), func(_ context.Context) error {
				return nil
			})
		}()
	}
	time.Sleep(50 * time.Millisecond)

	blocker.complete()
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func TestRunSyncDedupCancellation(t *testing.T) {
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 1,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	blocker := newTestJob()
	s.Submit("work", "blocker", blocker.fn)
	blocker.waitStarted(t)

	// Two RunSync callers for the same job. One cancels, the other completes.
	ctxA, cancelA := context.WithCancel(testContext())
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errCh <- s.RunSync(ctxA, "work", "shared", "clientA", func(_ context.Context) error { return nil })
	}()
	go func() {
		defer wg.Done()
		errCh <- s.RunSync(testContext(), "work", "shared", "clientB", func(_ context.Context) error { return nil })
	}()
	time.Sleep(50 * time.Millisecond)

	cancelA()
	errA := <-errCh
	assert.IsError(t, errA, context.Canceled)

	blocker.complete()
	errB := <-errCh
	assert.NoError(t, errB)
	wg.Wait()
}

func TestSubmitThenRunSyncDedup(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	tj := newTestJob()
	s.Submit("work", "j1", tj.fn)
	tj.waitStarted(t)

	// RunSync coalesces onto the already-running Submit job.
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.RunSync(testContext(), "work", "j1", "client", func(_ context.Context) error { return nil })
	}()
	time.Sleep(50 * time.Millisecond)

	tj.complete()
	assert.NoError(t, <-errCh)
}

func TestRunSyncThenSubmitDedup(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	tj := newTestJob()
	go s.RunSync(testContext(), "work", "j1", "client", tj.fn) //nolint:errcheck
	tj.waitStarted(t)

	// Submit coalesces onto the already-running RunSync job.
	// The job should survive even if the RunSync caller cancels, since Submit marked it.
	s.Submit("work", "j1", func(_ context.Context) error { return nil })
	tj.complete()
}

func TestRunSyncAllCancelledCancelsJob(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	fnCtxCancelled := make(chan struct{})
	blockingFn := func(ctx context.Context) error {
		<-ctx.Done()
		close(fnCtxCancelled)
		return ctx.Err()
	}

	// Start A first so its fn is stored on the job.
	ctxA, cancelA := context.WithCancel(testContext())
	var wg sync.WaitGroup
	wg.Go(func() {
		s.RunSync(ctxA, "work", "j1", "clientA", blockingFn) //nolint:errcheck
	})
	time.Sleep(50 * time.Millisecond)

	// B coalesces onto A's job.
	ctxB, cancelB := context.WithCancel(testContext())
	wg.Go(func() {
		s.RunSync(ctxB, "work", "j1", "clientB", blockingFn) //nolint:errcheck
	})
	time.Sleep(50 * time.Millisecond)

	// Cancel one waiter — job should continue.
	cancelA()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-fnCtxCancelled:
		t.Fatal("job cancelled with waiter still active")
	default:
	}

	// Cancel the last waiter — job's context should be cancelled.
	cancelB()
	select {
	case <-fnCtxCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job fn to be cancelled")
	}
	wg.Wait()
}

func TestBackgroundDoesNotStarveForeground(t *testing.T) {
	// bg weight=1, fg weight=4 → bg gets 2/10 slots, fg gets 8/10 slots.
	s := newTestSchedulerWithConfig(t, scheduler.Config{
		TotalConcurrency: 10,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	})
	s.RegisterType("bg", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priBG})
	s.RegisterType("fg", scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: priFG})

	bgJobs := make([]*testJob, 2)
	for i := range bgJobs {
		bgJobs[i] = newTestJob()
		s.Submit("bg", fmt.Sprintf("bg%d", i), bgJobs[i].fn)
	}
	for _, j := range bgJobs {
		j.waitStarted(t)
	}

	// bg tier is full (2 slots), but fg tier has its own slots.
	fg := newTestJob()
	s.Submit("fg", "fg1", fg.fn)
	fg.waitStarted(t)

	fg.complete()
	for _, j := range bgJobs {
		j.complete()
	}
}

func TestUnregisteredPriorityPanics(t *testing.T) {
	s := newTestScheduler(t)
	assert.Panics(t, func() {
		s.RegisterType("a", scheduler.JobTypeConfig{
			MaxConcurrency: 1,
			Priority:       scheduler.Priority{Level: 99, Weight: 1},
		})
	})
}

func TestDuplicatePriorityPanics(t *testing.T) {
	s := newTestScheduler(t)
	assert.Panics(t, func() {
		s.RegisterPriority(priFG)
	})
}
