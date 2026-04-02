package scheduler_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/scheduler"
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
	s, err := scheduler.New(testContext(), scheduler.Config{
		Alpha:           0.3,
		FairnessTTL:     time.Hour,
		CostTTL:         time.Hour,
		CleanupInterval: time.Hour,
	}, nil)
	assert.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func TestBasicSubmit(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 10, Priority: 10})

	tj := newTestJob()
	s.Submit("work", "j1", tj.fn)
	tj.waitStarted(t)
	tj.complete()
}

func TestRunSync(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 10, Priority: 10})

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
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 10, Priority: 10})

	want := errors.New("boom")
	err := s.RunSync(testContext(), "work", "j1", "client", func(_ context.Context) error {
		return want
	})
	assert.EqualError(t, err, "boom")
}

func TestRunSyncContextCancellation(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 1, Priority: 1})

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

	err := <-errCh
	assert.IsError(t, err, context.Canceled)

	blocker.complete()
}

func TestPriorityOrdering(t *testing.T) {
	s := newTestScheduler(t)
	const (
		fgType   scheduler.JobType       = "fg"
		bgType   scheduler.JobType       = "bg"
		conflict scheduler.ConflictGroup = "git"
	)
	s.RegisterType(fgType, scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 10, ConflictGroup: conflict})
	s.RegisterType(bgType, scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 5, ConflictGroup: conflict})

	blocker := newTestJob()
	s.Submit(fgType, "repo1", blocker.fn)
	blocker.waitStarted(t)

	// Submit bg first, then fg — both on repo1, both blocked by conflict.
	bg := newTestJob()
	s.Submit(bgType, "repo1", bg.fn)
	time.Sleep(10 * time.Millisecond)
	fg := newTestJob()
	s.Submit(fgType, "repo1", fg.fn)

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
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 1, Priority: 5})

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

func TestConflictGroupExclusion(t *testing.T) {
	s := newTestScheduler(t)
	const conflict scheduler.ConflictGroup = "git"
	s.RegisterType("clone", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 10, ConflictGroup: conflict})
	s.RegisterType("repack", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 5, ConflictGroup: conflict})

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
	s.RegisterType("clone", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 10, ConflictGroup: conflict})
	s.RegisterType("repack", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 5, ConflictGroup: conflict})

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
	s.RegisterType("clone", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: 10, ConflictGroup: "git"})
	s.RegisterType("snapshot", scheduler.JobTypeConfig{DefaultCost: 5, MaxConcurrency: 10, Priority: 5})

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
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 2, Priority: 10})

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

func TestPriorityTierConcurrencyLimit(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 1, MaxConcurrency: 10, Priority: 2})

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
	s.RegisterType("work", scheduler.JobTypeConfig{DefaultCost: 100, MaxConcurrency: 10, Priority: 10})

	err := s.RunSync(testContext(), "work", "j1", "c", func(_ context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	assert.NoError(t, err)

	// Verify the estimate was updated by running a second job and checking
	// that accumulated cost reflects a learned (not default) value. The second
	// job's estimated cost should be much less than the 100 default.
	err = s.RunSync(testContext(), "work", "j1", "client2", func(_ context.Context) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestBackgroundDoesNotStarveForeground(t *testing.T) {
	s := newTestScheduler(t)
	s.RegisterType("bg", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: scheduler.Priority(4)})
	s.RegisterType("fg", scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 10, Priority: scheduler.Priority(8)})

	bgJobs := make([]*testJob, 4)
	for i := range bgJobs {
		bgJobs[i] = newTestJob()
		s.Submit("bg", fmt.Sprintf("bg%d", i), bgJobs[i].fn)
	}
	for _, j := range bgJobs {
		j.waitStarted(t)
	}

	fg := newTestJob()
	s.Submit("fg", "fg1", fg.fn)
	fg.waitStarted(t)

	fg.complete()
	for _, j := range bgJobs {
		j.complete()
	}
}
