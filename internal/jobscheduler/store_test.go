package jobscheduler_test

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func TestScheduleStoreRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scheduler.db")
	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	defer store.Close()

	_, found, err := store.GetLastRun("key1")
	assert.NoError(t, err)
	assert.False(t, found)

	now := time.Now().Truncate(time.Second)
	assert.NoError(t, store.SetLastRun("key1", now))

	got, found, err := store.GetLastRun("key1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, now, got.Truncate(time.Second))
}

func TestScheduleStoreMultipleKeys(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scheduler.db")
	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	defer store.Close()

	t1 := time.Now().Add(-time.Hour)
	t2 := time.Now().Add(-2 * time.Hour)
	assert.NoError(t, store.SetLastRun("a", t1))
	assert.NoError(t, store.SetLastRun("b", t2))

	gotA, found, err := store.GetLastRun("a")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, t1.Truncate(time.Nanosecond), gotA.Truncate(time.Nanosecond))

	gotB, found, err := store.GetLastRun("b")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, t2.Truncate(time.Nanosecond), gotB.Truncate(time.Nanosecond))
}

func TestScheduleStorePersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scheduler.db")

	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)

	now := time.Now()
	assert.NoError(t, store.SetLastRun("key1", now))
	assert.NoError(t, store.Close())

	store2, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	defer store2.Close()

	got, found, err := store2.GetLastRun("key1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, now.Truncate(time.Nanosecond), got.Truncate(time.Nanosecond))
}

func TestScheduleStoreInvalidPath(t *testing.T) {
	_, err := jobscheduler.NewScheduleStore(filepath.Join(t.TempDir(), "nonexistent", "deep", "path", "scheduler.db"))
	assert.Error(t, err)
}

func TestPeriodicJobDelaysWhenRecentlyRun(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "scheduler.db")

	// Seed the store with a recent run time, then close it so the scheduler can open it.
	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	assert.NoError(t, store.SetLastRun("periodic:queue1", time.Now()))
	assert.NoError(t, store.Close())

	scheduler, err := jobscheduler.New(ctx, jobscheduler.Config{Concurrency: 2, SchedulerDB: dbPath})
	assert.NoError(t, err)
	defer scheduler.Close()

	var executed atomic.Bool
	scheduler.SubmitPeriodicJob("queue1", "periodic", 5*time.Second, func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	// The job should NOT have run within 200ms because interval is 5s and it "just ran".
	time.Sleep(200 * time.Millisecond)
	assert.False(t, executed.Load(), "job should be delayed because it ran recently")
}

func TestPeriodicJobRunsImmediatelyWhenNeverRun(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "scheduler.db")
	scheduler, err := jobscheduler.New(ctx, jobscheduler.Config{Concurrency: 2, SchedulerDB: dbPath})
	assert.NoError(t, err)
	defer scheduler.Close()

	var executed atomic.Bool
	scheduler.SubmitPeriodicJob("queue1", "periodic", 5*time.Second, func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "job should run immediately when no prior run recorded")
}

func TestPeriodicJobRunsImmediatelyWhenIntervalElapsed(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "scheduler.db")

	// Seed the store with a run time long ago, then close it.
	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	assert.NoError(t, store.SetLastRun("periodic:queue1", time.Now().Add(-10*time.Second)))
	assert.NoError(t, store.Close())

	scheduler, err := jobscheduler.New(ctx, jobscheduler.Config{Concurrency: 2, SchedulerDB: dbPath})
	assert.NoError(t, err)
	defer scheduler.Close()

	var executed atomic.Bool
	scheduler.SubmitPeriodicJob("queue1", "periodic", 5*time.Second, func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "job should run immediately when interval has elapsed")
}

func TestPeriodicJobRecordsLastRun(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	dbPath := filepath.Join(t.TempDir(), "scheduler.db")
	scheduler, err := jobscheduler.New(ctx, jobscheduler.Config{Concurrency: 2, SchedulerDB: dbPath})
	assert.NoError(t, err)

	var executed atomic.Bool
	before := time.Now()
	scheduler.SubmitPeriodicJob("queue1", "periodic", 5*time.Second, func(_ context.Context) error {
		executed.Store(true)
		return nil
	})

	eventually(t, time.Second, executed.Load, "job should execute")

	// Give a moment for the store write to complete.
	time.Sleep(50 * time.Millisecond)

	// Close scheduler to release the DB, then open a new store to check.
	assert.NoError(t, scheduler.Close())

	store, err := jobscheduler.NewScheduleStore(dbPath)
	assert.NoError(t, err)
	defer store.Close()

	lastRun, found, err := store.GetLastRun("periodic:queue1")
	assert.NoError(t, err)
	assert.True(t, found, "last run should be recorded")
	assert.True(t, !lastRun.Before(before), "last run should be at or after test start")
}
