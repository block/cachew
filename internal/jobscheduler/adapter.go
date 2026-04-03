package jobscheduler

import (
	"context"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/scheduler"
)

const (
	// All jobs share a single conflict group so that same-queue jobs serialise,
	// matching the old scheduler's one-job-per-queue behaviour.
	adapterConflictGroup scheduler.ConflictGroup = "queue"

	// Clone-like jobs are collapsed into a single job type for concurrency
	// limiting, matching the old scheduler's MaxCloneConcurrency behaviour.
	cloneJobType scheduler.JobType = "clone"
)

// SchedulerAdapter wraps a *scheduler.Scheduler to implement the Scheduler
// interface, allowing it to be used as a drop-in replacement for
// RootScheduler.
type SchedulerAdapter struct {
	inner               *scheduler.Scheduler
	config              Config
	mu                  sync.Mutex
	registeredTypes     map[scheduler.JobType]bool
	cloneTypeRegistered bool
}

// NewAdapter creates a scheduler adapter that wraps the new weighted fair
// queuing scheduler behind the old Scheduler interface.
func NewAdapter(ctx context.Context, config Config) (*SchedulerAdapter, error) {
	config = normaliseConfig(config)
	s, err := scheduler.New(ctx, scheduler.Config{
		Alpha:           0.3,
		CostTTL:         time.Hour,
		FairnessTTL:     10 * time.Minute,
		CleanupInterval: time.Minute,
	}, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create new scheduler")
	}
	return &SchedulerAdapter{
		inner:           s,
		config:          config,
		registeredTypes: make(map[scheduler.JobType]bool),
	}, nil
}

// Close stops the underlying scheduler and waits for it to shut down.
func (a *SchedulerAdapter) Close() error {
	a.inner.Close()
	return nil
}

// Wait blocks until the underlying scheduler has shut down.
func (a *SchedulerAdapter) Wait() {
	// The new scheduler's Close already waits for goroutines to exit.
}

func (a *SchedulerAdapter) WithQueuePrefix(prefix string) Scheduler {
	return &prefixedScheduler{prefix: prefix + "-", scheduler: a}
}

func (a *SchedulerAdapter) Submit(queue, id string, run func(ctx context.Context) error) {
	jt := a.ensureType(id)
	a.inner.Submit(jt, queue, run)
}

func (a *SchedulerAdapter) SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error) {
	jt := a.ensureType(id)
	a.inner.SubmitPeriodicJob(jt, queue, interval, run)
}

func (a *SchedulerAdapter) ensureType(id string) scheduler.JobType {
	if isCloneJob(id) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if !a.cloneTypeRegistered {
			a.inner.RegisterType(cloneJobType, scheduler.JobTypeConfig{
				DefaultCost:    10,
				MaxConcurrency: a.config.MaxCloneConcurrency,
				ConflictGroup:  adapterConflictGroup,
				Priority:       scheduler.Priority(a.config.Concurrency),
			})
			a.cloneTypeRegistered = true
		}
		return cloneJobType
	}

	jt := scheduler.JobType(id)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.registeredTypes[jt] {
		a.inner.RegisterType(jt, scheduler.JobTypeConfig{
			DefaultCost:    1,
			MaxConcurrency: a.config.Concurrency,
			ConflictGroup:  adapterConflictGroup,
			Priority:       scheduler.Priority(a.config.Concurrency),
		})
		a.registeredTypes[jt] = true
	}
	return jt
}
