// Package jobscheduler provides a means to schedule work across multiple queues while limiting overall work.
package jobscheduler

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

type Config struct {
	Concurrency int `hcl:"concurrency" help:"The maximum number of concurrent jobs to run (0 means number of cores)." default:"0"`
}

type queueJob struct {
	id    string
	queue string
	run   func(ctx context.Context) error
}

func (j *queueJob) String() string                { return fmt.Sprintf("job-%s-%s", j.queue, j.id) }
func (j *queueJob) Run(ctx context.Context) error { return errors.WithStack(j.run(ctx)) }

// Scheduler runs background jobs concurrently across multiple serialised queues.
//
// That is, each queue can have at most one job running at a time, but multiple queues can run concurrently.
//
// Its primary role is to rate limit concurrent background tasks so that we don't DoS the host when, for example,
// generating git snapshots, GCing git repos, etc.
type Scheduler interface {
	// WithQueuePrefix creates a new Scheduler that prefixes all queue names with the given prefix.
	//
	// This is useful to avoid collisions across strategies.
	WithQueuePrefix(prefix string) Scheduler
	// Submit a job to the queue.
	//
	// Jobs run concurrently across queues, but never within a queue.
	Submit(queue, id string, run func(ctx context.Context) error)
	// SubmitPeriodicJob submits a job to the queue that runs immediately, and then periodically after the interval.
	//
	// Jobs run concurrently across queues, but never within a queue.
	SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error)
}

type prefixedScheduler struct {
	prefix    string
	scheduler Scheduler
}

func (p *prefixedScheduler) Submit(queue, id string, run func(ctx context.Context) error) {
	p.scheduler.Submit(p.prefix+queue, id, run)
}

func (p *prefixedScheduler) SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error) {
	p.scheduler.SubmitPeriodicJob(p.prefix+queue, id, interval, run)
}

func (p *prefixedScheduler) WithQueuePrefix(prefix string) Scheduler {
	return &prefixedScheduler{
		prefix:    p.prefix + "-" + prefix,
		scheduler: p.scheduler,
	}
}

type RootScheduler struct {
	workAvailable chan bool
	lock          sync.Mutex
	queue         []queueJob
	active        map[string]bool
	cancel        context.CancelFunc
}

var _ Scheduler = &RootScheduler{}

// New creates a new JobScheduler.
func New(ctx context.Context, config Config) Scheduler {
	if config.Concurrency == 0 {
		config.Concurrency = runtime.NumCPU()
	}
	q := &RootScheduler{
		workAvailable: make(chan bool, 1024),
		active:        make(map[string]bool),
	}
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel
	for id := range config.Concurrency {
		go q.worker(ctx, id)
	}
	return q
}

func (q *RootScheduler) WithQueuePrefix(prefix string) Scheduler {
	return &prefixedScheduler{
		prefix:    prefix + "-",
		scheduler: q,
	}
}

func (q *RootScheduler) Submit(queue, id string, run func(ctx context.Context) error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queue = append(q.queue, queueJob{queue: queue, id: id, run: run})
	q.workAvailable <- true
}

func (q *RootScheduler) SubmitPeriodicJob(queue, description string, interval time.Duration, run func(ctx context.Context) error) {
	q.Submit(queue, description, func(ctx context.Context) error {
		err := run(ctx)
		go func() {
			time.Sleep(interval)
			q.SubmitPeriodicJob(queue, description, interval, run)
		}()
		return errors.WithStack(err)
	})
}

func (q *RootScheduler) worker(ctx context.Context, id int) {
	logger := logging.FromContext(ctx).With("scheduler-worker", id)
	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, fmt.Sprintf("Scheduler worker %d terminated", id))
			return

		case <-q.workAvailable:
			job, ok := q.takeNextJob()
			if !ok {
				continue
			}
			jlogger := logger.With("job", job.String())
			jlogger.InfoContext(ctx, fmt.Sprintf("Running job: %s", job.String()))
			if err := job.run(ctx); err != nil {
				jlogger.ErrorContext(ctx, fmt.Sprintf("Job failed (%s): %v", job.String(), err), "error", err)
			}
			q.markQueueInactive(job.queue)
			q.workAvailable <- true
		}
	}
}

func (q *RootScheduler) markQueueInactive(queue string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	delete(q.active, queue)
}

// Take the next job for any queue that is not already running a job.
func (q *RootScheduler) takeNextJob() (queueJob, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for i, job := range q.queue {
		if !q.active[job.queue] {
			q.queue = append(q.queue[:i], q.queue[i+1:]...)
			q.workAvailable <- true
			q.active[job.queue] = true
			return job, true
		}
	}
	return queueJob{}, false
}
