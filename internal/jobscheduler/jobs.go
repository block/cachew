// Package jobscheduler provides a means to schedule work across multiple queues while limiting overall work.
package jobscheduler

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

type Config struct {
	Concurrency         int    `hcl:"concurrency" help:"The maximum number of concurrent jobs to run (0 means number of cores)." default:"4"`
	MaxCloneConcurrency int    `hcl:"max-clone-concurrency" help:"Maximum number of concurrent clone jobs. Remaining worker slots are reserved for fetch/repack/snapshot jobs. 0 means no limit." default:"0"`
	SchedulerDB         string `hcl:"scheduler-db" help:"Path to the scheduler state database." default:"${CACHEW_STATE}/scheduler.db"`
}

type queueJob struct {
	id    string
	queue string
	run   func(ctx context.Context) error
}

func jobKey(queue, id string) string { return id + ":" + queue }

func (j *queueJob) String() string                { return jobKey(j.queue, j.id) }
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
	p.scheduler.Submit(queue, p.prefix+id, run)
}

func (p *prefixedScheduler) SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error) {
	p.scheduler.SubmitPeriodicJob(queue, p.prefix+id, interval, run)
}

func (p *prefixedScheduler) WithQueuePrefix(prefix string) Scheduler {
	return &prefixedScheduler{
		prefix:    p.prefix + "-" + prefix,
		scheduler: p.scheduler,
	}
}

type RootScheduler struct {
	workAvailable       chan bool
	lock                sync.Mutex
	queue               []queueJob
	active              map[string]string // queue -> job id
	activeClones        int
	maxCloneConcurrency int
	cancel              context.CancelFunc
	store               ScheduleStore
}

var _ Scheduler = &RootScheduler{}

type Provider func() (*RootScheduler, error)

// NewProvider returns a scheduler singleton provider function.
func NewProvider(ctx context.Context, config Config) Provider {
	return sync.OnceValues(func() (*RootScheduler, error) {
		return New(ctx, config)
	})
}

// New creates a new JobScheduler.
func New(ctx context.Context, config Config) (*RootScheduler, error) {
	if config.Concurrency == 0 {
		config.Concurrency = runtime.NumCPU()
	}
	var store ScheduleStore
	if config.SchedulerDB != "" {
		var err error
		store, err = NewScheduleStore(config.SchedulerDB)
		if err != nil {
			return nil, errors.Wrap(err, "create schedule store")
		}
	}
	maxClones := config.MaxCloneConcurrency
	if maxClones == 0 && config.Concurrency > 1 {
		// Default: reserve at least half the workers for non-clone jobs.
		maxClones = max(1, config.Concurrency/2)
	}
	q := &RootScheduler{
		workAvailable:       make(chan bool, 1024),
		active:              make(map[string]string),
		maxCloneConcurrency: maxClones,
		store:               store,
	}
	ctx, cancel := context.WithCancel(ctx)
	q.cancel = cancel
	for id := range config.Concurrency {
		go q.worker(ctx, id)
	}
	return q, nil
}

func (q *RootScheduler) Close() error {
	if q.store != nil {
		return errors.WithStack(q.store.Close())
	}
	return nil
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

func (q *RootScheduler) SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error) {
	key := jobKey(queue, id)
	delay := q.periodicDelay(key, interval)
	submit := func() {
		q.Submit(queue, id, func(ctx context.Context) error {
			err := run(ctx)
			if q.store != nil {
				if storeErr := q.store.SetLastRun(key, time.Now()); storeErr != nil {
					logging.FromContext(ctx).WarnContext(ctx, "Failed to record job last run", "key", key, "error", storeErr)
				}
			}
			go func() {
				time.Sleep(interval)
				q.SubmitPeriodicJob(queue, id, interval, run)
			}()
			return errors.WithStack(err)
		})
	}
	if delay <= 0 {
		submit()
		return
	}
	go func() {
		time.Sleep(delay)
		submit()
	}()
}

func (q *RootScheduler) periodicDelay(key string, interval time.Duration) time.Duration {
	if q.store == nil {
		return 0
	}
	lastRun, ok, err := q.store.GetLastRun(key)
	if err != nil || !ok {
		return 0
	}
	if remaining := time.Until(lastRun.Add(interval)); remaining > 0 {
		return remaining
	}
	return 0
}

func (q *RootScheduler) worker(ctx context.Context, id int) {
	logger := logging.FromContext(ctx).With("scheduler-worker", id)
	for {
		select {
		case <-ctx.Done():
			logger.InfoContext(ctx, "Worker terminated")
			return

		case <-q.workAvailable:
			job, ok := q.takeNextJob()
			if !ok {
				continue
			}
			start := time.Now()
			logger.InfoContext(ctx, "Starting job", "job", job)
			if err := job.run(ctx); err != nil {
				logger.ErrorContext(ctx, "Job failed", "job", job, "error", err, "elapsed", time.Since(start))
			} else {
				logger.InfoContext(ctx, "Job completed", "job", job, "elapsed", time.Since(start))
			}
			q.markQueueInactive(job.queue)
			q.workAvailable <- true
		}
	}
}

func (q *RootScheduler) markQueueInactive(queue string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if isCloneJob(q.active[queue]) {
		q.activeClones--
	}
	delete(q.active, queue)
}

// isCloneJob returns true for job IDs that represent long-running clone operations
// which should be subject to concurrency limits.
func isCloneJob(id string) bool {
	return strings.HasSuffix(id, "clone") || strings.HasSuffix(id, "deferred-mirror-restore")
}

// Take the next job for any queue that is not already running a job.
func (q *RootScheduler) takeNextJob() (queueJob, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for i, job := range q.queue {
		if _, active := q.active[job.queue]; active {
			continue
		}
		if q.maxCloneConcurrency > 0 && isCloneJob(job.id) && q.activeClones >= q.maxCloneConcurrency {
			continue
		}
		q.queue = append(q.queue[:i], q.queue[i+1:]...)
		q.workAvailable <- true
		q.active[job.queue] = job.id
		if isCloneJob(job.id) {
			q.activeClones++
		}
		return job, true
	}
	return queueJob{}, false
}
