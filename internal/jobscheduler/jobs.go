// Package jobscheduler provides a means to schedule work across multiple queues while limiting overall work.
package jobscheduler

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

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
	cond                *sync.Cond
	lock                sync.Mutex
	done                bool
	draining            bool
	queue               []queueJob
	active              map[string]string // queue -> job id
	activeClones        int
	maxCloneConcurrency int
	// ctx is cancelled when the scheduler is torn down; periodic re-arm
	// goroutines select on it so they exit instead of submitting to a dead
	// scheduler.
	ctx    context.Context //nolint:containedctx
	cancel context.CancelFunc
	// drain is closed by Drain so intake stops while workers keep running.
	drain   chan struct{}
	wg      sync.WaitGroup
	store   ScheduleStore
	metrics *schedulerMetrics
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
	m := newSchedulerMetrics()
	q := &RootScheduler{
		active:              make(map[string]string),
		maxCloneConcurrency: maxClones,
		store:               store,
		metrics:             m,
		drain:               make(chan struct{}),
	}
	q.cond = sync.NewCond(&q.lock)
	ctx, cancel := context.WithCancel(ctx)
	q.ctx = ctx
	q.cancel = cancel
	// Wake all workers on context cancellation so they can observe done and exit.
	go func() {
		<-ctx.Done()
		q.lock.Lock()
		q.done = true
		q.lock.Unlock()
		q.cond.Broadcast()
	}()
	q.wg.Add(config.Concurrency)
	for id := range config.Concurrency {
		go q.worker(ctx, id)
	}
	return q, nil
}

// Drain stops the scheduler accepting new submissions and re-arming periodic
// jobs, while leaving workers running to finish in-flight and queued jobs. The
// worker pool is torn down later by cancelling the context passed to New.
func (q *RootScheduler) Drain() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.draining {
		return
	}
	q.draining = true
	close(q.drain)
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
	if q.done || q.draining {
		q.lock.Unlock()
		return
	}
	q.queue = append(q.queue, queueJob{queue: queue, id: id, run: run})
	q.metrics.queueDepth.Record(context.Background(), int64(len(q.queue)))
	q.lock.Unlock()
	q.cond.Signal()
}

func (q *RootScheduler) SubmitPeriodicJob(queue, id string, interval time.Duration, run func(ctx context.Context) error) {
	if q.ctx.Err() != nil || q.isDraining() {
		return
	}
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
			// Re-arm the next firing on a cancellation-aware timer. Without
			// this select, a SIGTERM during the sleep would leave the goroutine
			// to wake and submit to a dead scheduler. The new pod's
			// warmExistingRepos re-registers periodic jobs on startup.
			go q.sleepThenSubmit(interval, func() {
				q.SubmitPeriodicJob(queue, id, interval, run)
			})
			return errors.WithStack(err)
		})
	}
	if delay <= 0 {
		submit()
		return
	}
	go q.sleepThenSubmit(delay, submit)
}

func (q *RootScheduler) isDraining() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.draining
}

// sleepThenSubmit waits for d, then runs fn — unless the scheduler is draining
// or being torn down, in which case it returns immediately.
func (q *RootScheduler) sleepThenSubmit(d time.Duration, fn func()) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-q.ctx.Done():
		return
	case <-q.drain:
		return
	case <-timer.C:
		fn()
	}
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

// Wait blocks until all worker goroutines have exited. The caller should
// cancel the context passed to New first, otherwise Wait blocks forever.
func (q *RootScheduler) Wait() { q.wg.Wait() }

func (q *RootScheduler) worker(ctx context.Context, id int) {
	defer q.wg.Done()
	logger := logging.FromContext(ctx).With("scheduler-worker", id)
	for {
		job, ok := q.waitForJob()
		if !ok {
			logger.InfoContext(ctx, "Worker terminated")
			return
		}
		q.runJob(ctx, logger, job)
	}
}

// waitForJob blocks until a job is available or the scheduler is shut down.
// cond.Wait() atomically releases the lock and suspends the goroutine, so the
// lock is only held during the brief check-and-take, never while sleeping.
// On context cancellation, the goroutine in New() sets done and broadcasts,
// waking all sleeping workers so they can exit.
func (q *RootScheduler) waitForJob() (queueJob, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for {
		if q.done {
			return queueJob{}, false
		}
		if job, ok := q.takeNextJobLocked(); ok {
			return job, true
		}
		q.cond.Wait()
	}
}

func (q *RootScheduler) runJob(ctx context.Context, logger *slog.Logger, job queueJob) {
	defer q.markQueueInactive(job.queue)

	jobAttrs := []attribute.KeyValue{
		attribute.String("job.type", jobType(job.id)),
		attribute.String("job.queue", job.queue),
	}
	start := time.Now()
	logger.InfoContext(ctx, "Starting job", "job", job)

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := make([]byte, 4096)
				stack = stack[:runtime.Stack(stack, false)]
				err = errors.Errorf("panic: %v\n%s", r, stack)
			}
		}()
		err = job.run(ctx)
	}()

	elapsed := time.Since(start)
	status := "success"
	if err != nil {
		status = "error"
		logger.ErrorContext(ctx, "Job failed", "job", job, "error", err, "elapsed", elapsed)
	} else {
		logger.InfoContext(ctx, "Job completed", "job", job, "elapsed", elapsed)
	}
	jobAttrs = append(jobAttrs, attribute.String("status", status))
	q.metrics.jobsTotal.Add(ctx, 1, metric.WithAttributes(jobAttrs...))
	q.metrics.jobDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(jobAttrs...))
}

// jobType extracts a normalised job type from the job ID for metric labels.
func jobType(id string) string {
	switch {
	case strings.HasSuffix(id, "clone"):
		return "clone"
	case strings.HasSuffix(id, "deferred-mirror-restore"):
		return "clone"
	case strings.HasSuffix(id, "fetch"):
		return "fetch"
	case strings.HasSuffix(id, "snapshot-periodic"), strings.HasSuffix(id, "mirror-snapshot-periodic"):
		return "snapshot"
	case strings.HasSuffix(id, "repack-periodic"):
		return "repack"
	default:
		return "other"
	}
}

func (q *RootScheduler) markQueueInactive(queue string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if isCloneJob(q.active[queue]) {
		q.activeClones--
	}
	delete(q.active, queue)
	q.recordGaugesLocked()
	q.cond.Signal()
}

// isCloneJob returns true for job IDs that represent long-running clone operations
// which should be subject to concurrency limits.
func isCloneJob(id string) bool {
	return strings.HasSuffix(id, "clone") || strings.HasSuffix(id, "deferred-mirror-restore")
}

// takeNextJobLocked takes the next job for any queue that is not already running a job.
// Must be called with q.lock held.
func (q *RootScheduler) takeNextJobLocked() (queueJob, bool) {
	for i, job := range q.queue {
		if _, active := q.active[job.queue]; active {
			continue
		}
		if q.maxCloneConcurrency > 0 && isCloneJob(job.id) && q.activeClones >= q.maxCloneConcurrency {
			continue
		}
		q.queue = append(q.queue[:i], q.queue[i+1:]...)
		q.active[job.queue] = job.id
		if isCloneJob(job.id) {
			q.activeClones++
		}
		q.recordGaugesLocked()
		q.cond.Signal()
		return job, true
	}
	return queueJob{}, false
}

// recordGaugesLocked updates gauge metrics. Must be called with q.lock held.
func (q *RootScheduler) recordGaugesLocked() {
	ctx := context.Background()
	q.metrics.queueDepth.Record(ctx, int64(len(q.queue)))
	q.metrics.activeWorkers.Record(ctx, int64(len(q.active)))
	q.metrics.activeClones.Record(ctx, int64(q.activeClones))
}
