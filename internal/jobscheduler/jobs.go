// Package jobscheduler provides a means to schedule work across multiple queues while limiting overall work.
package jobscheduler

import (
	"context"
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
	MaxCost             int    `hcl:"max-cost" help:"Maximum total cost of concurrently running jobs. Each job declares its own cost at submission. 0 means Concurrency * 4." default:"0"`
	SchedulerDB         string `hcl:"scheduler-db" help:"Path to the scheduler state database." default:"${CACHEW_STATE}/scheduler.db"`
}

// Job describes a unit of work to submit to the scheduler.
type Job struct {
	Queue string
	ID    string
	Cost  int
	Clone bool // Subject to MaxCloneConcurrency limits.
	Run   func(ctx context.Context) error
}

func jobKey(queue, id string) string { return id + ":" + queue }

func (j *Job) String() string                { return jobKey(j.Queue, j.ID) }
func (j *Job) run(ctx context.Context) error { return errors.WithStack(j.Run(ctx)) }

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
	// Submit a job to the scheduler.
	//
	// Jobs run concurrently across queues, but never within a queue.
	Submit(job Job)
	// SubmitPeriodicJob submits a job that runs immediately, then repeats after the interval.
	//
	// Jobs run concurrently across queues, but never within a queue.
	SubmitPeriodicJob(job Job, interval time.Duration)
}

type prefixedScheduler struct {
	prefix    string
	scheduler Scheduler
}

func (p *prefixedScheduler) Submit(job Job) {
	job.ID = p.prefix + job.ID
	p.scheduler.Submit(job)
}

func (p *prefixedScheduler) SubmitPeriodicJob(job Job, interval time.Duration) {
	job.ID = p.prefix + job.ID
	p.scheduler.SubmitPeriodicJob(job, interval)
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
	queue               []Job
	active              map[string]string // queue -> job id
	activeCost          int
	activeCosts         map[string]int // queue -> cost of running job
	maxCost             int
	activeClones        int
	activeCloneQueues   map[string]bool
	maxCloneConcurrency int
	cancel              context.CancelFunc
	store               ScheduleStore
	metrics             *schedulerMetrics
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
		maxClones = max(1, config.Concurrency/2)
	}
	maxCost := config.MaxCost
	if maxCost == 0 {
		maxCost = config.Concurrency * 4
	}
	m, err := newSchedulerMetrics()
	if err != nil {
		return nil, errors.Wrap(err, "create scheduler metrics")
	}
	q := &RootScheduler{
		workAvailable:       make(chan bool, 1024),
		active:              make(map[string]string),
		activeCosts:         make(map[string]int),
		activeCloneQueues:   make(map[string]bool),
		maxCost:             maxCost,
		maxCloneConcurrency: maxClones,
		store:               store,
		metrics:             m,
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

func (q *RootScheduler) Submit(job Job) {
	q.lock.Lock()
	q.queue = append(q.queue, job)
	q.metrics.queueDepth.Record(context.Background(), int64(len(q.queue)))
	q.lock.Unlock()
	q.workAvailable <- true
}

func (q *RootScheduler) SubmitPeriodicJob(job Job, interval time.Duration) {
	key := jobKey(job.Queue, job.ID)
	delay := q.periodicDelay(key, interval)
	origRun := job.Run
	submit := func() {
		job.Run = func(ctx context.Context) error {
			err := origRun(ctx)
			if q.store != nil {
				if storeErr := q.store.SetLastRun(key, time.Now()); storeErr != nil {
					logging.FromContext(ctx).WarnContext(ctx, "Failed to record job last run", "key", key, "error", storeErr)
				}
			}
			go func() {
				time.Sleep(interval)
				q.SubmitPeriodicJob(job, interval)
			}()
			return errors.WithStack(err)
		}
		q.Submit(job)
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
			jobAttrs := attribute.String("job.type", jobType(job.ID))
			start := time.Now()
			logger.InfoContext(ctx, "Starting job", "job", job)
			err := job.run(ctx)
			elapsed := time.Since(start)
			status := "success"
			if err != nil {
				status = "error"
				logger.ErrorContext(ctx, "Job failed", "job", job, "error", err, "elapsed", elapsed)
			} else {
				logger.InfoContext(ctx, "Job completed", "job", job, "elapsed", elapsed)
			}
			statusAttr := attribute.String("status", status)
			q.metrics.jobsTotal.Add(ctx, 1, metric.WithAttributes(jobAttrs, statusAttr))
			q.metrics.jobDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(jobAttrs, statusAttr))
			q.markQueueInactive(job.Queue)
			q.workAvailable <- true
		}
	}
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
	q.activeCost -= q.activeCosts[queue]
	delete(q.activeCosts, queue)
	if q.activeCloneQueues[queue] {
		q.activeClones--
		delete(q.activeCloneQueues, queue)
	}
	delete(q.active, queue)
	q.recordGaugesLocked()
}

func (q *RootScheduler) takeNextJob() (Job, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	for i, job := range q.queue {
		if _, active := q.active[job.Queue]; active {
			continue
		}
		if q.activeCost > 0 && q.activeCost+job.Cost > q.maxCost {
			continue
		}
		if job.Clone && q.maxCloneConcurrency > 0 && q.activeClones >= q.maxCloneConcurrency {
			continue
		}
		q.queue = append(q.queue[:i], q.queue[i+1:]...)
		q.workAvailable <- true
		q.active[job.Queue] = job.ID
		q.activeCost += job.Cost
		q.activeCosts[job.Queue] = job.Cost
		if job.Clone {
			q.activeClones++
			q.activeCloneQueues[job.Queue] = true
		}
		q.recordGaugesLocked()
		return job, true
	}
	return Job{}, false
}

// recordGaugesLocked updates gauge metrics. Must be called with q.lock held.
func (q *RootScheduler) recordGaugesLocked() {
	ctx := context.Background()
	q.metrics.queueDepth.Record(ctx, int64(len(q.queue)))
	q.metrics.activeWorkers.Record(ctx, int64(len(q.active)))
	q.metrics.activeCost.Record(ctx, int64(q.activeCost))
	q.metrics.activeClones.Record(ctx, int64(q.activeClones))
}
