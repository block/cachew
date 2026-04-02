// Package scheduler implements weighted fair queuing with conflict exclusion.
//
// All work — foreground and background — flows through a single scheduler that
// controls admission via four layered constraints:
//
//   - Total concurrency: a hard global cap on running jobs.
//   - Priority tiers: each tier (e.g. foreground, background) gets a
//     weighted share of total concurrency. Higher-level tiers are dispatched
//     first, so foreground work is never starved by background.
//   - Per-type limits: within a tier, each job type may use at most a
//     fraction of the tier's slots (MaxConcurrency).
//   - Conflict groups: jobs sharing a conflict group and job ID are
//     mutually exclusive, preventing unsafe concurrent operations on the
//     same resource.
//
// Within a tier, jobs are ordered by accumulated cost per fairness key
// (lowest first, then arrival time). Cost is estimated via an exponential
// moving average of observed wall time, so clients that consume more
// capacity naturally yield to those that have consumed less.
package scheduler

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// JobType is a named string type for type safety. Constants are defined by the
// application, not the scheduler package.
type JobType string

func (jt JobType) String() string { return string(jt) }

// Priority defines a scheduling tier with dispatch ordering and a share of total concurrency.
// Two Priority values with the same Level are the same tier and must have the same Weight.
type Priority struct {
	// Level controls dispatch ordering; higher values are dispatched first.
	Level int
	// Weight is this tier's weight when dividing TotalConcurrency among tiers.
	// A tier's slot allocation is Weight / sum(all tiers' weights) * TotalConcurrency.
	Weight float64
}

// ConflictGroup names a set of job types that conflict on the same job ID. Two
// jobs conflict if they share a job_id and belong to the same non-empty
// conflict group.
//
// For example the jobs `(type:"repack", id:"git/git")` and `(type:"snapshot", id:"git/git")`
// if in the same ConflictGroup would not run concurrently.
type ConflictGroup string

// JobTypeConfig configures scheduling behaviour for a job type.
type JobTypeConfig struct {
	// MaxConcurrency is the fraction (0-1) of the priority tier's slots this job type may use.
	MaxConcurrency float64
	// ConflictGroup prevents concurrent jobs with the same job ID within the group.
	ConflictGroup ConflictGroup
	// Priority is the scheduling tier this job type belongs to. Must be registered via RegisterPriority.
	Priority Priority
}

// Config holds scheduler tuning parameters.
type Config struct {
	// TotalConcurrency is the maximum number of jobs that can run simultaneously across all tiers.
	TotalConcurrency int           `hcl:"total-concurrency" help:"Maximum total concurrent jobs." default:"50"`
	Alpha            float64       `hcl:"alpha,optional" help:"EMA smoothing factor for cost estimation (0-1)." default:"0.3"`
	CostTTL          time.Duration `hcl:"cost-ttl,optional" help:"TTL for cost estimate entries." default:"1h"`
	FairnessTTL      time.Duration `hcl:"fairness-ttl,optional" help:"TTL for accumulated cost entries." default:"10m"`
	CleanupInterval  time.Duration `hcl:"cleanup-interval,optional" help:"How often to run TTL cleanup." default:"1m"`
}

type job struct {
	jobType     JobType
	jobID       string
	fairnessKey string
	fn          func(ctx context.Context) error
	arrivalTime time.Time
	done        chan error // non-nil for RunSync
}

func (j *job) String() string { return j.jobType.String() + ":" + j.jobID }

type runningJob struct {
	job       *job
	startTime time.Time
}

type costKey struct {
	jobType JobType
	jobID   string
}

type costEntry struct {
	estimate time.Duration
	lastSeen time.Time
}

type fairnessEntry struct {
	cost     time.Duration
	lastSeen time.Time
}

// Scheduler implements weighted fair queuing with conflict exclusion.
type Scheduler struct {
	mu         sync.Mutex
	priorities map[Priority]bool
	types      map[JobType]JobTypeConfig
	pending    []*job
	running    []*runningJob
	fairness   map[string]*fairnessEntry
	costs      map[costKey]*costEntry
	config     Config
	logger     *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	wg     sync.WaitGroup

	metrics *schedulerMetrics

	// Optional persistence for periodic job last-run times.
	lastRuns      *metadatadb.Map[string, int64]
	lastRunsLocal map[string]time.Time

	now func() time.Time // for testing; defaults to time.Now
}

// New creates a new Scheduler. If ns is non-nil it is used to persist periodic
// job last-run times across restarts.
func New(ctx context.Context, config Config, ns *metadatadb.Namespace) (*Scheduler, error) {
	m := newSchedulerMetrics()
	ctx, cancel := context.WithCancel(ctx)
	s := &Scheduler{
		priorities:    make(map[Priority]bool),
		types:         make(map[JobType]JobTypeConfig),
		fairness:      make(map[string]*fairnessEntry),
		costs:         make(map[costKey]*costEntry),
		lastRunsLocal: make(map[string]time.Time),
		config:        config,
		logger:        logging.FromContext(ctx),
		ctx:           ctx,
		cancel:        cancel,
		wake:          make(chan struct{}, 1),
		metrics:       m,
		now:           time.Now,
	}
	if ns != nil {
		s.lastRuns = metadatadb.NewMap[string, int64](ns, "lastRuns")
	}
	s.wg.Add(2)
	go s.dispatchLoop()
	go s.cleanupLoop()
	return s, nil
}

// Close stops the scheduler and waits for background goroutines.
func (s *Scheduler) Close() {
	s.cancel()
	s.wg.Wait()
}

// RegisterPriority registers a priority tier. Must be called before
// registering job types that reference this priority. Panics if already
// registered.
func (s *Scheduler) RegisterPriority(p Priority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.priorities[p] {
		panic("scheduler: priority already registered: " + strconv.Itoa(p.Level))
	}
	s.priorities[p] = true
}

// RegisterType registers a job type with its configuration. Must be called
// before submitting jobs of that type. Panics if the priority has not been
// registered via RegisterPriority.
func (s *Scheduler) RegisterType(jobType JobType, config JobTypeConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.priorities[config.Priority] {
		panic("scheduler: unregistered priority: " + strconv.Itoa(config.Priority.Level))
	}
	s.types[jobType] = config
}

func (s *Scheduler) validateType(jt JobType) {
	if _, ok := s.types[jt]; !ok {
		panic("scheduler: unregistered job type: " + jt.String())
	}
}

// Submit queues a background job for async execution. Returns immediately.
func (s *Scheduler) Submit(jobType JobType, jobID string, fn func(ctx context.Context) error) {
	s.mu.Lock()
	s.validateType(jobType)
	s.pending = append(s.pending, &job{
		jobType:     jobType,
		jobID:       jobID,
		fn:          fn,
		arrivalTime: s.now(),
	})
	s.mu.Unlock()
	s.signal()
}

// RunSync submits a foreground job and blocks until it completes or ctx is
// cancelled. The fn receives a context that is cancelled when either the
// caller's ctx or the scheduler's context is done.
func (s *Scheduler) RunSync(ctx context.Context, jobType JobType, jobID, fairnessKey string, fn func(ctx context.Context) error) error {
	jobCtx, jobCancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, jobCancel)

	s.mu.Lock()
	s.validateType(jobType)
	s.mu.Unlock()

	done := make(chan error, 1)
	j := &job{
		jobType:     jobType,
		jobID:       jobID,
		fairnessKey: fairnessKey,
		fn:          func(_ context.Context) error { return fn(jobCtx) },
		arrivalTime: s.now(),
		done:        done,
	}
	s.mu.Lock()
	s.pending = append(s.pending, j)
	s.mu.Unlock()
	s.signal()

	select {
	case err := <-done:
		stop()
		jobCancel()
		return err
	case <-ctx.Done():
		s.mu.Lock()
		s.removePendingLocked(j)
		s.mu.Unlock()
		stop()
		jobCancel()
		return errors.WithStack(ctx.Err())
	}
}

// SubmitPeriodicJob submits a recurring background job. The first execution is
// delayed by the remaining interval since the last recorded run (if any).
func (s *Scheduler) SubmitPeriodicJob(jobType JobType, jobID string, interval time.Duration, fn func(ctx context.Context) error) {
	key := string(jobType) + "\x00" + jobID
	delay := s.periodicDelay(key, interval)
	submit := func() {
		s.Submit(jobType, jobID, func(ctx context.Context) error {
			err := fn(ctx)
			s.recordLastRun(key)
			go func() {
				select {
				case <-time.After(interval):
					s.SubmitPeriodicJob(jobType, jobID, interval, fn)
				case <-s.ctx.Done():
				}
			}()
			return err
		})
	}
	if delay <= 0 {
		submit()
		return
	}
	go func() {
		select {
		case <-time.After(delay):
			submit()
		case <-s.ctx.Done():
		}
	}()
}

// signal wakes the dispatch loop. Non-blocking; coalesces multiple signals.
func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
			s.dispatch()
		}
	}
}

// dispatch evaluates the pending queue and admits all eligible jobs.
func (s *Scheduler) dispatch() {
	s.mu.Lock()
	slices.SortFunc(s.pending, s.compareJobs)

	var toRun []*job
	n := 0
	for _, j := range s.pending {
		if s.canAdmitLocked(j) {
			est := s.estimatedCostLocked(j)
			s.addFairnessLocked(j.fairnessKey, est)
			s.running = append(s.running, &runningJob{job: j, startTime: s.now()})
			toRun = append(toRun, j)
		} else {
			s.pending[n] = j
			n++
		}
	}
	clear(s.pending[n:])
	s.pending = s.pending[:n]
	s.recordMetricsLocked()
	s.mu.Unlock()

	for _, j := range toRun {
		go s.executeJob(j)
	}
}

func (s *Scheduler) compareJobs(a, b *job) int {
	pa := s.types[a.jobType].Priority.Level
	pb := s.types[b.jobType].Priority.Level
	if pa != pb {
		return pb - pa // higher priority first
	}
	ca := s.fairnessCostLocked(a.fairnessKey)
	cb := s.fairnessCostLocked(b.fairnessKey)
	switch {
	case ca < cb:
		return -1
	case ca > cb:
		return 1
	default:
		return a.arrivalTime.Compare(b.arrivalTime)
	}
}

func (s *Scheduler) canAdmitLocked(j *job) bool {
	if len(s.running) >= s.config.TotalConcurrency {
		return false
	}
	cfg := s.types[j.jobType]
	tierSlots := s.tierSlotsLocked(cfg.Priority.Weight)
	if s.priorityRunningCountLocked(cfg.Priority.Level) >= tierSlots {
		return false
	}
	typeSlots := max(1, int(cfg.MaxConcurrency*float64(tierSlots)))
	if s.typeRunningCountLocked(j.jobType) >= typeSlots {
		return false
	}
	return !s.hasConflictLocked(j)
}

// tierSlotsLocked computes the number of slots for a priority tier as its
// share of TotalConcurrency, proportional to the tier's Weight relative to
// the sum of all registered tiers' weights.
func (s *Scheduler) tierSlotsLocked(weight float64) int {
	var totalWeight float64
	for p := range s.priorities {
		totalWeight += p.Weight
	}
	if totalWeight == 0 {
		return 1
	}
	return max(1, int(weight/totalWeight*float64(s.config.TotalConcurrency)))
}

func (s *Scheduler) executeJob(j *job) {
	start := s.now()
	s.logger.InfoContext(s.ctx, "Starting job", "job", j)
	err := j.fn(s.ctx)
	elapsed := s.now().Sub(start)

	if err != nil {
		s.logger.ErrorContext(s.ctx, "Job failed", "job", j, "error", err, "elapsed", elapsed)
	} else {
		s.logger.InfoContext(s.ctx, "Job completed", "job", j, "elapsed", elapsed)
	}

	s.mu.Lock()
	s.updateCostEstimateLocked(j.jobType, j.jobID, elapsed)
	s.removeFromRunningLocked(j)
	s.recordMetricsLocked()
	s.mu.Unlock()

	if j.done != nil {
		j.done <- err
	}
	s.signal()
}

// --- Type registry helpers ---

func (s *Scheduler) priorityRunningCountLocked(level int) int {
	count := 0
	for _, rj := range s.running {
		if s.types[rj.job.jobType].Priority.Level == level {
			count++
		}
	}
	return count
}

func (s *Scheduler) typeRunningCountLocked(jt JobType) int {
	count := 0
	for _, rj := range s.running {
		if rj.job.jobType == jt {
			count++
		}
	}
	return count
}

func (s *Scheduler) hasConflictLocked(j *job) bool {
	cfg := s.types[j.jobType]
	if cfg.ConflictGroup == "" {
		return false
	}
	for _, rj := range s.running {
		if rj.job.jobID != j.jobID {
			continue
		}
		if s.types[rj.job.jobType].ConflictGroup == cfg.ConflictGroup {
			return true
		}
	}
	return false
}

// --- Cost estimation ---

// defaultCost is the initial cost estimate used for jobs that have not yet
// been observed. Kept small so that unknown jobs don't disproportionately
// penalise a client's fairness counter before real data is available.
const defaultCost = time.Second

func (s *Scheduler) estimatedCostLocked(j *job) time.Duration {
	if entry, ok := s.costs[costKey{j.jobType, j.jobID}]; ok {
		return entry.estimate
	}
	return defaultCost
}

func (s *Scheduler) updateCostEstimateLocked(jt JobType, jobID string, elapsed time.Duration) {
	key := costKey{jt, jobID}
	entry, ok := s.costs[key]
	if !ok {
		s.costs[key] = &costEntry{estimate: elapsed, lastSeen: s.now()}
		return
	}
	alpha := s.config.Alpha
	entry.estimate = time.Duration(alpha*float64(elapsed) + (1-alpha)*float64(entry.estimate))
	entry.lastSeen = s.now()
}

// --- Fairness tracking ---

func (s *Scheduler) fairnessCostLocked(key string) time.Duration {
	if entry, ok := s.fairness[key]; ok {
		return entry.cost
	}
	return 0
}

func (s *Scheduler) addFairnessLocked(key string, cost time.Duration) {
	entry, ok := s.fairness[key]
	if !ok {
		entry = &fairnessEntry{}
		s.fairness[key] = entry
	}
	entry.cost += cost
	entry.lastSeen = s.now()
}

// --- Periodic job persistence ---

func (s *Scheduler) periodicDelay(key string, interval time.Duration) time.Duration {
	var lastRun time.Time
	if s.lastRuns != nil {
		if nanos, ok := s.lastRuns.Get(key); ok {
			lastRun = time.Unix(0, nanos)
		}
	} else {
		s.mu.Lock()
		lastRun = s.lastRunsLocal[key]
		s.mu.Unlock()
	}
	if lastRun.IsZero() {
		return 0
	}
	if remaining := time.Until(lastRun.Add(interval)); remaining > 0 {
		return remaining
	}
	return 0
}

func (s *Scheduler) recordLastRun(key string) {
	now := s.now()
	if s.lastRuns != nil {
		s.lastRuns.Set(key, now.UnixNano())
		return
	}
	s.mu.Lock()
	s.lastRunsLocal[key] = now
	s.mu.Unlock()
}

// --- Slice helpers ---

func (s *Scheduler) removeFromRunningLocked(j *job) {
	s.running = slices.DeleteFunc(s.running, func(rj *runningJob) bool { return rj.job == j })
}

func (s *Scheduler) removePendingLocked(j *job) {
	s.pending = slices.DeleteFunc(s.pending, func(pj *job) bool { return pj == j })
}

// --- TTL cleanup ---

func (s *Scheduler) cleanupLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

func (s *Scheduler) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for key, entry := range s.fairness {
		if now.Sub(entry.lastSeen) > s.config.FairnessTTL {
			delete(s.fairness, key)
		}
	}
	for key, entry := range s.costs {
		if now.Sub(entry.lastSeen) > s.config.CostTTL {
			delete(s.costs, key)
		}
	}
}

// --- Metrics ---

func (s *Scheduler) recordMetricsLocked() {
	ctx := context.Background()
	s.metrics.pendingJobs.Record(ctx, int64(len(s.pending)))
	s.metrics.runningJobs.Record(ctx, int64(len(s.running)))
}
