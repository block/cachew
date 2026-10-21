package scheduler_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/scheduler"
)

func TestSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("set SOAK_TEST=1 to run soak tests")
	}

	const (
		totalConcurrency = 8
		numClients       = 10
		numRepos         = 5
		soakDuration     = 30 * time.Second
		maxJobLatency    = 5 * time.Millisecond
	)

	soakFG := scheduler.Priority{Level: 10, Weight: 4}
	soakBG := scheduler.Priority{Level: 3, Weight: 1}

	type jobTypeInfo struct {
		name   scheduler.JobType
		config scheduler.JobTypeConfig
	}

	types := []jobTypeInfo{
		{name: "fgFetch", config: scheduler.JobTypeConfig{MaxConcurrency: 0.5, Priority: soakFG, ConflictGroup: "git"}},
		{name: "fgClone", config: scheduler.JobTypeConfig{MaxConcurrency: 0.3, Priority: soakFG, ConflictGroup: "git"}},
		{name: "bgRepack", config: scheduler.JobTypeConfig{MaxConcurrency: 0.5, Priority: soakBG, ConflictGroup: "git"}},
		{name: "bgSnapshot", config: scheduler.JobTypeConfig{MaxConcurrency: 1, Priority: soakBG}},
		{name: "fgDownload", config: scheduler.JobTypeConfig{MaxConcurrency: 0.8, Priority: soakFG}},
	}

	s := newCustomPriorityScheduler(t, scheduler.Config{
		TotalConcurrency: totalConcurrency,
		Alpha:            0.3,
		FairnessTTL:      time.Hour,
		CostTTL:          time.Hour,
		CleanupInterval:  time.Hour,
	}, soakFG, soakBG)
	for _, jt := range types {
		s.RegisterType(jt.name, jt.config)
	}

	// Invariant tracking: per-repo conflict group concurrency.
	type conflictKey struct {
		repo          string
		conflictGroup scheduler.ConflictGroup
	}
	var conflictMu sync.Mutex
	conflictCounts := make(map[conflictKey]int)
	var conflictViolations atomic.Int64

	var totalSubmitted atomic.Int64
	var totalCompleted atomic.Int64
	var totalCancelled atomic.Int64
	var totalErrors atomic.Int64

	ctx, cancel := context.WithTimeout(testContext(), soakDuration)
	defer cancel()

	var wg sync.WaitGroup
	for clientID := range numClients {
		wg.Go(func() {
			fairnessKey := fmt.Sprintf("client-%d", clientID)
			rng := rand.New(rand.NewPCG(uint64(clientID), uint64(clientID+42))) //nolint:gosec

			for ctx.Err() == nil {
				jt := types[rng.IntN(len(types))]
				repo := fmt.Sprintf("repo-%d", rng.IntN(numRepos))
				useSync := rng.IntN(3) > 0 // 2/3 sync, 1/3 async

				jobFn := func(_ context.Context) error {
					// Check conflict group concurrency.
					if jt.config.ConflictGroup != "" {
						ck := conflictKey{repo: repo, conflictGroup: jt.config.ConflictGroup}
						conflictMu.Lock()
						conflictCounts[ck]++
						if conflictCounts[ck] > 1 {
							conflictViolations.Add(1)
						}
						conflictMu.Unlock()
						defer func() {
							conflictMu.Lock()
							conflictCounts[ck]--
							conflictMu.Unlock()
						}()
					}

					time.Sleep(time.Duration(rng.Int64N(int64(maxJobLatency))))
					return nil
				}

				totalSubmitted.Add(1)
				if useSync {
					err := s.RunSync(ctx, jt.name, repo, fairnessKey, jobFn)
					if err != nil {
						if ctx.Err() != nil {
							totalCancelled.Add(1)
							return
						}
						totalErrors.Add(1)
					}
					totalCompleted.Add(1)
				} else {
					s.Submit(jt.name, repo, func(ctx context.Context) error {
						err := jobFn(ctx)
						if err != nil {
							totalErrors.Add(1)
						}
						totalCompleted.Add(1)
						return err
					})
					// Dedup keeps the pending queue bounded, so a brief sleep
					// is enough to avoid busy-spinning.
					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	wg.Wait()

	t.Logf("submitted=%d completed=%d cancelled=%d errors=%d conflict_violations=%d",
		totalSubmitted.Load(), totalCompleted.Load(), totalCancelled.Load(), totalErrors.Load(),
		conflictViolations.Load())

	assert.Equal(t, int64(0), conflictViolations.Load(), "conflict group exclusion violated")
	assert.True(t, totalCompleted.Load() > 0, "no jobs completed")
	assert.Equal(t, int64(0), totalErrors.Load(), "unexpected job errors")
}
