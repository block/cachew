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
		numClients    = 10
		numRepos      = 5
		soakDuration  = 30 * time.Second
		maxJobLatency = 5 * time.Millisecond
	)

	type jobTypeInfo struct {
		name   scheduler.JobType
		config scheduler.JobTypeConfig
	}

	types := []jobTypeInfo{
		{name: "fgFetch", config: scheduler.JobTypeConfig{DefaultCost: 5, MaxConcurrency: 4, Priority: 8, ConflictGroup: "git"}},
		{name: "fgClone", config: scheduler.JobTypeConfig{DefaultCost: 10, MaxConcurrency: 2, Priority: 8, ConflictGroup: "git"}},
		{name: "bgRepack", config: scheduler.JobTypeConfig{DefaultCost: 20, MaxConcurrency: 2, Priority: 3, ConflictGroup: "git"}},
		{name: "bgSnapshot", config: scheduler.JobTypeConfig{DefaultCost: 5, MaxConcurrency: 4, Priority: 3}},
		{name: "fgDownload", config: scheduler.JobTypeConfig{DefaultCost: 3, MaxConcurrency: 6, Priority: 10}},
	}

	s := newTestScheduler(t)
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

	// Per-type concurrency tracking.
	typeCounts := make(map[scheduler.JobType]*atomic.Int64)
	for _, jt := range types {
		typeCounts[jt.name] = &atomic.Int64{}
	}
	var typeViolations atomic.Int64

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
					// Check type concurrency.
					cur := typeCounts[jt.name].Add(1)
					if int(cur) > jt.config.MaxConcurrency {
						typeViolations.Add(1)
					}
					defer typeCounts[jt.name].Add(-1)

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
					done := make(chan struct{})
					s.Submit(jt.name, repo, func(ctx context.Context) error {
						defer close(done)
						err := jobFn(ctx)
						if err != nil {
							totalErrors.Add(1)
						}
						totalCompleted.Add(1)
						return err
					})
					// Wait for async job so we don't flood the queue unboundedly.
					select {
					case <-done:
					case <-ctx.Done():
						totalCancelled.Add(1)
						return
					}
				}
			}
		})
	}

	wg.Wait()

	t.Logf("submitted=%d completed=%d cancelled=%d errors=%d conflict_violations=%d type_violations=%d",
		totalSubmitted.Load(), totalCompleted.Load(), totalCancelled.Load(), totalErrors.Load(),
		conflictViolations.Load(), typeViolations.Load())

	assert.Equal(t, int64(0), conflictViolations.Load(), "conflict group exclusion violated")
	assert.Equal(t, int64(0), typeViolations.Load(), "type concurrency limit violated")
	assert.True(t, totalCompleted.Load() > 0, "no jobs completed")
	assert.Equal(t, int64(0), totalErrors.Load(), "unexpected job errors")
}
