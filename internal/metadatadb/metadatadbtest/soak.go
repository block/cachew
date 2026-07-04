package metadatadbtest

import (
	"context"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// SoakConfig configures the soak test parameters.
type SoakConfig struct {
	Duration    time.Duration
	Concurrency int
	NumKeys     int
}

func (c *SoakConfig) setDefaults() {
	if c.Duration == 0 {
		c.Duration = 30 * time.Second
	}
	if c.Concurrency == 0 {
		c.Concurrency = 4
	}
	if c.NumKeys == 0 {
		c.NumKeys = 50
	}
}

// SoakResult contains the results of a soak test run.
type SoakResult struct {
	Ops      int64
	Flushes  int64
	Errors   int64
	Duration time.Duration
}

// tracker collects monotonic invariants from workers for verification.
type tracker struct {
	mu sync.Mutex
	// Add-only counters: each worker adds positive deltas, total must be >= 0.
	counterAdds map[string]int64
	// Add-only sets: members are only added, never removed.
	setAdds map[string]map[string]bool
	// Append-only lists: total appended count per key.
	listAppends map[string]int
}

func newTracker() *tracker {
	return &tracker{
		counterAdds: make(map[string]int64),
		setAdds:     make(map[string]map[string]bool),
		listAppends: make(map[string]int),
	}
}

func (tr *tracker) addCounter(key string, delta int64) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.counterAdds[key] += delta
}

func (tr *tracker) addSetMember(key, member string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.setAdds[key] == nil {
		tr.setAdds[key] = make(map[string]bool)
	}
	tr.setAdds[key][member] = true
}

func (tr *tracker) appendList(key string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.listAppends[key]++
}

// Soak runs a concurrent soak test against a backend, exercising all data
// structure types with random operations and periodic flushes, then verifies
// consistency by checking monotonic invariants and round-trip equality.
func Soak(t *testing.T, backend metadatadb.Backend, config SoakConfig) SoakResult {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}
	config.setDefaults()

	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	ctx, cancel := context.WithTimeout(ctx, config.Duration+time.Minute)
	defer cancel()

	store := metadatadb.New(ctx, backend)
	t.Cleanup(func() { assert.NoError(t, store.Close(ctx)) })

	ns := store.Namespace("soak")
	tr := newTracker()

	var result SoakResult
	startTime := time.Now()
	deadline := startTime.Add(config.Duration)

	var wg sync.WaitGroup
	for i := range config.Concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			soakWorker(ctx, ns, &config, deadline, workerID, &result, tr)
		}(i)
	}
	wg.Wait()
	result.Duration = time.Since(startTime)

	assert.NoError(t, ns.Flush(ctx))

	verifyMonotonicInvariants(t, ns, tr)
	logSoakResult(t, &result)

	return result
}

// SoakReplicas runs the soak workload concurrently across multiple backends
// sharing the same underlying storage, then verifies the monotonic invariants
// on every replica and that all replicas converge to identical state.
func SoakReplicas(t *testing.T, backends []metadatadb.Backend, config SoakConfig) SoakResult {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}
	config.setDefaults()

	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	ctx, cancel := context.WithTimeout(ctx, config.Duration+time.Minute)
	defer cancel()

	replicas := make([]*metadatadb.Namespace, len(backends))
	for i, backend := range backends {
		store := metadatadb.New(ctx, backend)
		t.Cleanup(func() { assert.NoError(t, store.Close(ctx)) })
		replicas[i] = store.Namespace("soak-replicas")
	}

	tr := newTracker()
	var result SoakResult
	startTime := time.Now()
	deadline := startTime.Add(config.Duration)

	var wg sync.WaitGroup
	for replica, ns := range replicas {
		for worker := range config.Concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				soakWorker(ctx, ns, &config, deadline, replica*config.Concurrency+worker, &result, tr)
			}()
		}
	}
	wg.Wait()
	result.Duration = time.Since(startTime)

	// Writes are synchronous, so everything is durable once the workers
	// stop; each replica's flush then observes every write.
	for _, ns := range replicas {
		assert.NoError(t, ns.Flush(ctx))
	}
	for _, ns := range replicas {
		verifyMonotonicInvariants(t, ns, tr)
	}
	verifyReplicasConverge(t, replicas, config.NumKeys)
	logSoakResult(t, &result)
	return result
}

func verifyReplicasConverge(t *testing.T, replicas []*metadatadb.Namespace, numKeys int) {
	t.Helper()
	base := replicas[0]
	for i, ns := range replicas[1:] {
		for k := range numKeys {
			key := fmt.Sprintf("key-%d", k)

			baseScalar, baseOK := metadatadb.NewScalar[string](base, "sc-"+key).Get()
			scalar, ok := metadatadb.NewScalar[string](ns, "sc-"+key).Get()
			assert.Equal(t, baseOK, ok, "replica %d scalar sc-%s presence", i+1, key)
			assert.Equal(t, baseScalar, scalar, "replica %d scalar sc-%s", i+1, key)

			assert.Equal(t,
				metadatadb.NewInt(base, "int-"+key).Get(),
				metadatadb.NewInt(ns, "int-"+key).Get(), "replica %d int-%s", i+1, key)
			assert.Equal(t,
				metadatadb.NewSet[string](base, "set-"+key).Members(),
				metadatadb.NewSet[string](ns, "set-"+key).Members(), "replica %d set-%s", i+1, key)
			assert.Equal(t,
				metadatadb.NewMap[string, string](base, "map-"+key).Entries(),
				metadatadb.NewMap[string, string](ns, "map-"+key).Entries(), "replica %d map-%s", i+1, key)
			assert.Equal(t,
				metadatadb.NewInt(base, "mono-int-"+key).Get(),
				metadatadb.NewInt(ns, "mono-int-"+key).Get(), "replica %d mono-int-%s", i+1, key)
			assert.Equal(t,
				metadatadb.NewSet[string](base, "mono-set-"+key).Members(),
				metadatadb.NewSet[string](ns, "mono-set-"+key).Members(), "replica %d mono-set-%s", i+1, key)
			// Element order must match too: canonical replay order is total.
			assert.Equal(t,
				metadatadb.NewList[string](base, "mono-list-"+key).Entries(),
				metadatadb.NewList[string](ns, "mono-list-"+key).Entries(), "replica %d mono-list-%s", i+1, key)
		}
	}
}

func soakWorker(
	ctx context.Context,
	ns *metadatadb.Namespace,
	config *SoakConfig,
	deadline time.Time,
	workerID int,
	result *SoakResult,
	tr *tracker,
) {
	rng := mrand.New(mrand.NewPCG(uint64(workerID), uint64(time.Now().UnixNano()))) //nolint:gosec

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("key-%d", rng.IntN(config.NumKeys))
		op := rng.IntN(100)

		switch {
		// Chaotic ops (not tracked for invariants).
		case op < 10:
			recordSoakWrite(result, metadatadb.NewScalar[string](ns, "sc-"+key).Set(fmt.Sprintf("val-%d", rng.IntN(1000))))
		case op < 15:
			recordSoakWrite(result, metadatadb.NewScalar[string](ns, "sc-"+key).Delete())
		case op < 20:
			recordSoakWrite(result, metadatadb.NewInt(ns, "int-"+key).Set(int64(rng.IntN(1000))))
		case op < 25:
			recordSoakWrite(result, metadatadb.NewSet[string](ns, "set-"+key).Add(fmt.Sprintf("m-%d", rng.IntN(20))))
		case op < 28:
			recordSoakWrite(result, metadatadb.NewSet[string](ns, "set-"+key).Remove(fmt.Sprintf("m-%d", rng.IntN(20))))
		case op < 35:
			recordSoakWrite(result, metadatadb.NewMap[string, string](ns, "map-"+key).Set(
				fmt.Sprintf("k-%d", rng.IntN(20)),
				fmt.Sprintf("v-%d", rng.IntN(1000)),
			))

		// Monotonic ops (tracked for invariant verification).
		case op < 50:
			delta := int64(rng.IntN(100) + 1) // always positive
			if recordSoakWrite(result, metadatadb.NewInt(ns, "mono-int-"+key).Add(delta)) {
				tr.addCounter("mono-int-"+key, delta)
			}
		case op < 65:
			member := fmt.Sprintf("m-%d", rng.IntN(50))
			if recordSoakWrite(result, metadatadb.NewSet[string](ns, "mono-set-"+key).Add(member)) {
				tr.addSetMember("mono-set-"+key, member)
			}
		case op < 75:
			if recordSoakWrite(result, metadatadb.NewList[string](ns, "mono-list-"+key).Append(fmt.Sprintf("e-%d", rng.IntN(1000)))) {
				tr.appendList("mono-list-" + key)
			}

		// Reads.
		case op < 80:
			metadatadb.NewScalar[string](ns, "sc-"+key).Get()
		case op < 85:
			metadatadb.NewInt(ns, "int-"+key).Get()
		case op < 90:
			metadatadb.NewSet[string](ns, "set-"+key).Members()
		case op < 95:
			metadatadb.NewMap[string, string](ns, "map-"+key).Entries()

		// Flushes.
		default:
			if err := ns.Flush(ctx); err != nil {
				atomic.AddInt64(&result.Errors, 1)
			} else {
				atomic.AddInt64(&result.Flushes, 1)
			}
		}
		atomic.AddInt64(&result.Ops, 1)
	}
}

func recordSoakWrite(result *SoakResult, err error) bool {
	if err == nil {
		return true
	}
	atomic.AddInt64(&result.Errors, 1)
	return false
}

func verifyMonotonicInvariants(t *testing.T, ns *metadatadb.Namespace, tr *tracker) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()

	for key, expectedTotal := range tr.counterAdds {
		actual := metadatadb.NewInt(ns, key).Get()
		assert.Equal(t, expectedTotal, actual, "counter %s mismatch", key)
	}

	for key, expectedMembers := range tr.setAdds {
		actual := metadatadb.NewSet[string](ns, key).Members()
		actualSet := make(map[string]bool, len(actual))
		for _, m := range actual {
			actualSet[m] = true
		}
		for member := range expectedMembers {
			assert.True(t, actualSet[member], "set %s missing member %q", key, member)
		}
	}

	for key, expectedLen := range tr.listAppends {
		actual := metadatadb.NewList[string](ns, key).Entries()
		assert.Equal(t, expectedLen, len(actual), "list %s length mismatch", key)
	}
}

func logSoakResult(t *testing.T, result *SoakResult) {
	t.Helper()
	t.Logf("Soak test completed:")
	t.Logf("  Duration: %v", result.Duration)
	t.Logf("  Ops: %d (%.1f/sec)", result.Ops, float64(result.Ops)/result.Duration.Seconds())
	t.Logf("  Flushes: %d", result.Flushes)
	t.Logf("  Errors: %d", result.Errors)
}
