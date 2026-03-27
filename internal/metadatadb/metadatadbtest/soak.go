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

	cfg := metadatadb.Config{SyncInterval: time.Hour, LockTTL: 5 * time.Second}
	store := metadatadb.New(ctx, cfg, backend)
	t.Cleanup(func() { assert.NoError(t, store.Close()) })

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
			metadatadb.NewScalar[string](ns, "sc-"+key).Set(fmt.Sprintf("val-%d", rng.IntN(1000)))
		case op < 15:
			metadatadb.NewScalar[string](ns, "sc-"+key).Delete()
		case op < 20:
			metadatadb.NewInt(ns, "int-"+key).Set(int64(rng.IntN(1000)))
		case op < 25:
			metadatadb.NewSet[string](ns, "set-"+key).Add(fmt.Sprintf("m-%d", rng.IntN(20)))
		case op < 28:
			metadatadb.NewSet[string](ns, "set-"+key).Remove(fmt.Sprintf("m-%d", rng.IntN(20)))
		case op < 35:
			metadatadb.NewMap[string, string](ns, "map-"+key).Set(
				fmt.Sprintf("k-%d", rng.IntN(20)),
				fmt.Sprintf("v-%d", rng.IntN(1000)),
			)

		// Monotonic ops (tracked for invariant verification).
		case op < 50:
			delta := int64(rng.IntN(100) + 1) // always positive
			metadatadb.NewInt(ns, "mono-int-"+key).Add(delta)
			tr.addCounter("mono-int-"+key, delta)
		case op < 65:
			member := fmt.Sprintf("m-%d", rng.IntN(50))
			metadatadb.NewSet[string](ns, "mono-set-"+key).Add(member)
			tr.addSetMember("mono-set-"+key, member)
		case op < 75:
			metadatadb.NewList[string](ns, "mono-list-"+key).Append(fmt.Sprintf("e-%d", rng.IntN(1000)))
			tr.appendList("mono-list-" + key)

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
