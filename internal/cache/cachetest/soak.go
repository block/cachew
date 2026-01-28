package cachetest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
)

// SoakConfig configures the soak test parameters.
type SoakConfig struct {
	// Duration is how long to run the soak test (default: 1 minute).
	Duration time.Duration
	// NumObjects is the total number of unique object keys to use (default: 1000).
	NumObjects int
	// MaxObjectSize is the maximum size of each object in bytes (default: 1MB).
	MaxObjectSize int
	// MinObjectSize is the minimum size of each object in bytes (default: 1KB).
	MinObjectSize int
	// OverwritePercent is the percentage of writes that should overwrite existing keys (default: 20).
	OverwritePercent int
	// Concurrency is the number of concurrent goroutines writing to the cache (default: 4).
	Concurrency int
	// TTL is the time-to-live for cache entries (default: 1 hour).
	TTL time.Duration
}

func (c *SoakConfig) setDefaults() {
	if c.Duration == 0 {
		c.Duration = time.Minute
	}
	if c.NumObjects == 0 {
		c.NumObjects = 1000
	}
	if c.MaxObjectSize == 0 {
		c.MaxObjectSize = 1024 * 1024 // 1MB
	}
	if c.MinObjectSize == 0 {
		c.MinObjectSize = 1024 // 1KB
	}
	if c.OverwritePercent == 0 {
		c.OverwritePercent = 20
	}
	if c.Concurrency == 0 {
		c.Concurrency = 4
	}
	if c.TTL == 0 {
		c.TTL = time.Hour
	}
}

// SoakResult contains the results of a soak test run.
type SoakResult struct {
	Writes       int64
	Reads        int64
	ReadHits     int64
	ReadMisses   int64
	Deletes      int64
	BytesWritten int64
	Duration     time.Duration

	// Memory stats
	HeapAllocStart uint64
	HeapAllocEnd   uint64
	TotalAlloc     uint64
	NumGC          uint32
}

// Soak runs an extended soak test against a cache implementation.
//
// The test writes random objects of varying sizes, with some overwrites,
// and verifies that the cache behaves correctly under sustained load.
// It also performs periodic reads and deletes.
func Soak(t *testing.T, c cache.Cache, config SoakConfig) SoakResult {
	config.setDefaults()

	ctx, cancel := context.WithTimeout(t.Context(), config.Duration+time.Minute)
	defer cancel()

	var result SoakResult
	var mu sync.Mutex
	// key index -> list of SHA256 hashes of all values ever written to this key
	writtenHashes := make(map[int][][32]byte)

	// Capture initial memory stats
	runtime.GC()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)
	result.HeapAllocStart = memStart.HeapAlloc

	startTime := time.Now()
	deadline := startTime.Add(config.Duration)

	var wg sync.WaitGroup
	for i := range config.Concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			soakWorker(ctx, t, c, &config, deadline, workerID, &result, &mu, writtenHashes)
		}(i)
	}

	wg.Wait()
	result.Duration = time.Since(startTime)

	// Capture final memory stats
	runtime.GC()
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	result.HeapAllocEnd = memEnd.HeapAlloc
	result.TotalAlloc = memEnd.TotalAlloc - memStart.TotalAlloc
	result.NumGC = memEnd.NumGC - memStart.NumGC

	verifyHealth(t, c, &result)

	return result
}

func soakWorker(
	ctx context.Context,
	t *testing.T,
	c cache.Cache,
	config *SoakConfig,
	deadline time.Time,
	workerID int,
	result *SoakResult,
	mu *sync.Mutex,
	writtenHashes map[int][][32]byte,
) {
	//nolint:gosec // math/rand is fine for soak testing, we don't need cryptographic randomness for test operations
	rng := mrand.New(mrand.NewPCG(uint64(workerID), uint64(time.Now().UnixNano())))

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		op := rng.IntN(100)
		switch {
		case op < 60: // 60% writes
			doWrite(ctx, t, c, config, rng, result, mu, writtenHashes)
		case op < 90: // 30% reads
			doRead(ctx, t, c, config, rng, result, mu, writtenHashes)
		default: // 10% deletes
			doDelete(ctx, t, c, config, rng, result, mu, writtenHashes)
		}
	}
}

func doWrite(
	ctx context.Context,
	t *testing.T,
	c cache.Cache,
	config *SoakConfig,
	rng *mrand.Rand,
	result *SoakResult,
	mu *sync.Mutex,
	writtenHashes map[int][][32]byte,
) {
	var keyIdx int
	mu.Lock()
	numWritten := len(writtenHashes)
	mu.Unlock()

	if numWritten > 0 && rng.IntN(100) < config.OverwritePercent {
		// Overwrite an existing key
		keyIdx = rng.IntN(min(numWritten, config.NumObjects))
	} else {
		// Write a new key
		keyIdx = rng.IntN(config.NumObjects)
	}

	size := config.MinObjectSize + rng.IntN(config.MaxObjectSize-config.MinObjectSize+1)
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Errorf("failed to generate random data: %v", err)
		return
	}

	key := cache.NewKey(fmt.Sprintf("soak-key-%d", keyIdx))
	writer, err := c.Create(ctx, key, nil, config.TTL)
	if err != nil {
		t.Errorf("failed to create cache entry: %v", err)
		return
	}

	n, err := writer.Write(data)
	if err != nil {
		t.Errorf("failed to write cache entry: %v", err)
		_ = writer.Close()
		return
	}

	if err := writer.Close(); err != nil {
		t.Errorf("failed to close cache entry: %v", err)
		return
	}

	hash := sha256.Sum256(data)
	mu.Lock()
	writtenHashes[keyIdx] = append(writtenHashes[keyIdx], hash)
	mu.Unlock()

	atomic.AddInt64(&result.Writes, 1)
	atomic.AddInt64(&result.BytesWritten, int64(n))
}

func doRead(
	ctx context.Context,
	t *testing.T,
	c cache.Cache,
	config *SoakConfig,
	rng *mrand.Rand,
	result *SoakResult,
	mu *sync.Mutex,
	writtenHashes map[int][][32]byte,
) {
	mu.Lock()
	numWritten := len(writtenHashes)
	mu.Unlock()

	if numWritten == 0 {
		return
	}

	keyIdx := rng.IntN(config.NumObjects)
	key := cache.NewKey(fmt.Sprintf("soak-key-%d", keyIdx))

	reader, _, err := c.Open(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			atomic.AddInt64(&result.ReadMisses, 1)
			atomic.AddInt64(&result.Reads, 1)
			return
		}
		t.Errorf("failed to open cache entry: %v", err)
		return
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Errorf("failed to read cache entry: %v", err)
		return
	}

	// Hash the data we read, then check against all historical writes
	readHash := sha256.Sum256(data)
	mu.Lock()
	hashes := writtenHashes[keyIdx]
	mu.Unlock()

	if !slices.Contains(hashes, readHash) {
		t.Errorf("data mismatch for key %d: read %d bytes with hash not in %d historical writes",
			keyIdx, len(data), len(hashes))
		return
	}

	atomic.AddInt64(&result.ReadHits, 1)
	atomic.AddInt64(&result.Reads, 1)
}

func doDelete(
	ctx context.Context,
	t *testing.T,
	c cache.Cache,
	config *SoakConfig,
	rng *mrand.Rand,
	result *SoakResult,
	mu *sync.Mutex,
	writtenHashes map[int][][32]byte,
) {
	mu.Lock()
	numWritten := len(writtenHashes)
	mu.Unlock()

	if numWritten == 0 {
		return
	}

	keyIdx := rng.IntN(config.NumObjects)
	key := cache.NewKey(fmt.Sprintf("soak-key-%d", keyIdx))

	if err := c.Delete(ctx, key); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		t.Errorf("failed to delete cache entry: %v", err)
		return
	}

	mu.Lock()
	delete(writtenHashes, keyIdx)
	mu.Unlock()

	atomic.AddInt64(&result.Deletes, 1)
}

func verifyHealth(t *testing.T, c cache.Cache, result *SoakResult) {
	stats, err := c.Stats(context.Background())
	assert.NoError(t, err, "failed to get cache stats")

	t.Logf("Soak test completed:")
	t.Logf("  Duration: %v", result.Duration)
	t.Logf("  Writes: %d (%.1f/sec)", result.Writes, float64(result.Writes)/result.Duration.Seconds())
	t.Logf("  Reads: %d (hits: %d, misses: %d)", result.Reads, result.ReadHits, result.ReadMisses)
	t.Logf("  Deletes: %d", result.Deletes)
	t.Logf("  Bytes written: %d MB", result.BytesWritten/(1024*1024))
	t.Logf("Cache stats:")
	t.Logf("  Objects: %d", stats.Objects)
	t.Logf("  Size: %d MB", stats.Size/(1024*1024))
	t.Logf("  Capacity: %d MB", stats.Capacity/(1024*1024))
	t.Logf("Memory stats:")
	t.Logf("  Heap start: %d MB", result.HeapAllocStart/(1024*1024))
	t.Logf("  Heap end: %d MB", result.HeapAllocEnd/(1024*1024))
	t.Logf("  Total allocated: %d MB", result.TotalAlloc/(1024*1024))
	t.Logf("  GC cycles: %d", result.NumGC)

	// Verify size is within capacity
	if stats.Capacity > 0 {
		assert.True(t, stats.Size <= stats.Capacity,
			"cache size (%d) exceeds capacity (%d)", stats.Size, stats.Capacity)
	}

	// Verify object count is non-negative
	assert.True(t, stats.Objects >= 0, "object count should be non-negative")
}
