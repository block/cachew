package cache_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

type cacheFactory struct {
	name string
	new  func(t *testing.T) cache.Cache
}

func tieredPermutations(t *testing.T) []struct {
	name    string
	lower   cacheFactory
	upper   cacheFactory
	cleanup func()
} {
	t.Helper()

	bucket := s3clienttest.Start(t)

	newMemory := cacheFactory{"Memory", func(t *testing.T) cache.Cache {
		t.Helper()
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		return c
	}}

	newDisk := cacheFactory{"Disk", func(t *testing.T) cache.Cache {
		t.Helper()
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		c, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		return c
	}}

	newS3 := cacheFactory{"S3", func(t *testing.T) cache.Cache {
		t.Helper()
		s3clienttest.CleanBucket(t, bucket)
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
			Endpoint: s3clienttest.Addr,
			UseSSL:   false,
		})
		c, err := cache.NewS3(ctx, cache.S3Config{
			Bucket:           bucket,
			MaxTTL:           3 * time.Second,
			UploadPartSizeMB: 16,
		}, clientProvider)
		assert.NoError(t, err)
		return c
	}}

	backends := []cacheFactory{newMemory, newDisk, newS3}
	var perms []struct {
		name    string
		lower   cacheFactory
		upper   cacheFactory
		cleanup func()
	}
	for _, lower := range backends {
		for _, upper := range backends {
			if lower.name == upper.name {
				continue
			}
			perms = append(perms, struct {
				name    string
				lower   cacheFactory
				upper   cacheFactory
				cleanup func()
			}{
				name:  fmt.Sprintf("%s+%s", lower.name, upper.name),
				lower: lower,
				upper: upper,
			})
		}
	}
	return perms
}

func TestTieredCachePermutations(t *testing.T) {
	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			cachetest.Suite(t, func(t *testing.T) cache.Cache {
				_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
				lower := perm.lower.new(t)
				upper := perm.upper.new(t)
				return cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			})
		})
	}
}

func TestTieredBackfillPermutations(t *testing.T) {
	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			upper := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			defer tiered.Close()

			key := cache.NewKey("backfill-test")
			content := []byte("hello backfill")

			// Write only to upper tier, simulating a lower-tier miss.
			w, err := upper.Create(ctx, key, nil, time.Minute)
			assert.NoError(t, err)
			_, err = w.Write(content)
			assert.NoError(t, err)
			assert.NoError(t, w.Close())

			// Verify lower tier does not have it.
			_, _, err = lower.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)

			// Open through tiered — should hit upper and backfill lower.
			r, _, err := tiered.Open(ctx, key)
			assert.NoError(t, err)
			data, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			assert.Equal(t, content, data)

			// Now lower tier should have the entry via backfill.
			r2, _, err := lower.Open(ctx, key)
			assert.NoError(t, err)
			data2, err := io.ReadAll(r2)
			assert.NoError(t, err)
			assert.NoError(t, r2.Close())
			assert.Equal(t, content, data2)
		})
	}
}

// TestTieredRangeBackfill verifies that a ranged read through the tiered cache
// does not commit a truncated tier-0 entry, and instead triggers a background
// full copy that warms tier 0 with the whole object.
func TestTieredRangeBackfill(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	lower, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("range-backfill")
	content := []byte("0123456789abcdefghij")

	// Seed only the upper tier, simulating a lower-tier miss.
	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Ranged read through the tiered cache.
	reader, _, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	off, err := reader.Seek(10, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), off)
	got, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())
	assert.Equal(t, content[10:], got)

	// The background singleton copy eventually warms tier 0 with the whole
	// object — never the truncated range slice.
	assert.NoError(t, waitFor(2*time.Second, func() bool {
		r, _, oErr := lower.Open(ctx, key)
		if oErr != nil {
			return false
		}
		data, _ := io.ReadAll(r)
		_ = r.Close()
		return string(data) == string(content)
	}))
}

// TestTieredFullReadTees verifies that a full sequential read still tees into
// tier 0 via the cheap backfill path (no background copy needed).
func TestTieredFullReadTees(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	lower, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("full-read-tees")
	content := []byte("the quick brown fox jumps over the lazy dog")

	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	reader, _, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	got, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())
	assert.Equal(t, content, got)

	assert.NoError(t, waitFor(2*time.Second, func() bool {
		r, _, oErr := lower.Open(ctx, key)
		if oErr != nil {
			return false
		}
		data, _ := io.ReadAll(r)
		_ = r.Close()
		return string(data) == string(content)
	}))
}

// TestTieredIncompleteReadDiscards verifies that closing a higher-tier-backed
// reader before fully consuming it never commits a truncated or empty tier-0
// entry. The close-without-read case is exactly what the conditional-request
// (304/412) short-circuit does.
func TestTieredIncompleteReadDiscards(t *testing.T) {
	content := []byte("0123456789abcdefghij")

	setup := func(t *testing.T) (ctx context.Context, lower, tiered cache.Cache, key cache.Key) {
		t.Helper()
		_, ctx = logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		lower, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
		assert.NoError(t, err)
		tiered = cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
		t.Cleanup(func() { tiered.Close() })

		key = cache.NewKey("incomplete-read")
		w, err := upper.Create(ctx, key, nil, time.Minute)
		assert.NoError(t, err)
		_, err = w.Write(content)
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
		return ctx, lower, tiered, key
	}

	t.Run("CloseWithoutReading", func(t *testing.T) {
		ctx, lower, tiered, key := setup(t)
		r, _, err := tiered.Open(ctx, key)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		_, _, err = lower.Open(ctx, key)
		assert.IsError(t, err, os.ErrNotExist)
	})

	t.Run("PartialReadWarmsFullObject", func(t *testing.T) {
		ctx, lower, tiered, key := setup(t)
		r, _, err := tiered.Open(ctx, key)
		assert.NoError(t, err)
		buf := make([]byte, 5)
		_, err = io.ReadFull(r, buf)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		// The partial read abandons the tee (so no truncated entry is committed)
		// and warms tier 0 with a background full copy of the whole object.
		assert.NoError(t, waitFor(2*time.Second, func() bool {
			lr, _, oErr := lower.Open(ctx, key)
			if oErr != nil {
				return false
			}
			data, _ := io.ReadAll(lr)
			_ = lr.Close()
			return string(data) == string(content)
		}))
	})
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("condition not met within timeout")
}

func TestTieredDeletePermutations(t *testing.T) {
	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			upper := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			defer tiered.Close()

			key := cache.NewKey("delete-test")
			content := []byte("delete me")

			// Write through tiered so both tiers have the entry.
			w, err := tiered.Create(ctx, key, nil, time.Minute)
			assert.NoError(t, err)
			_, err = w.Write(content)
			assert.NoError(t, err)
			assert.NoError(t, w.Close())

			// Verify both tiers have it.
			r, _, err := lower.Open(ctx, key)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			r, _, err = upper.Open(ctx, key)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())

			// Delete through tiered.
			err = tiered.Delete(ctx, key)
			assert.NoError(t, err)

			// Verify both tiers no longer have it.
			_, _, err = lower.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)
			_, _, err = upper.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)
		})
	}
}

func TestTieredCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	memory, err := cache.NewMemory(ctx, cache.MemoryConfig{
		LimitMB: 25,
		MaxTTL:  10 * time.Minute,
	})
	assert.NoError(t, err)
	disk, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          t.TempDir(),
		LimitMB:       50,
		MaxTTL:        10 * time.Minute,
		EvictInterval: time.Second,
	})
	assert.NoError(t, err)
	c := cache.MaybeNewTiered(ctx, []cache.Cache{memory, disk})
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      8,
		TTL:              5 * time.Minute,
	})
}
