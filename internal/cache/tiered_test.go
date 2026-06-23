package cache_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
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
			_, _, err = lower.Open(ctx, key, 0, -1)
			assert.IsError(t, err, os.ErrNotExist)

			// Open through tiered — should hit upper and backfill lower.
			r, _, err := tiered.Open(ctx, key, 0, -1)
			assert.NoError(t, err)
			data, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			assert.Equal(t, content, data)

			// Now lower tier should have the entry via backfill.
			r2, _, err := lower.Open(ctx, key, 0, -1)
			assert.NoError(t, err)
			data2, err := io.ReadAll(r2)
			assert.NoError(t, err)
			assert.NoError(t, r2.Close())
			assert.Equal(t, content, data2)
		})
	}
}

// newMemoryTier returns an in-memory cache for deterministic tiered tests.
func newMemoryTier(ctx context.Context, t *testing.T) cache.Cache {
	t.Helper()
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 64, MaxTTL: time.Hour})
	assert.NoError(t, err)
	return c
}

// readFull opens key over c and returns its full contents, or os.ErrNotExist.
func readFull(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key) ([]byte, error) {
	t.Helper()
	r, _, err := c.Open(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	return data, nil
}

// eventually polls fn until it returns nil or the deadline elapses.
func eventually(t *testing.T, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if err = fn(); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within timeout: %v", err)
}

// A partial read that hits a higher tier must backfill the lower tier with the
// *whole* object in the background, not just the bytes read.
func TestTieredPartialReadBackfillsFullObject(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	lower := newMemoryTier(ctx, t)
	upper := newMemoryTier(ctx, t)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("partial-backfill")
	content := []byte("0123456789abcdef")

	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Read only the middle few bytes through the tiered cache.
	r, headers, err := tiered.Open(ctx, key, 4, 8)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, []byte("4567"), data)
	assert.Equal(t, "4", headers.Get("Content-Length"))

	// The lower tier must eventually hold the entire object, not the fragment.
	eventually(t, func() error {
		got, err := readFull(ctx, t, lower, key)
		if err != nil {
			return err
		}
		if string(got) != string(content) {
			return errors.Errorf("lower tier has %q, want %q", got, content)
		}
		return nil
	})
}

// A write to a key supersedes an in-flight backfill so the lower tier ends up
// with the written data, never the stale backfilled data.
func TestTieredCreateCancelsBackfill(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	lower := newMemoryTier(ctx, t)
	upper := newMemoryTier(ctx, t)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("backfill-vs-write")

	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write([]byte("stale upper data"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Trigger a background full backfill via a partial read, then immediately
	// write fresh data through the tiered cache.
	r, _, err := tiered.Open(ctx, key, 0, 4)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	fresh := []byte("fresh written data")
	assert.NoError(t, cache.WriteFunc(ctx, tiered, key, nil, time.Minute, func(w io.Writer) error {
		_, err := w.Write(fresh)
		return err
	}))

	// The lower tier must converge on the freshly written data and stay there.
	eventually(t, func() error {
		got, err := readFull(ctx, t, lower, key)
		if err != nil {
			return err
		}
		if string(got) != string(fresh) {
			return errors.Errorf("lower tier has %q, want %q", got, fresh)
		}
		return nil
	})
	time.Sleep(100 * time.Millisecond)
	got, err := readFull(ctx, t, lower, key)
	assert.NoError(t, err)
	assert.Equal(t, fresh, got)
}

// A full read backfill that is abandoned before EOF (caller Closes early) must
// NOT commit the partial bytes to the lower tier as if they were the whole
// object. Close blocks until the background writer finishes, so the discard is
// observable immediately afterwards.
func TestTieredBackfillDiscardsOnEarlyClose(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	lower := newMemoryTier(ctx, t)
	upper := newMemoryTier(ctx, t)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("early-close")
	content := make([]byte, 64*1024) // large enough that reading a little leaves EOF unreached
	for i := range content {
		content[i] = byte(i)
	}
	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Full read through tiered (hits upper, backfills lower), but abandon it
	// after a few bytes without reaching EOF.
	r, _, err := tiered.Open(ctx, key, 0, -1)
	assert.NoError(t, err)
	buf := make([]byte, 16)
	_, err = io.ReadFull(r, buf)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// The fragment must not have been committed to the lower tier.
	_, _, err = lower.Open(ctx, key, 0, -1)
	assert.IsError(t, err, os.ErrNotExist)

	// A subsequent full read should still backfill the complete object.
	r, _, err = tiered.Open(ctx, key, 0, -1)
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, content, got)
	eventually(t, func() error {
		lowerData, lerr := readFull(ctx, t, lower, key)
		if lerr != nil {
			return lerr
		}
		if len(lowerData) != len(content) {
			return errors.Errorf("lower has %d bytes, want %d", len(lowerData), len(content))
		}
		return nil
	})
}

// A full read of a zero-byte object must succeed with an empty body, not be
// reported as an unsatisfiable range. Covered for memory and disk; the S3
// backend cannot currently store zero-byte objects (a pre-existing upload
// checksum limitation), so it is excluded here.
func TestOpenRangeEmptyObject(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 64, MaxTTL: time.Hour})
	assert.NoError(t, err)
	for _, c := range []cache.Cache{newMemoryTier(ctx, t), disk} {
		t.Run(c.String(), func(t *testing.T) {
			defer c.Close()
			key := cache.NewKey("empty-object")
			w, err := c.Create(ctx, key, nil, time.Hour)
			assert.NoError(t, err)
			assert.NoError(t, w.Close())

			reader, headers, err := c.Open(ctx, key, 0, -1)
			assert.NoError(t, err)
			defer reader.Close()
			data, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, "", string(data))
			assert.Equal(t, "0", headers.Get("Content-Length"))
		})
	}
}

// Hammer concurrent partial reads (which spawn background backfills) against
// concurrent writes to the same key. Run under -race, this exercises the
// backfill manager's acquire/release/cancel interleavings (e.g. a finishing
// backfill must not deregister a newer one). It asserts the absence of races,
// panics and goroutine leaks rather than a specific final value.
func TestTieredBackfillConcurrencyStress(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	lower := newMemoryTier(ctx, t)
	upper := newMemoryTier(ctx, t)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("stress")
	seed := []byte("seed-content-for-stress")
	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(seed)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 20 {
				r, _, err := tiered.Open(ctx, key, int64(j%4), int64(j%4)+1)
				if err == nil {
					_, _ = io.Copy(io.Discard, r)
					_ = r.Close()
				}
			}
		})
		wg.Go(func() {
			for j := range 20 {
				content := fmt.Appendf(nil, "writer-%d-%d", i, j)
				_ = cache.WriteFunc(ctx, tiered, key, nil, time.Minute, func(w io.Writer) error {
					_, err := w.Write(content)
					return err
				})
			}
		})
	}
	wg.Wait()

	// Both tiers must remain readable and uncorrupted. We deliberately do NOT
	// assert the tiers hold identical bytes: the backfill dedup/cancel is
	// best-effort, so a stale backfill can still race a write and leave the
	// lower tier momentarily behind the upper one. The point of this test is to
	// catch races, panics and goroutine leaks under -race, not value coherence.
	_, lErr := readFull(ctx, t, lower, key)
	_, uErr := readFull(ctx, t, upper, key)
	assert.NoError(t, uErr)
	if lErr != nil {
		assert.IsError(t, lErr, os.ErrNotExist) // a cancelled backfill simply leaves no lower entry
	}
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
			r, _, err := lower.Open(ctx, key, 0, -1)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			r, _, err = upper.Open(ctx, key, 0, -1)
			assert.NoError(t, err)
			assert.NoError(t, r.Close())

			// Delete through tiered.
			err = tiered.Delete(ctx, key)
			assert.NoError(t, err)

			// Verify both tiers no longer have it.
			_, _, err = lower.Open(ctx, key, 0, -1)
			assert.IsError(t, err, os.ErrNotExist)
			_, _, err = upper.Open(ctx, key, 0, -1)
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
