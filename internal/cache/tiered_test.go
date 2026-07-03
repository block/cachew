package cache_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

// seedTier writes content to a single tier directly, so tests can diverge the
// versions held by each tier.
func seedTier(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, content []byte, rawETag ...string) {
	t.Helper()
	var opts []cache.Option
	if len(rawETag) > 0 {
		opts = append(opts, cache.WithETag(rawETag[0]))
	}
	w, err := c.Create(ctx, key, nil, time.Minute, opts...)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

func readAllAndClose(t *testing.T, r io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	return data
}

type failingCache struct {
	cache.Cache
	err error
}

func newFailingCache(err error) cache.Cache {
	return failingCache{Cache: cache.NoOpCache(), err: err}
}

func (c failingCache) String() string { return "failing" }

func (c failingCache) Stat(_ context.Context, _ cache.Key, _ ...cache.Option) (http.Header, error) {
	return nil, c.err
}

func (c failingCache) Open(_ context.Context, _ cache.Key, _ ...cache.Option) (io.ReadCloser, http.Header, error) {
	return nil, nil, c.err
}

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

func tieredMemoryDiskPermutations() []struct {
	name  string
	lower cacheFactory
	upper cacheFactory
} {
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

	return []struct {
		name  string
		lower cacheFactory
		upper cacheFactory
	}{
		{name: "Memory+Disk", lower: newMemory, upper: newDisk},
		{name: "Disk+Memory", lower: newDisk, upper: newMemory},
	}
}

func TestTieredCachePermutations(t *testing.T) {
	for _, perm := range tieredMemoryDiskPermutations() {
		t.Run(perm.name, func(t *testing.T) {
			cachetest.Suite(t, func(t *testing.T) cache.Cache {
				_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
				lower := perm.lower.new(t)
				upper := perm.upper.new(t)
				return cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			}, cachetest.WithoutInvalidate())
		})
	}
}

func TestTieredBackfillPermutations(t *testing.T) {
	for _, perm := range tieredMemoryDiskPermutations() {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			upper := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			defer tiered.Close()

			key := cache.NewKey("backfill-test")
			content := []byte("hello backfill")
			etag := `"backfill-etag"`

			// Write only to upper tier, simulating a lower-tier miss.
			seedTier(ctx, t, upper, key, content, "backfill-etag")

			// Verify lower tier does not have it.
			_, _, err := lower.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)

			// Open through tiered — should hit upper and backfill lower.
			r, headers, err := tiered.Open(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, content, readAllAndClose(t, r))
			assert.Equal(t, etag, headers.Get(cache.ETagKey))

			// Now lower tier should have the entry via backfill.
			r2, lowerHeaders, err := lower.Open(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, content, readAllAndClose(t, r2))
			assert.Equal(t, etag, lowerHeaders.Get(cache.ETagKey))
		})
	}
}

func TestTieredCreateUsesSameETagInEveryTier(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	lower, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("tiered-create-etag")
	seedTier(ctx, t, tiered, key, []byte("same etag"))

	lowerReader, lowerHeaders, err := lower.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, lowerReader.Close())
	upperReader, upperHeaders, err := upper.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, upperReader.Close())
	assert.Equal(t, lowerHeaders.Get(cache.ETagKey), upperHeaders.Get(cache.ETagKey))
}

func TestTieredDivergentValidatorPermutations(t *testing.T) {
	stale := []byte("0123456789")
	pinned := []byte("abcdefghij")
	const staleETag = `"stale-etag"`
	const pinnedETag = `"pinned-etag"`

	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			upper := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			defer tiered.Close()

			keyBoth := cache.NewKey("divergent-both")
			seedTier(ctx, t, lower, keyBoth, stale, "stale-etag")
			seedTier(ctx, t, upper, keyBoth, pinned, "pinned-etag")

			keyLowerOnly := cache.NewKey("divergent-lower-only")
			seedTier(ctx, t, lower, keyLowerOnly, stale, "stale-etag")

			t.Run("IfRangePinnedToUpperServesRangeFromUpper", func(t *testing.T) {
				r, headers, err := tiered.Open(ctx, keyBoth, cache.Range(2, 6), cache.IfRange(pinnedETag))
				assert.NoError(t, err)
				assert.Equal(t, []byte("cdef"), readAllAndClose(t, r))
				assert.Equal(t, "bytes 2-5/10", headers.Get("Content-Range"))
				assert.Equal(t, pinnedETag, headers.Get(cache.ETagKey))
			})

			t.Run("IfRangePinnedToLowerServesRangeFromLower", func(t *testing.T) {
				r, headers, err := tiered.Open(ctx, keyBoth, cache.Range(2, 6), cache.IfRange(staleETag))
				assert.NoError(t, err)
				assert.Equal(t, []byte("2345"), readAllAndClose(t, r))
				assert.Equal(t, staleETag, headers.Get(cache.ETagKey))
			})

			t.Run("IfRangeMissingEverywhereServesFullFromLower", func(t *testing.T) {
				r, headers, err := tiered.Open(ctx, keyBoth, cache.Range(2, 6), cache.IfRange(`"unknown"`))
				assert.NoError(t, err)
				assert.Equal(t, stale, readAllAndClose(t, r))
				assert.Equal(t, "", headers.Get("Content-Range"))
				assert.Equal(t, staleETag, headers.Get(cache.ETagKey))
			})

			t.Run("IfRangePinnedUpperMissingServesFullFromLower", func(t *testing.T) {
				r, headers, err := tiered.Open(ctx, keyLowerOnly, cache.Range(2, 6), cache.IfRange(pinnedETag))
				assert.NoError(t, err)
				assert.Equal(t, stale, readAllAndClose(t, r))
				assert.Equal(t, "", headers.Get("Content-Range"))
			})

			t.Run("IfMatchPinnedToUpperServesRangeFromUpper", func(t *testing.T) {
				r, headers, err := tiered.Open(ctx, keyBoth, cache.Range(2, 6), cache.IfMatch(pinnedETag))
				assert.NoError(t, err)
				assert.Equal(t, []byte("cdef"), readAllAndClose(t, r))
				assert.Equal(t, "bytes 2-5/10", headers.Get("Content-Range"))
				assert.Equal(t, pinnedETag, headers.Get(cache.ETagKey))

				// A partial body must never be backfilled: the lower tier keeps
				// its own version.
				r2, _, err := lower.Open(ctx, keyBoth)
				assert.NoError(t, err)
				assert.Equal(t, stale, readAllAndClose(t, r2))
			})

			t.Run("IfMatchMissingEverywhereReturnsPreconditionFailed", func(t *testing.T) {
				_, _, err := tiered.Open(ctx, keyBoth, cache.Range(2, 6), cache.IfMatch(`"unknown"`))
				assert.IsError(t, err, cache.ErrPreconditionFailed)
				_, err = tiered.Stat(ctx, keyBoth, cache.IfMatch(`"unknown"`))
				assert.IsError(t, err, cache.ErrPreconditionFailed)
			})

			t.Run("IfMatchPinnedUpperMissingReturnsPreconditionFailed", func(t *testing.T) {
				_, _, err := tiered.Open(ctx, keyLowerOnly, cache.Range(2, 6), cache.IfMatch(pinnedETag))
				assert.IsError(t, err, cache.ErrPreconditionFailed)
			})

			t.Run("StatIfMatchPinnedToUpperReturnsUpperHeaders", func(t *testing.T) {
				headers, err := tiered.Stat(ctx, keyBoth, cache.IfMatch(pinnedETag))
				assert.NoError(t, err)
				assert.Equal(t, pinnedETag, headers.Get(cache.ETagKey))
			})

			t.Run("IfNoneMatchLowerRemainsDefinitive", func(t *testing.T) {
				_, _, err := tiered.Open(ctx, keyBoth, cache.IfNoneMatch(staleETag))
				assert.IsError(t, err, cache.ErrNotModified)
			})

			t.Run("IfMatchFullReadBackfillsLower", func(t *testing.T) {
				keyHeal := cache.NewKey("divergent-heal")
				seedTier(ctx, t, lower, keyHeal, stale, "stale-etag")
				seedTier(ctx, t, upper, keyHeal, pinned, "pinned-etag")

				r, headers, err := tiered.Open(ctx, keyHeal, cache.IfMatch(pinnedETag))
				assert.NoError(t, err)
				assert.Equal(t, pinned, readAllAndClose(t, r))
				assert.Equal(t, pinnedETag, headers.Get(cache.ETagKey))

				// A full-body serve from the upper tier heals the divergent
				// lower tier via the usual backfill.
				r2, _, err := lower.Open(ctx, keyHeal)
				assert.NoError(t, err)
				assert.Equal(t, pinned, readAllAndClose(t, r2))
			})
		})
	}
}

func TestTieredDivergentValidatorProbeErrors(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	lower, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	outage := errors.New("backend unavailable")
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, newFailingCache(outage)})
	defer tiered.Close()

	key := cache.NewKey("divergent-probe-error")
	stale := []byte("0123456789")
	seedTier(ctx, t, lower, key, stale)

	const pinnedETag = `"pinned-etag"`
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "OpenIfMatchReturnsProbeError",
			run: func(t *testing.T) {
				_, _, err := tiered.Open(ctx, key, cache.IfMatch(pinnedETag))
				assert.IsError(t, err, outage)
			},
		},
		{
			name: "StatIfMatchReturnsProbeError",
			run: func(t *testing.T) {
				_, err := tiered.Stat(ctx, key, cache.IfMatch(pinnedETag))
				assert.IsError(t, err, outage)
			},
		},
		{
			name: "OpenIfRangeReturnsProbeError",
			run: func(t *testing.T) {
				_, _, err := tiered.Open(ctx, key, cache.Range(2, 6), cache.IfRange(pinnedETag))
				assert.IsError(t, err, outage)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
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
			seedTier(ctx, t, tiered, key, content)

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

func TestTieredInvalidateSkipsAuthoritativeTier(t *testing.T) {
	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			authoritative := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, authoritative})
			defer tiered.Close()

			key := cache.NewKey("invalidate-test")
			content := []byte("keep authoritative")
			seedTier(ctx, t, tiered, key, content)

			assert.NoError(t, tiered.Invalidate(ctx, key))
			assert.NoError(t, tiered.Invalidate(ctx, cache.NewKey("missing-invalidate-test")))

			_, _, err := lower.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)

			r, _, err := authoritative.Open(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, content, readAllAndClose(t, r))
		})
	}
}

func TestSingleTierInvalidateIsNoop(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	authoritative, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{authoritative})
	defer tiered.Close()

	key := cache.NewKey("single-tier-invalidate")
	content := []byte("single authoritative")
	seedTier(ctx, t, tiered, key, content)

	assert.NoError(t, tiered.Invalidate(ctx, key))

	r, _, err := authoritative.Open(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, content, readAllAndClose(t, r))
}

func TestSingleTierDelegatesUnsupportedOperations(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{cache.NoOpCache()})
	defer tiered.Close()

	_, err := tiered.Stats(ctx)
	assert.IsError(t, err, cache.ErrStatsUnavailable)

	namespaces, err := tiered.ListNamespaces(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{}, namespaces)
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
