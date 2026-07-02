package cache_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

// seedTier writes content to a single tier directly, so tests can diverge the
// versions held by each tier.
func seedTier(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, content []byte) {
	t.Helper()
	w, err := c.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

func contentETag(content []byte) string {
	sum := sha256.Sum256(content)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func readAllAndClose(t *testing.T, r io.ReadCloser) []byte {
	t.Helper()
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	return data
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
			seedTier(ctx, t, upper, key, content)

			// Verify lower tier does not have it.
			_, _, err := lower.Open(ctx, key)
			assert.IsError(t, err, os.ErrNotExist)

			// Open through tiered — should hit upper and backfill lower.
			r, _, err := tiered.Open(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, content, readAllAndClose(t, r))

			// Now lower tier should have the entry via backfill.
			r2, _, err := lower.Open(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, content, readAllAndClose(t, r2))
		})
	}
}

// TestTieredDivergentValidatorPermutations covers a replica whose local (lower)
// tier holds a different version of an object than the shared (upper) tier:
// requests pinned to a version via If-Range or If-Match must be satisfied by
// whichever tier holds that version rather than answered from the first tier
// that has any version.
func TestTieredDivergentValidatorPermutations(t *testing.T) {
	stale := []byte("0123456789")
	pinned := []byte("abcdefghij")
	staleETag := contentETag(stale)
	pinnedETag := contentETag(pinned)

	for _, perm := range tieredPermutations(t) {
		t.Run(perm.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
			lower := perm.lower.new(t)
			upper := perm.upper.new(t)
			tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
			defer tiered.Close()

			keyBoth := cache.NewKey("divergent-both")
			seedTier(ctx, t, lower, keyBoth, stale)
			seedTier(ctx, t, upper, keyBoth, pinned)

			keyLowerOnly := cache.NewKey("divergent-lower-only")
			seedTier(ctx, t, lower, keyLowerOnly, stale)

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
				seedTier(ctx, t, lower, keyHeal, stale)
				seedTier(ctx, t, upper, keyHeal, pinned)

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
