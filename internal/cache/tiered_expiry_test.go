package cache_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

const expiresAtHeader = "X-Cachew-Expires-At"

// ttlRecordingCache records the TTL of every Create, so tests can check backfill lifetimes without waiting for them.
type ttlRecordingCache struct {
	cache.Cache
	ttls chan time.Duration
}

func (c *ttlRecordingCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) (cache.Writer, error) {
	c.ttls <- ttl
	return c.Cache.Create(ctx, key, headers, ttl, opts...)
}

func (c *ttlRecordingCache) nextTTL(t *testing.T) time.Duration {
	t.Helper()
	select {
	case ttl := <-c.ttls:
		return ttl
	case <-time.After(5 * time.Second):
		t.Fatal("tier 0 was not written")
		return 0
	}
}

func (c *ttlRecordingCache) assertNotWritten(t *testing.T) {
	t.Helper()
	select {
	case ttl := <-c.ttls:
		t.Fatalf("tier 0 was written with TTL %s", ttl)
	default:
	}
}

func newExpiryTiers(t *testing.T) (context.Context, cache.Cache, *ttlRecordingCache, cache.Cache) {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	lowerMemory, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 16, MaxTTL: time.Hour})
	assert.NoError(t, err)
	lower := &ttlRecordingCache{Cache: lowerMemory, ttls: make(chan time.Duration, 8)}
	upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 16, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := newTiered(ctx, lower, upper)
	t.Cleanup(func() { assert.NoError(t, tiered.Close()) })
	return ctx, tiered, lower, upper
}

func writeEntry(ctx context.Context, t *testing.T, c cache.Cache, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) {
	t.Helper()
	w, err := c.Create(ctx, key, headers, ttl, opts...)
	assert.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

// backfillTier0 evicts tier 0's copy and reads the entry back through the tiered cache, which backfills tier 0.
func backfillTier0(ctx context.Context, t *testing.T, tiered cache.Cache, lower *ttlRecordingCache, key cache.Key) time.Duration { //nolint:privatedoc // explains why
	t.Helper()
	assert.NoError(t, lower.Delete(ctx, key))
	r, headers, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("payload"), readAllAndClose(t, r))
	assert.Zero(t, headers.Values(expiresAtHeader))
	return lower.nextTTL(t)
}

func assertRemainingTTL(t *testing.T, got, ttl time.Duration) {
	t.Helper()
	assert.True(t, got > ttl-10*time.Second && got <= ttl, "backfill TTL %s, want at most %s", got, ttl)
}

func TestTieredBackfillKeepsWriterTTL(t *testing.T) {
	ctx, tiered, lower, _ := newExpiryTiers(t)
	key := cache.NewKey("backfill")
	writeEntry(ctx, t, tiered, key, nil, 5*time.Minute)
	assert.Equal(t, 5*time.Minute, lower.nextTTL(t))

	headers, err := tiered.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Zero(t, headers.Values(expiresAtHeader))

	assertRemainingTTL(t, backfillTier0(ctx, t, tiered, lower, key), 5*time.Minute)
}

func TestTieredBackfillWithoutTTLUsesMaxTTL(t *testing.T) {
	ctx, tiered, lower, _ := newExpiryTiers(t)
	key := cache.NewKey("no-ttl")
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	writeEntry(ctx, t, tiered, key, http.Header{expiresAtHeader: {past}}, 0)
	assert.Equal(t, time.Duration(0), lower.nextTTL(t))

	assert.Equal(t, time.Duration(0), backfillTier0(ctx, t, tiered, lower, key))
}

func TestTieredBackfillKeepsUpdatedTTL(t *testing.T) {
	ctx, tiered, lower, _ := newExpiryTiers(t)
	key := cache.NewKey("updated")
	writeEntry(ctx, t, tiered, key, nil, time.Hour)
	lower.nextTTL(t)

	assert.NoError(t, tiered.UpdateMetadata(ctx, key, nil, 5*time.Minute))
	assertRemainingTTL(t, backfillTier0(ctx, t, tiered, lower, key), 5*time.Minute)

	assert.NoError(t, tiered.UpdateMetadata(ctx, key, nil, 0))
	assert.Equal(t, time.Duration(0), backfillTier0(ctx, t, tiered, lower, key))
}

func TestTieredHealKeepsWriterTTL(t *testing.T) {
	ctx, tiered, lower, _ := newExpiryTiers(t)
	key := cache.NewKey("heal")
	writeEntry(ctx, t, tiered, key, nil, time.Hour, cache.WithETag("stale-etag"))
	writeEntry(ctx, t, tiered, key, nil, 5*time.Minute, cache.WithETag("pinned-etag"))
	writeEntry(ctx, t, lower, key, nil, time.Hour, cache.WithETag("stale-etag"))
	for range 3 {
		lower.nextTTL(t)
	}

	r, _, err := tiered.Open(ctx, key, cache.Range(0, 3), cache.IfMatch(`"pinned-etag"`))
	assert.NoError(t, err)
	assert.Equal(t, []byte("pay"), readAllAndClose(t, r))
	assertRemainingTTL(t, lower.nextTTL(t), 5*time.Minute)
}

func TestTieredSkipsBackfillPastWriterTTL(t *testing.T) {
	ctx, tiered, lower, upper := newExpiryTiers(t)
	key := cache.NewKey("expired")
	past := time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano)
	// Stands in for a lower tier that rounds expiry up and still serves the entry just past the writer's TTL.
	writeEntry(ctx, t, upper, key, http.Header{expiresAtHeader: {past}}, time.Hour)

	r, headers, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, []byte("payload"), readAllAndClose(t, r))
	assert.Zero(t, headers.Values(expiresAtHeader))
	lower.assertNotWritten(t)
	_, err = lower.Stat(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}
