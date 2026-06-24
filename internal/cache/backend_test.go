package cache_test

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestBackendKind(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer disk.Close()
	assert.Equal(t, "disk", cache.BackendKind(disk))

	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer mem.Close()
	assert.Equal(t, "memory", cache.BackendKind(mem))

	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{disk, mem})
	assert.Equal(t, "tiered", cache.BackendKind(tiered))

	assert.Equal(t, "unknown", cache.BackendKind(nil))
}

func TestBackendFromHeaders(t *testing.T) {
	assert.Equal(t, "", cache.BackendFromHeaders(nil))
	assert.Equal(t, "", cache.BackendFromHeaders(http.Header{}))

	h := http.Header{}
	h.Set(cache.ServedByHeader, "s3")
	assert.Equal(t, "s3", cache.BackendFromHeaders(h))
}

// TestTieredOpenAnnotatesServingTier verifies that Tiered.Open reports which
// tier produced the object via ServedByHeader, and that the annotation is not
// persisted into the backfilled lower-tier entry.
func TestTieredOpenAnnotatesServingTier(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	lower, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	upper, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, upper})
	defer tiered.Close()

	key := cache.NewKey("served-by-test")
	content := []byte("hello tier")

	// Seed only the upper tier so the first tiered Open hits upper and
	// backfills lower.
	w, err := upper.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, headers, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, content, data)
	assert.Equal(t, "memory", cache.BackendFromHeaders(headers))

	// The backfilled lower-tier entry must not carry the serving-tier
	// annotation; a subsequent direct read of the lower tier has no such header.
	lr, lowerHeaders, err := lower.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, lr.Close())
	assert.Equal(t, "", cache.BackendFromHeaders(lowerHeaders))

	// A direct read through the tiered cache now hits the (backfilled) lower
	// tier and is annotated accordingly.
	r2, headers2, err := tiered.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())
	assert.Equal(t, "disk", cache.BackendFromHeaders(headers2))
}
