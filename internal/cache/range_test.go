package cache_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

// TestRangeEmptyObject verifies range handling for a zero-length object, which
// the in-memory backend (unlike S3) can store. Any range is unsatisfiable and
// the Content-Range reports the total size.
func TestRangeEmptyObject(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("range-empty")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	_, headers, err := c.Open(ctx, key, cache.Range("bytes=0-0"))
	assert.IsError(t, err, cache.ErrRangeNotSatisfiable)
	assert.Equal(t, "bytes */0", headers.Get("Content-Range"))
}
