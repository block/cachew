package cache_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
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

	_, headers, err := c.Open(ctx, key, cache.Range(0, 1))
	assert.IsError(t, err, cache.ErrRangeNotSatisfiable)
	assert.Equal(t, "bytes */0", headers.Get("Content-Range"))
}

// TestRangeStaleContentRangeStripped verifies that a Content-Range persisted in
// an object's stored headers (e.g. via a direct Cache.Create that bypasses the
// APIV1 PUT filter) is dropped on a full, non-range Open, so it can't be
// mistaken for a 206 partial response.
func TestRangeStaleContentRangeStripped(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("range-stale-cr")
	content := []byte("0123456789")
	stored := http.Header{"Content-Range": {"bytes 0-4/10"}}
	assert.NoError(t, cache.WriteFunc(ctx, c, key, stored, time.Hour, func(w io.Writer) error {
		_, err := w.Write(content)
		return err
	}))

	reader, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, content, data)
	assert.Equal(t, "", headers.Get("Content-Range"))

	// Stat (HEAD) ignores Range and never runs rangeShortCircuit, so it must also
	// drop the stale header rather than advertise partial metadata on a 200.
	statHeaders, err := c.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "", statHeaders.Get("Content-Range"))
}
