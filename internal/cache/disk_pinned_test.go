package cache_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func newPinnableDisk(t *testing.T) *cache.Disk {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), MaxTTL: time.Minute})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func writeDiskEntry(t *testing.T, c cache.Cache, key cache.Key, headers http.Header, data []byte) {
	t.Helper()
	w, err := c.Create(t.Context(), key, headers, 0)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
}

// TestDiskPinnedRange verifies that a disk entry carrying a content ETag (as set
// by S3 and persisted via tiered backfill) serves byte ranges that stitch back
// to the original, and that the wrong token is reported absent so the tiered
// cache falls through to S3.
func TestDiskPinnedRange(t *testing.T) {
	c := newPinnableDisk(t)
	ctx := t.Context()
	key := cache.NewKey("disk-pinned")

	data := make([]byte, 4<<20)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	writeDiskEntry(t, c, key, http.Header{cache.ContentETagHeader: {"etag-abc"}}, data)

	pin, err := c.Pin(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), pin.Size)
	assert.Equal(t, cache.ETagPin("etag-abc"), pin.Pin)

	chunk := int64(len(data)) / 2
	got := make([]byte, len(data))
	for _, b := range [][2]int64{{chunk, int64(len(data)) - 1}, {0, chunk - 1}} {
		r, total, err := c.OpenPinnedRange(ctx, key, pin.Pin, b[0], b[1])
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), total)
		part, err := io.ReadAll(io.LimitReader(r, b[1]-b[0]+1))
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		copy(got[b[0]:], part)
	}
	assert.True(t, bytes.Equal(data, got), "stitched disk ranges must match original")

	// A start past EOF is reported as not satisfiable.
	_, _, err = c.OpenPinnedRange(ctx, key, pin.Pin, int64(len(data)), int64(len(data))+10)
	assert.IsError(t, err, cache.ErrRangeNotSatisfiable)

	// A token for a different revision must not be served from disk.
	_, _, err = c.OpenPinnedRange(ctx, key, cache.ETagPin("etag-other"), 0, chunk-1)
	assert.IsError(t, err, os.ErrNotExist)
}

// TestDiskPinnedRangeNotPinnableWithoutETag verifies that a locally generated
// disk entry (never round-tripped through S3, so no content ETag) is not
// pinnable, forcing callers to the authoritative shared tier.
func TestDiskPinnedRangeNotPinnableWithoutETag(t *testing.T) {
	c := newPinnableDisk(t)
	ctx := t.Context()
	key := cache.NewKey("disk-no-etag")

	writeDiskEntry(t, c, key, nil, []byte("hello world"))

	_, err := c.Pin(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	_, _, err = c.OpenPinnedRange(ctx, key, cache.ETagPin("anything"), 0, 4)
	assert.IsError(t, err, os.ErrNotExist)
}
