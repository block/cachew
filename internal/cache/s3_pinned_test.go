package cache_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

// TestS3PinnedRange verifies that an object can be pinned and reassembled
// byte-for-byte from independent ranged reads, and that overwriting the object
// invalidates the old pin (fail-closed) rather than mixing revisions.
func TestS3PinnedRange(t *testing.T) {
	bucket := s3clienttest.Start(t)
	c := newS3Cache(t, bucket)
	defer c.Close()

	pc, ok := c.(cache.PinnedRangeCache)
	assert.True(t, ok, "S3 cache must implement PinnedRangeCache")

	ctx := t.Context()
	key := cache.NewKey("pinned-object")

	// 10 MiB of random data so multiple distinct ranges are exercised.
	data := make([]byte, 10<<20)
	_, err := rand.Read(data)
	assert.NoError(t, err)

	w, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	pin, err := pc.Pin(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), pin.Size)
	assert.NotZero(t, pin.Pin)

	// Reassemble from four ranges in arbitrary order.
	chunk := int64(len(data)) / 4
	bounds := [][2]int64{
		{2 * chunk, 3*chunk - 1},
		{0, chunk - 1},
		{3 * chunk, int64(len(data)) - 1},
		{chunk, 2*chunk - 1},
	}
	got := make([]byte, len(data))
	for _, b := range bounds {
		r, total, err := pc.OpenPinnedRange(ctx, key, pin.Pin, b[0], b[1])
		assert.NoError(t, err)
		assert.Equal(t, int64(len(data)), total)
		part, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		assert.Equal(t, b[1]-b[0]+1, int64(len(part)))
		copy(got[b[0]:], part)
	}
	assert.True(t, bytes.Equal(data, got), "stitched ranges must match original")

	// Overwrite the object: the old pin must now fail closed up front.
	w2, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = w2.Write(make([]byte, len(data)))
	assert.NoError(t, err)
	assert.NoError(t, w2.Close())

	_, _, err = pc.OpenPinnedRange(ctx, key, pin.Pin, 0, chunk-1)
	assert.IsError(t, err, cache.ErrPinStale)
}
