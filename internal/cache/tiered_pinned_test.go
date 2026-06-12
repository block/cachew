package cache_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

// TestTieredPinnedRangeFallthrough verifies the disk-first/S3-fallback pin path:
// a range is served from S3 while disk is cold, and after a tiered Open backfills
// the disk copy (persisting S3's content ETag) the same pin serves identical bytes
// from disk. The pin token is identical across both, so chunks stitch regardless
// of which tier answers each range.
func TestTieredPinnedRangeFallthrough(t *testing.T) {
	bucket := s3clienttest.Start(t)
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), LimitMB: 1024, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer disk.Close()

	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{Endpoint: s3clienttest.Addr, UseSSL: false})
	s3, err := cache.NewS3(ctx, cache.S3Config{Bucket: bucket, MaxTTL: time.Hour, UploadPartSizeMB: 16}, clientProvider)
	assert.NoError(t, err)
	defer s3.Close()

	key := cache.NewKey("tiered-pinned")
	data := make([]byte, 4<<20)
	_, err = rand.Read(data)
	assert.NoError(t, err)

	// Seed S3 only, leaving the disk tier cold.
	w, err := s3.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{disk, s3}).(cache.PinnedRangeCache)

	pin, err := tiered.Pin(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), pin.Size)

	stitch := func() []byte {
		t.Helper()
		chunk := int64(len(data)) / 2
		got := make([]byte, len(data))
		for _, b := range [][2]int64{{0, chunk - 1}, {chunk, int64(len(data)) - 1}} {
			r, total, err := tiered.OpenPinnedRange(ctx, key, pin.Pin, b[0], b[1])
			assert.NoError(t, err)
			assert.Equal(t, int64(len(data)), total)
			part, err := io.ReadAll(io.LimitReader(r, b[1]-b[0]+1))
			assert.NoError(t, err)
			assert.NoError(t, r.Close())
			copy(got[b[0]:], part)
		}
		return got
	}

	// Disk cold: served from S3.
	assert.True(t, bytes.Equal(data, stitch()), "S3-fallback ranges must match")

	// Warm the disk tier; backfill persists S3's content ETag onto the disk copy.
	r, _, err := tiered.(cache.Cache).Open(ctx, key)
	assert.NoError(t, err)
	_, err = io.Copy(io.Discard, r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	// Disk warm: same pin still serves identical bytes (now from disk).
	assert.True(t, bytes.Equal(data, stitch()), "disk-served ranges must match")
}
