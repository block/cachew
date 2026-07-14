package cache

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// rangeGetCounters counts range GETs issued to S3, and how many of them lack
// an If-Match revision pin.
type rangeGetCounters struct {
	rangeGets    atomic.Int64
	unpinnedGets atomic.Int64
}

func (c *rangeGetCounters) reset() {
	c.rangeGets.Store(0)
	c.unpinnedGets.Store(0)
}

// newCountingS3 returns an S3 cache whose transport records range GETs in the
// returned counters. Uploads are forced serial because minio-go's parallel
// multipart upload races on a shared CRC digest and these tests only exercise
// downloads.
func newCountingS3(t *testing.T, cfg S3Config) (*S3, context.Context, *rangeGetCounters) {
	t.Helper()
	cfg.Bucket = s3clienttest.Start(t)
	cfg.MaxTTL = time.Hour
	cfg.UploadConcurrency = 1
	cfg.UploadPartSizeMB = 16
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	counters := &rangeGetCounters{}
	base, err := minio.DefaultTransport(false)
	assert.NoError(t, err)
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && req.Header.Get("Range") != "" {
			counters.rangeGets.Add(1)
			if req.Header.Get("If-Match") == "" {
				counters.unpinnedGets.Add(1)
			}
		}
		return base.RoundTrip(req)
	})
	client, err := minio.New(s3clienttest.Addr, &minio.Options{
		Creds:           credentials.NewStaticV4(s3clienttest.Username, s3clienttest.Password, ""),
		Secure:          false,
		Transport:       transport,
		TrailingHeaders: true,
	})
	assert.NoError(t, err)

	s, err := NewS3(ctx, cfg, func() (*minio.Client, error) { return client, nil })
	assert.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s, ctx, counters
}

func TestS3ParallelGetBackpressure(t *testing.T) {
	const concurrency = 2
	s, ctx, counters := newCountingS3(t, S3Config{
		DownloadPartSizeMB:  1,
		DownloadConcurrency: concurrency,
	})

	data := make([]byte, 16<<20)
	_, err := rand.New(rand.NewSource(1)).Read(data)
	assert.NoError(t, err)

	key := NewKey("parallel-get-backpressure")
	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, _, err := s.Open(ctx, key)
	assert.NoError(t, err)
	defer r.Close()

	buf := make([]byte, 1)
	_, err = io.ReadFull(r, buf)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	assert.True(t, counters.rangeGets.Load() <= 3*concurrency,
		"workers fetched %d chunks while the consumer stalled; the reorder window (2x concurrency pages) plus workers parked mid-write must bound fetches at <= %d",
		counters.rangeGets.Load(), 3*concurrency)

	rest, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.True(t, buf[0] == data[0] && bytes.Equal(data[1:], rest), "reassembled data differs from original")
	assert.NoError(t, r.Close())
}

func TestS3ParallelRangedGet(t *testing.T) {
	s, ctx, counters := newCountingS3(t, S3Config{
		DownloadPartSizeMB:  32,
		DownloadConcurrency: 8,
	})

	data := make([]byte, 24<<20)
	_, err := rand.New(rand.NewSource(2)).Read(data)
	assert.NoError(t, err)

	key := NewKey("parallel-ranged-get")
	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	t.Run("LargeRangeFansOut", func(t *testing.T) {
		counters.reset()

		start, length := int64(1<<20), int64(16<<20)
		r, _, err := s.Open(ctx, key, Range(start, start+length))
		assert.NoError(t, err)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		assert.True(t, bytes.Equal(data[start:start+length], got), "ranged read differs from original slice")
		assert.Equal(t, int64(4), counters.rangeGets.Load(),
			"an unaligned 16MiB range with concurrency 8 must split into 4 sub-requests of minRangePartSize")
		assert.Equal(t, int64(0), counters.unpinnedGets.Load(), "sub-range requests must be etag-pinned")
	})

	t.Run("SmallRangeSingleStream", func(t *testing.T) {
		counters.reset()

		start, length := int64(3<<20), int64(1<<20)
		r, _, err := s.Open(ctx, key, Range(start, start+length))
		assert.NoError(t, err)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		assert.True(t, bytes.Equal(data[start:start+length], got), "ranged read differs from original slice")
		assert.Equal(t, int64(1), counters.rangeGets.Load(),
			"a range below 2x minRangePartSize must be served by a single request")
		assert.Equal(t, int64(0), counters.unpinnedGets.Load(), "range request must be etag-pinned")
	})
}
