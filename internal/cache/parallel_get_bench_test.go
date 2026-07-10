package cache_test

import (
	"context"
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

const (
	// benchObjectSize matches real git snapshots, which run to several GB.
	benchObjectSize  = 4 << 30
	benchChunkSize   = 8 << 20 // the cachew git restore default
	benchConcurrency = 8
)

// discardWriterAt is the io.WriterAt analogue of io.Discard, so benchmarks
// measure the get strategies rather than a destination file.
type discardWriterAt struct{}

func (discardWriterAt) WriteAt(p []byte, _ int64) (int, error) { return len(p), nil }

func newBenchDiskCache(b *testing.B) (context.Context, cache.Cache, cache.Key) {
	b.Helper()
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:    b.TempDir(),
		LimitMB: 2 * benchObjectSize >> 20,
		MaxTTL:  time.Hour,
	})
	assert.NoError(b, err)
	b.Cleanup(func() { _ = c.Close() })
	return ctx, c, writeBenchObject(ctx, b, c)
}

func newBenchS3Cache(b *testing.B) (context.Context, cache.Cache, cache.Key) {
	b.Helper()
	bucket := s3clienttest.Start(b)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint: s3clienttest.Addr,
		UseSSL:   false,
	})
	c, err := cache.NewS3(ctx, cache.S3Config{
		Bucket:            bucket,
		MaxTTL:            time.Hour,
		UploadPartSizeMB:  16,
		UploadConcurrency: 0, // all cores, to speed up the 4GiB setup upload
	}, clientProvider)
	assert.NoError(b, err)
	b.Cleanup(func() { _ = c.Close() })
	return ctx, c, writeBenchObject(ctx, b, c)
}

// writeBenchObject writes the object in pattern-block increments so setup
// never holds gigabytes in RAM.
func writeBenchObject(ctx context.Context, b *testing.B, c cache.Cache) cache.Key {
	b.Helper()
	pattern := make([]byte, 1<<20)
	for i := range pattern {
		pattern[i] = byte(i % 251)
	}
	key := cache.NewKey("bench-object")
	assert.NoError(b, cache.WriteFunc(ctx, c, key, nil, time.Hour, func(w io.Writer) error {
		for written := 0; written < benchObjectSize; written += len(pattern) {
			if _, err := w.Write(pattern); err != nil {
				return err
			}
		}
		return nil
	}))
	return key
}

func runGetBenchmarks(ctx context.Context, b *testing.B, c cache.Cache, key cache.Key) {
	b.Run("Sequential", func(b *testing.B) {
		b.SetBytes(benchObjectSize)
		for b.Loop() {
			rc, _, err := c.Open(ctx, key)
			assert.NoError(b, err)
			_, err = io.Copy(io.Discard, rc)
			assert.NoError(b, err)
			assert.NoError(b, rc.Close())
		}
	})

	b.Run("Parallel", func(b *testing.B) {
		b.SetBytes(benchObjectSize)
		for b.Loop() {
			assert.NoError(b, cache.ParallelGet(ctx, c, key, discardWriterAt{}, benchChunkSize, benchConcurrency))
		}
	})

	b.Run("ParallelStream", func(b *testing.B) {
		spillDir := b.TempDir()
		b.SetBytes(benchObjectSize)
		for b.Loop() {
			assert.NoError(b, cache.ParallelGetStream(ctx, c, key, io.Discard, benchChunkSize, benchConcurrency, spillDir))
		}
	})
}

func BenchmarkGet(b *testing.B) {
	b.Run("Disk", func(b *testing.B) {
		ctx, c, key := newBenchDiskCache(b)
		runGetBenchmarks(ctx, b, c, key)
	})
	b.Run("S3", func(b *testing.B) {
		ctx, c, key := newBenchS3Cache(b)
		runGetBenchmarks(ctx, b, c, key)
	})
}
