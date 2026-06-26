package cache_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

// collect runs cache.ParallelGet into a StreamSink and returns the reassembled
// bytes, reading the sink concurrently as the engine requires.
func collect(ctx context.Context, c cache.Cache, key cache.Key, chunkSize int64, concurrency int) ([]byte, error) {
	sink := client.NewStreamSink(chunkSize, concurrency)
	type result struct {
		data []byte
		err  error
	}
	rc := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(sink)
		rc <- result{data: data, err: err}
	}()
	err := cache.ParallelGet(ctx, c, key, sink, chunkSize, concurrency)
	sink.Done(err)
	res := <-rc
	if err != nil {
		return res.data, err
	}
	return res.data, res.err
}

func TestParallelGet(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	content := make([]byte, 1000)
	for i := range content {
		content[i] = byte(i % 251)
	}
	key := cache.NewKey("parallel-get")
	assert.NoError(t, cache.WriteFunc(ctx, c, key, nil, time.Hour, func(w io.Writer) error {
		_, err := w.Write(content)
		return err
	}))

	tests := []struct {
		name        string
		chunkSize   int64
		concurrency int
	}{
		{name: "EvenChunks", chunkSize: 100, concurrency: 4},
		{name: "UnevenChunks", chunkSize: 300, concurrency: 3},
		{name: "SingleByteChunks", chunkSize: 1, concurrency: 8},
		{name: "ChunkLargerThanObject", chunkSize: 5000, concurrency: 4},
		{name: "SingleWorkerFullRead", chunkSize: 100, concurrency: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := collect(ctx, c, key, tt.chunkSize, tt.concurrency)
			assert.NoError(t, err)
			assert.Equal(t, content, got)
		})
	}
}

func TestParallelGetEmptyObject(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("parallel-empty")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// concurrency 4 takes the ranged discovery path (ErrRangeNotSatisfiable),
	// concurrency 1 takes the up-front full-read path; both must yield nothing.
	for _, concurrency := range []int{4, 1} {
		got, err := collect(ctx, c, key, 100, concurrency)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(got))
	}
}

func TestParallelGetNotFound(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	_, err = collect(ctx, c, cache.NewKey("missing"), 100, 4)
	assert.IsError(t, err, os.ErrNotExist)
}
