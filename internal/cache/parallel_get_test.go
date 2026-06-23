package cache_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

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
		{name: "SerialFastPath", chunkSize: 100, concurrency: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, headers, err := cache.ParallelGet(ctx, c, key, tt.chunkSize, tt.concurrency)
			assert.NoError(t, err)
			defer reader.Close()
			assert.Equal(t, "1000", headers.Get("Content-Length"))
			data, err := io.ReadAll(reader)
			assert.NoError(t, err)
			assert.Equal(t, content, data)
		})
	}
}

func TestParallelGetNotFound(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	_, _, err = cache.ParallelGet(ctx, c, cache.NewKey("missing"), 100, 4)
	assert.IsError(t, err, os.ErrNotExist)
}
