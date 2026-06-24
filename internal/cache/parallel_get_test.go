package cache_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

// bufferAt is an in-memory io.WriterAt that extends like a file, zero-filling
// any gap, so tests can assert reassembly without touching disk.
type bufferAt struct {
	mu  sync.Mutex
	buf []byte
}

func (b *bufferAt) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if end := int(off) + len(p); end > len(b.buf) {
		b.buf = append(b.buf, make([]byte, end-len(b.buf))...)
	}
	copy(b.buf[off:], p)
	return len(p), nil
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
		{name: "SerialFastPath", chunkSize: 100, concurrency: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dst bufferAt
			err := cache.ParallelGet(ctx, c, key, &dst, tt.chunkSize, tt.concurrency)
			assert.NoError(t, err)
			assert.Equal(t, content, dst.buf)
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

	var dst bufferAt
	assert.NoError(t, cache.ParallelGet(ctx, c, key, &dst, 100, 4))
	assert.Equal(t, 0, len(dst.buf))
}

func TestParallelGetNotFound(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	var dst bufferAt
	err = cache.ParallelGet(ctx, c, cache.NewKey("missing"), &dst, 100, 4)
	assert.IsError(t, err, os.ErrNotExist)
}
