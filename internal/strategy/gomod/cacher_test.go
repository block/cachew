package gomod_test

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
	"github.com/block/cachew/internal/strategy/gomod"
)

func TestGoproxyCacherPutAbortsOnReadError(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	cacher := gomod.NewTestableGoproxyCacher(memCache)

	// Create a reader that fails after some bytes
	content := &failingReadSeeker{data: []byte("partial module data"), failAfter: 7}

	err = cacher.Put(ctx, "example.com/mod/@v/v1.0.0.zip", content)
	assert.Error(t, err)

	// The partial data must not be cached
	key := cache.NewKey("example.com/mod/@v/v1.0.0.zip")
	_, _, err = memCache.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

type failingReadSeeker struct {
	data      []byte
	failAfter int
	pos       int
}

func (r *failingReadSeeker) Read(p []byte) (int, error) {
	if r.pos >= r.failAfter {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.data[r.pos:])
	if r.pos+n > r.failAfter {
		n = r.failAfter - r.pos
	}
	r.pos += n
	if r.pos >= r.failAfter {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}

func (r *failingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = int(offset)
	case io.SeekCurrent:
		r.pos += int(offset)
	case io.SeekEnd:
		r.pos = len(r.data) + int(offset)
	}
	return int64(r.pos), nil
}
