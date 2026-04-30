package strategy_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func TestPutObjectAbortsOnReadError(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)

	key := cache.NewKey("abort-test")

	// Create a reader that returns an error after some data.
	body := &failingReader{data: []byte("partial data"), failAfter: 5}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/test/"+key.String(), body)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)

	// The partial data must not be cached.
	nsCache := memCache.Namespace("test")
	_, _, err = nsCache.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

// failingReader returns data up to failAfter bytes, then returns an error.
type failingReader struct {
	data      []byte
	failAfter int
	read      int
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.read >= r.failAfter {
		return 0, io.ErrUnexpectedEOF
	}
	n := min(len(p), r.failAfter-r.read, len(r.data)-r.read)
	copy(p[:n], r.data[r.read:r.read+n])
	r.read += n
	if r.read >= r.failAfter {
		return n, io.ErrUnexpectedEOF
	}
	return n, nil
}
