package strategy_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
	_, _, err = nsCache.Open(ctx, key, 0, -1)
	assert.IsError(t, err, os.ErrNotExist)
}

func testAPISetup(t *testing.T) (http.Handler, context.Context) {
	t.Helper()
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)
	return mux, ctx
}

func apiPut(ctx context.Context, t *testing.T, handler http.Handler, key cache.Key) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/test/"+key.String(), strings.NewReader("test data"))
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Stat to get the ETag
	req = httptest.NewRequest(http.MethodHead, "/api/v1/object/test/"+key.String(), nil)
	req = req.WithContext(ctx)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	etag := w.Header().Get("ETag")
	assert.NotZero(t, etag)
	return etag
}

func TestConditionalGetIfNoneMatch(t *testing.T) {
	handler, ctx := testAPISetup(t)
	key := cache.NewKey("cond-get")
	etag := apiPut(ctx, t, handler, key)

	tests := []struct {
		name           string
		ifNoneMatch    string
		expectedStatus int
	}{
		{name: "Matching", ifNoneMatch: etag, expectedStatus: http.StatusNotModified},
		{name: "NonMatching", ifNoneMatch: `"wrong"`, expectedStatus: http.StatusOK},
		{name: "Wildcard", ifNoneMatch: "*", expectedStatus: http.StatusNotModified},
		{name: "ListMatching", ifNoneMatch: `"a", ` + etag + `, "b"`, expectedStatus: http.StatusNotModified},
		{name: "ListNonMatching", ifNoneMatch: `"a", "b"`, expectedStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/object/test/"+key.String(), nil)
			req = req.WithContext(ctx)
			req.Header.Set("If-None-Match", tt.ifNoneMatch)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			assert.Equal(t, tt.expectedStatus, w.Code)
			if tt.expectedStatus == http.StatusNotModified {
				assert.Equal(t, etag, w.Header().Get("ETag"))
				assert.Equal(t, 0, w.Body.Len())
			}
		})
	}
}

func TestConditionalHeadIfNoneMatch(t *testing.T) {
	handler, ctx := testAPISetup(t)
	key := cache.NewKey("cond-head")
	etag := apiPut(ctx, t, handler, key)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/object/test/"+key.String(), nil)
	req = req.WithContext(ctx)
	req.Header.Set("If-None-Match", etag)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Equal(t, etag, w.Header().Get("ETag"))
}

func TestConditionalGetIfMatch(t *testing.T) {
	handler, ctx := testAPISetup(t)
	key := cache.NewKey("cond-ifmatch")
	etag := apiPut(ctx, t, handler, key)

	tests := []struct {
		name           string
		ifMatch        string
		expectedStatus int
	}{
		{name: "Matching", ifMatch: etag, expectedStatus: http.StatusOK},
		{name: "NonMatching", ifMatch: `"wrong"`, expectedStatus: http.StatusPreconditionFailed},
		{name: "Wildcard", ifMatch: "*", expectedStatus: http.StatusOK},
		{name: "ListMatching", ifMatch: `"a", ` + etag + `, "b"`, expectedStatus: http.StatusOK},
		{name: "ListNonMatching", ifMatch: `"a", "b"`, expectedStatus: http.StatusPreconditionFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/object/test/"+key.String(), nil)
			req = req.WithContext(ctx)
			req.Header.Set("If-Match", tt.ifMatch)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			assert.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

func TestGetObjectRange(t *testing.T) {
	handler, ctx := testAPISetup(t)
	key := cache.NewKey("range-get")
	// apiPut writes the body "test data" (9 bytes).
	etag := apiPut(ctx, t, handler, key)

	get := func(t *testing.T, rangeHeader, ifMatch, ifNoneMatch string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/object/test/"+key.String(), nil)
		req = req.WithContext(ctx)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}

	t.Run("PartialContent", func(t *testing.T) {
		w := get(t, "bytes=0-3", "", "")
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, "test", w.Body.String())
		assert.Equal(t, "bytes 0-3/9", w.Header().Get("Content-Range"))
		assert.Equal(t, "4", w.Header().Get("Content-Length"))
		assert.Equal(t, "bytes", w.Header().Get("Accept-Ranges"))
	})

	t.Run("Suffix", func(t *testing.T) {
		w := get(t, "bytes=-4", "", "")
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, "data", w.Body.String())
		assert.Equal(t, "bytes 5-8/9", w.Header().Get("Content-Range"))
	})

	t.Run("NotSatisfiable", func(t *testing.T) {
		w := get(t, "bytes=100-200", "", "")
		assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)
		assert.Equal(t, "bytes */9", w.Header().Get("Content-Range"))
	})

	t.Run("MultiRangeFallsBackToFull", func(t *testing.T) {
		w := get(t, "bytes=0-1,3-4", "", "")
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "test data", w.Body.String())
	})

	// Preconditions take precedence over the range (RFC 9110 §13.2.2).
	t.Run("IfNoneMatchBeatsRange", func(t *testing.T) {
		w := get(t, "bytes=0-3", "", etag)
		assert.Equal(t, http.StatusNotModified, w.Code)
		assert.Equal(t, 0, w.Body.Len())
		assert.Equal(t, etag, w.Header().Get("ETag"))
	})

	t.Run("IfMatchFailureBeatsRange", func(t *testing.T) {
		w := get(t, "bytes=0-3", `"wrong"`, "")
		assert.Equal(t, http.StatusPreconditionFailed, w.Code)
	})

	// A failed precondition wins even when the range is unsatisfiable.
	t.Run("IfMatchFailureBeatsUnsatisfiableRange", func(t *testing.T) {
		w := get(t, "bytes=100-200", `"wrong"`, "")
		assert.Equal(t, http.StatusPreconditionFailed, w.Code)
	})

	getIfRange := func(t *testing.T, rangeHeader, ifRange string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/object/test/"+key.String(), nil)
		req = req.WithContext(ctx)
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("If-Range", ifRange)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}

	// If-Range with the current validator serves the partial response.
	t.Run("IfRangeMatchServesPartial", func(t *testing.T) {
		w := getIfRange(t, "bytes=0-3", etag)
		assert.Equal(t, http.StatusPartialContent, w.Code)
		assert.Equal(t, "test", w.Body.String())
	})

	// If-Range with a stale validator must serve the full current object so a
	// resuming client discards its partial copy (RFC 9110 §13.1.5).
	t.Run("IfRangeStaleServesFull", func(t *testing.T) {
		w := getIfRange(t, "bytes=0-3", `"stale"`)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "test data", w.Body.String())
		assert.Zero(t, w.Header().Get("Content-Range"))
	})
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
