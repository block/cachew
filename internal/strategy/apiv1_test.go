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

// setupAPIV1 creates a memory cache, registers the APIV1 strategy, and stores
// a test object. Returns the mux, context, key, and stored ETag.
func setupAPIV1(t *testing.T) (http.Handler, context.Context, cache.Key, string) {
	t.Helper()
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)

	key := cache.NewKey("etag-test")

	// Store an object
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("hello etag"))
	req.Header.Set("Content-Type", "text/plain")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// GET to retrieve the ETag
	req = httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req = req.WithContext(ctx)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	etag := w.Header().Get("ETag")
	assert.NotZero(t, etag)

	return mux, ctx, key, etag
}

func TestGetObjectIfNoneMatchHit(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-None-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Equal(t, etag, w.Header().Get("ETag"))
	assert.Equal(t, "", w.Body.String()) // no body on 304
}

func TestGetObjectIfNoneMatchMiss(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-None-Match", `"wrong-etag"`)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello etag", w.Body.String())
}

func TestHeadObjectIfNoneMatchHit(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-None-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotModified, w.Code)
	assert.Equal(t, etag, w.Header().Get("ETag"))
}

func TestPutObjectIfNoneMatchStar(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	// Try to create-if-absent — should fail because object already exists
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("new data"))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestPutObjectIfNoneMatchStarNewKey(t *testing.T) {
	mux, ctx, _, _ := setupAPIV1(t)

	// Create-if-absent on a new key — should succeed
	newKey := cache.NewKey("brand-new-key")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+newKey.String(),
		strings.NewReader("new data"))
	req.Header.Set("If-None-Match", "*")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestPutObjectIfMatchStarNewKey(t *testing.T) {
	mux, ctx, _, _ := setupAPIV1(t)

	// If-Match: * on a non-existent key — should fail
	newKey := cache.NewKey("nonexistent-key")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+newKey.String(),
		strings.NewReader("new data"))
	req.Header.Set("If-Match", "*")
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestPutObjectIfMatchCorrect(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	// Conditional overwrite with correct ETag — should succeed
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("updated data"))
	req.Header.Set("If-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestPutObjectIfMatchWrong(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	// Conditional overwrite with wrong ETag — should fail
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("updated data"))
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestDeleteObjectIfMatchCorrect(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify object is actually deleted
	req = httptest.NewRequest(http.MethodHead, "/api/v1/object/default/"+key.String(), nil)
	req = req.WithContext(ctx)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestDeleteObjectIfMatchWrong(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)

	// Verify object still exists
	req = httptest.NewRequest(http.MethodHead, "/api/v1/object/default/"+key.String(), nil)
	req = req.WithContext(ctx)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHeadObjectIfMatchCorrect(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHeadObjectIfMatchWrong(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodHead, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestPutObjectIfNoneMatchSpecificETag(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	// If-None-Match with the existing ETag should fail
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("new data"))
	req.Header.Set("If-None-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}

func TestPutObjectReturnsNewETag(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	// Overwrite with new content
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("updated content"))
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	newETag := w.Header().Get("ETag")
	assert.NotZero(t, newETag, "PUT response should include the new ETag")
	assert.True(t, strings.HasPrefix(newETag, `"sha256:`), "ETag should be sha256-based")
}

func TestOverwriteChangesETag(t *testing.T) {
	mux, ctx, key, originalETag := setupAPIV1(t)

	// Overwrite with different content
	req := httptest.NewRequest(http.MethodPost, "/api/v1/object/default/"+key.String(),
		strings.NewReader("different content"))
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// GET and verify ETag changed
	req = httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req = req.WithContext(ctx)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	newETag := w.Header().Get("ETag")
	assert.NotEqual(t, originalETag, newETag, "ETag should change when content changes")
}

func TestGetObjectIfMatchCorrect(t *testing.T) {
	mux, ctx, key, etag := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", etag)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "hello etag", w.Body.String())
}

func TestGetObjectIfMatchWrong(t *testing.T) {
	mux, ctx, key, _ := setupAPIV1(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/object/default/"+key.String(), nil)
	req.Header.Set("If-Match", `"wrong-etag"`)
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusPreconditionFailed, w.Code)
}
