package httputil_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/httputil"
)

const testETag = `"abc123"`

func cacheHeaders(extra ...[2]string) http.Header {
	h := http.Header{}
	h.Set("ETag", testETag)
	for _, kv := range extra {
		h.Set(kv[0], kv[1])
	}
	return h
}

func newRequest(t *testing.T, ifMatch, ifNoneMatch string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if ifMatch != "" {
		r.Header.Set("If-Match", ifMatch)
	}
	if ifNoneMatch != "" {
		r.Header.Set("If-None-Match", ifNoneMatch)
	}
	return r
}

func TestCheckConditionals(t *testing.T) {
	tests := []struct {
		name        string
		ifMatch     string
		ifNoneMatch string
		etag        string
		want        int
	}{
		{name: "NoConditions", etag: testETag, want: 0},
		{name: "IfMatchExact", ifMatch: testETag, etag: testETag, want: 0},
		{name: "IfMatchWildcard", ifMatch: "*", etag: testETag, want: 0},
		{name: "IfMatchList", ifMatch: `"other", ` + testETag, etag: testETag, want: 0},
		{name: "IfMatchMismatch", ifMatch: `"other"`, etag: testETag, want: http.StatusPreconditionFailed},
		{name: "IfMatchNoETag", ifMatch: "*", etag: "", want: http.StatusPreconditionFailed},
		{name: "IfNoneMatchExact", ifNoneMatch: testETag, etag: testETag, want: http.StatusNotModified},
		{name: "IfNoneMatchWildcard", ifNoneMatch: "*", etag: testETag, want: http.StatusNotModified},
		{name: "IfNoneMatchList", ifNoneMatch: `"other", ` + testETag, etag: testETag, want: http.StatusNotModified},
		{name: "IfNoneMatchMismatch", ifNoneMatch: `"other"`, etag: testETag, want: 0},
		{name: "IfNoneMatchNoETag", ifNoneMatch: "*", etag: "", want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := httputil.CheckConditionals(newRequest(t, tt.ifMatch, tt.ifNoneMatch), tt.etag)
			assert.Equal(t, tt.want, got)
		})
	}
}

// trackingReader records whether Close was called, so tests can assert the body
// is always released.
type trackingReader struct {
	io.Reader
	closed bool
}

func (t *trackingReader) Close() error {
	t.closed = true
	return nil
}

func TestServeCacheHit(t *testing.T) {
	t.Run("StreamsBodyWhenFresh", func(t *testing.T) {
		body := &trackingReader{Reader: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Type", "text/plain"})
		w := httptest.NewRecorder()

		handled, err := httputil.ServeCacheHit(w, headers, body, nil)
		assert.True(t, handled)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testETag, resp.Header.Get("ETag"))
		assert.Equal(t, "text/plain", resp.Header.Get("Content-Type"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "payload", string(data))
	})

	t.Run("NotModifiedKeepsHeaders", func(t *testing.T) {
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		handled, err := httputil.ServeCacheHit(w, headers, nil, client.ErrNotModified)
		assert.True(t, handled)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotModified, resp.StatusCode)
		assert.Equal(t, testETag, resp.Header.Get("ETag"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "", string(data))
	})

	t.Run("PreconditionFailed", func(t *testing.T) {
		w := httptest.NewRecorder()

		handled, err := httputil.ServeCacheHit(w, nil, nil, client.ErrPreconditionFailed)
		assert.True(t, handled)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
	})

	t.Run("NotHandledForOtherError", func(t *testing.T) {
		w := httptest.NewRecorder()

		handled, err := httputil.ServeCacheHit(w, nil, nil, os.ErrNotExist)
		assert.False(t, handled)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, w.Result().StatusCode) // response untouched
	})
}

func TestServeCacheStat(t *testing.T) {
	t.Run("OKWhenFresh", func(t *testing.T) {
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		assert.True(t, httputil.ServeCacheStat(w, headers, nil))

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testETag, resp.Header.Get("ETag"))
	})

	t.Run("NotModified", func(t *testing.T) {
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		assert.True(t, httputil.ServeCacheStat(w, headers, client.ErrNotModified))

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotModified, resp.StatusCode)
		assert.Equal(t, testETag, resp.Header.Get("ETag"))
	})

	t.Run("NotHandledForOtherError", func(t *testing.T) {
		w := httptest.NewRecorder()
		assert.False(t, httputil.ServeCacheStat(w, nil, os.ErrNotExist))
	})
}
