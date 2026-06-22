package httputil_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

const testETag = `"abc123"`

func cacheHeaders(extra ...[2]string) http.Header {
	h := http.Header{}
	h.Set(httputil.ETagHeader, testETag)
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
// is always released. It embeds an *strings.Reader so it satisfies
// io.ReadSeekCloser.
type trackingReader struct {
	io.ReadSeeker
	closed bool
}

func (t *trackingReader) Close() error {
	t.closed = true
	return nil
}

func TestServeCacheHit(t *testing.T) {
	t.Run("StreamsBodyWhenFresh", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Type", "text/plain"})
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, newRequest(t, "", ""), headers, body)
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

	t.Run("NotModifiedSkipsBody", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, newRequest(t, "", testETag), headers, body)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotModified, resp.StatusCode)
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "", string(data))
	})

	t.Run("PreconditionFailedSkipsBody", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, newRequest(t, `"other"`, ""), headers, body)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
	})

	t.Run("AdvertisesAcceptRangesOnFullRequest", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, newRequest(t, "", ""), headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "payload", string(data))
	})

	t.Run("ServesSingleRange", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=2-4")
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, "bytes 2-4/7", resp.Header.Get("Content-Range"))
		assert.Equal(t, "3", resp.Header.Get("Content-Length"))
		assert.Equal(t, "bytes", resp.Header.Get("Accept-Ranges"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "ylo", string(data))
	})

	t.Run("ServesOpenEndedRange", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=3-")
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, "bytes 3-6/7", resp.Header.Get("Content-Range"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "load", string(data))
	})

	t.Run("UnsatisfiableRange", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=10-20")
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, resp.StatusCode)
		assert.Equal(t, "bytes */7", resp.Header.Get("Content-Range"))
	})

	t.Run("MultiRangeServedInFull", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=0-1,3-4")
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "payload", string(data))
	})

	t.Run("IfRangeMatchingETagServesRange", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=2-4")
		r.Header.Set("If-Range", testETag)
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "ylo", string(data))
	})

	t.Run("IfRangeStaleETagServesFull", func(t *testing.T) {
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=2-4")
		r.Header.Set("If-Range", `"stale"`)
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "", resp.Header.Get("Content-Range"))
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "payload", string(data))
	})

	t.Run("IfRangeMatchingDateServesRange", func(t *testing.T) {
		const lastMod = "Mon, 02 Jan 2006 15:04:05 GMT"
		body := &trackingReader{ReadSeeker: strings.NewReader("payload")}
		headers := cacheHeaders([2]string{"Content-Length", "7"}, [2]string{"Last-Modified", lastMod})
		r := newRequest(t, "", "")
		r.Header.Set("Range", "bytes=2-4")
		r.Header.Set("If-Range", lastMod)
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, r, headers, body)
		assert.NoError(t, err)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		data, _ := io.ReadAll(resp.Body)
		assert.Equal(t, "ylo", string(data))
	})
}

func TestServeCacheStat(t *testing.T) {
	t.Run("OKWhenFresh", func(t *testing.T) {
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		httputil.ServeCacheStat(w, newRequest(t, "", ""), headers)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testETag, resp.Header.Get("ETag"))
	})

	t.Run("NotModified", func(t *testing.T) {
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		httputil.ServeCacheStat(w, newRequest(t, "", testETag), headers)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotModified, resp.StatusCode)
	})
}
