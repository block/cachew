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
		body := &trackingReader{Reader: strings.NewReader("payload")}
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
		body := &trackingReader{Reader: strings.NewReader("payload")}
		headers := cacheHeaders()
		w := httptest.NewRecorder()

		err := httputil.ServeCacheHit(w, newRequest(t, `"other"`, ""), headers, body)
		assert.NoError(t, err)
		assert.True(t, body.closed)

		resp := w.Result()
		defer resp.Body.Close()
		assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
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

func TestParseByteRange(t *testing.T) {
	const size = 10
	for _, tt := range []struct {
		name            string
		header          string
		start, end      int64
		ok, satisfiable bool
	}{
		{name: "Prefix", header: "bytes=0-3", start: 0, end: 4, ok: true, satisfiable: true},
		{name: "Middle", header: "bytes=3-6", start: 3, end: 7, ok: true, satisfiable: true},
		{name: "OpenEnded", header: "bytes=5-", start: 5, end: 10, ok: true, satisfiable: true},
		{name: "Suffix", header: "bytes=-4", start: 6, end: 10, ok: true, satisfiable: true},
		{name: "ClampEnd", header: "bytes=8-100", start: 8, end: 10, ok: true, satisfiable: true},
		{name: "EndOverflow", header: "bytes=8-9223372036854775807", start: 8, end: 10, ok: true, satisfiable: true},
		{name: "StartPastSize", header: "bytes=10-", ok: true, satisfiable: false},
		{name: "SuffixTooBig", header: "bytes=-100", start: 0, end: 10, ok: true, satisfiable: true},
		{name: "Reversed", header: "bytes=6-3"},
		{name: "Multiple", header: "bytes=0-1,3-4"},
		{name: "Garbage", header: "bytes=abc"},
		{name: "NoPrefix", header: "0-3"},
		{name: "Empty", header: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			start, end, ok, satisfiable := httputil.ParseByteRange(tt.header, size)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.satisfiable, satisfiable)
			if ok && satisfiable {
				assert.Equal(t, tt.start, start)
				assert.Equal(t, tt.end, end)
			}
		})
	}
}
