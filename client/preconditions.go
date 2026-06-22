package client

import (
	"fmt"
	"net/http"

	"github.com/alecthomas/errors"
)

// ErrNotModified is returned when the server responds with 304 Not Modified,
// indicating the resource has not changed since the ETag in If-None-Match.
var ErrNotModified = errors.New("not modified")

// ErrPreconditionFailed is returned when the server responds with 412
// Precondition Failed, indicating an If-Match or If-None-Match condition was not met.
var ErrPreconditionFailed = errors.New("precondition failed")

// RequestOption configures conditional headers on an outgoing cache request.
type RequestOption func(req *http.Request)

// IfMatch sets the If-Match header. The server will return 412 Precondition
// Failed if the stored ETag does not match.
func IfMatch(etag string) RequestOption {
	return func(req *http.Request) {
		req.Header.Set("If-Match", etag)
	}
}

// IfNoneMatch sets the If-None-Match header. For GET/HEAD the server returns
// 304 Not Modified when the ETag matches; for other methods it returns 412.
func IfNoneMatch(etag string) RequestOption {
	return func(req *http.Request) {
		req.Header.Set("If-None-Match", etag)
	}
}

// byteRange sets a bounded "bytes=start-end" Range header (both ends inclusive).
//
// It is internal: ranged reads are driven by Open's seek-then-read path, which
// returns whole-object headers and reads a slice. Exposing a bounded Range as a
// public Open option would return a short body alongside those whole-object
// headers, so a dedicated ranged-fetch API (returning the 206 headers) is the
// right home for that when it is needed.
func byteRange(start, end int64) RequestOption {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
}
