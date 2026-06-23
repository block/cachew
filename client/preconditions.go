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

// ErrRangeNotSatisfiable is returned when the server responds with 416 Range Not
// Satisfiable, indicating the requested byte range lies outside the object.
var ErrRangeNotSatisfiable = errors.New("range not satisfiable")

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

// WithByteRange sets a Range header requesting the half-open [start, end) byte
// range. An end of -1 requests from start to the end of the object. The server
// responds with 206 Partial Content.
func WithByteRange(start, end int64) RequestOption {
	return func(req *http.Request) {
		if end == -1 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
			return
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end-1))
	}
}
