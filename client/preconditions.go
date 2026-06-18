package client

import (
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
