package httputil

import (
	"io"
	"maps"
	"net/http"
	"strings"

	"github.com/alecthomas/errors"
)

// ETagHeader is the HTTP header used to carry an object's entity tag.
const ETagHeader = "ETag"

// CheckConditionals evaluates RFC 7232 If-Match and If-None-Match precondition
// headers against the stored ETag. It returns 0 when all preconditions pass,
// otherwise the HTTP status code the caller should send: 412 Precondition
// Failed for a failed If-Match, or 304 Not Modified for a satisfied
// If-None-Match.
func CheckConditionals(r *http.Request, etag string) int {
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if etag == "" || !etagListMatches(ifMatch, etag) {
			return http.StatusPreconditionFailed
		}
	}
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if etag != "" && etagListMatches(ifNoneMatch, etag) {
			return http.StatusNotModified
		}
	}
	return 0
}

// etagListMatches reports whether etag matches an If-Match / If-None-Match
// header value, which may be a comma-separated list of ETags or the "*"
// wildcard. Stored ETags are always strong, so weak comparison is not required.
func etagListMatches(headerValue, etag string) bool {
	for candidate := range strings.SplitSeq(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

// ServeCacheHit serves a cache hit over HTTP. It copies the stored headers onto
// the response, evaluates conditional request preconditions against the stored
// ETag, and either short-circuits with a 304/412 status or streams the body.
// The body is always closed.
//
// This consolidates the validator-aware serving path shared by handlers that
// return a single cached object (e.g. the API and the generic caching handler).
func ServeCacheHit(w http.ResponseWriter, r *http.Request, headers http.Header, body io.ReadCloser) error {
	maps.Copy(w.Header(), headers)
	if status := CheckConditionals(r, headers.Get(ETagHeader)); status != 0 {
		w.WriteHeader(status)
		return errors.WithStack(body.Close())
	}
	_, copyErr := io.Copy(w, body)
	return errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache hit")
}

// ServeCacheStat answers a metadata-only (HEAD) request from stored headers. It
// copies the headers onto the response and writes the status determined by the
// conditional request preconditions, defaulting to 200 OK.
func ServeCacheStat(w http.ResponseWriter, r *http.Request, headers http.Header) {
	maps.Copy(w.Header(), headers)
	status := CheckConditionals(r, headers.Get(ETagHeader))
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
}
