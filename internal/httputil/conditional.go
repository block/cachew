package httputil

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
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

// IfRangeAllowsRange reports whether a Range request guarded by an If-Range
// header may be served as a partial response (RFC 9110 §13.1.5). It returns
// true when there is no If-Range header or the validator still matches the
// current representation; false means the validator is stale and the caller
// must serve the full object so a resuming client discards its partial copy.
//
// If-Range uses strong comparison: an ETag matches only if byte-identical to
// the stored (always strong) ETag, and a date matches only if equal to
// Last-Modified at second precision.
func IfRangeAllowsRange(r *http.Request, etag, lastModified string) bool {
	ir := strings.TrimSpace(r.Header.Get("If-Range"))
	if ir == "" {
		return true
	}
	if strings.HasPrefix(ir, `"`) || strings.HasPrefix(ir, "W/") {
		return etag != "" && ir == etag
	}
	if lastModified == "" {
		return false
	}
	irTime, err := http.ParseTime(ir)
	if err != nil {
		return false
	}
	lmTime, err := http.ParseTime(lastModified)
	if err != nil {
		return false
	}
	return irTime.Equal(lmTime)
}

// ParseByteRange parses a single HTTP Range header value against the object
// size, returning the resolved half-open [start, end) byte range. ok is false
// when the header is absent, malformed, or specifies multiple ranges, in which
// case callers should serve the full object. satisfiable is false when the
// range lies entirely outside the object, in which case callers should return
// 416 Range Not Satisfiable.
func ParseByteRange(header string, size int64) (start, end int64, ok, satisfiable bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(header), "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false, false
	}

	from, to, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false, false
	}
	from, to = strings.TrimSpace(from), strings.TrimSpace(to)

	// Suffix range: "-N" requests the final N bytes.
	if from == "" {
		n, err := strconv.ParseInt(to, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false, false
		}
		if size == 0 {
			return 0, 0, true, false
		}
		return max(0, size-n), size, true, true
	}

	start, err := strconv.ParseInt(from, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, false
	}
	if start >= size {
		return 0, 0, true, false
	}

	if to == "" {
		return start, size, true, true
	}
	last, err := strconv.ParseInt(to, 10, 64)
	if err != nil || last < start {
		return 0, 0, false, false
	}
	// Clamp before incrementing so a last value near math.MaxInt64 cannot
	// overflow to a negative end.
	if last >= size {
		end = size
	} else {
		end = last + 1
	}
	return start, end, true, true
}

// ServeCachePartial serves a 206 Partial Content response for the half-open
// [start, end) byte range of an object of the given total size. It copies the
// stored headers (whose Content-Length already reflects the partial length),
// sets Accept-Ranges and Content-Range, honours conditional preconditions, and
// streams the body. The body is always closed.
func ServeCachePartial(w http.ResponseWriter, r *http.Request, headers http.Header, body io.ReadCloser, start, end, size int64) error {
	maps.Copy(w.Header(), headers)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, size))
	if status := CheckConditionals(r, headers.Get(ETagHeader)); status != 0 {
		w.WriteHeader(status)
		return errors.WithStack(body.Close())
	}
	w.WriteHeader(http.StatusPartialContent)
	_, copyErr := io.Copy(w, body)
	return errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache partial")
}

// ServeRangeNotSatisfiable writes a 416 Range Not Satisfiable response with the
// Content-Range header set to the object size.
func ServeRangeNotSatisfiable(w http.ResponseWriter, size int64) {
	w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
	http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
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
