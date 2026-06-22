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
// ETag, and either short-circuits with a 304/412 status, serves a single byte
// range, or streams the whole body. The body is always closed.
//
// This consolidates the validator-aware serving path shared by handlers that
// return a single cached object (e.g. the API and the generic caching handler).
//
// Ranges are parsed manually against the stored Content-Length rather than via
// http.ServeContent, because the body is only guaranteed to support a single
// seek-to-start followed by sequential reads (some backends stream over the
// network and cannot probe the end via Seek(0, io.SeekEnd)). Only single ranges
// are supported; multi-range requests are served in full.
func ServeCacheHit(w http.ResponseWriter, r *http.Request, headers http.Header, body io.ReadSeekCloser) error {
	maps.Copy(w.Header(), headers)
	w.Header().Set("Accept-Ranges", "bytes")

	if status := CheckConditionals(r, headers.Get(ETagHeader)); status != 0 {
		w.WriteHeader(status)
		return errors.WithStack(body.Close())
	}

	rangeHeader := r.Header.Get("Range")
	// A Range conditioned on a stale If-Range validator must be ignored, serving
	// the full representation so a resuming client cannot append bytes from a
	// different cached object (RFC 7233 §3.2).
	if rangeHeader != "" && !ifRangeAllowsRange(r, headers) {
		rangeHeader = ""
	}
	size, haveSize := parseContentLength(headers)
	if rangeHeader == "" || !haveSize {
		_, copyErr := io.Copy(w, body)
		return errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache hit")
	}

	start, end, satisfiable, ok := parseSingleRange(rangeHeader, size)
	if !ok {
		// Unsupported or multi-range: serve the whole object.
		_, copyErr := io.Copy(w, body)
		return errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache hit")
	}
	if !satisfiable {
		w.Header().Del("Content-Length")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return errors.WithStack(body.Close())
	}

	if _, err := body.Seek(start, io.SeekStart); err != nil {
		return errors.Wrap(errors.Join(err, body.Close()), "seek cache hit range")
	}
	length := end - start + 1
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, copyErr := io.CopyN(w, body, length)
	return errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache hit range")
}

// ifRangeAllowsRange reports whether a Range request should be served as a
// partial response given any If-Range validator. Per RFC 7233 §3.2 a Range is
// honoured only when If-Range is absent or its validator still matches the
// current representation; otherwise the full representation is served so a
// client resuming a stale download restarts cleanly.
func ifRangeAllowsRange(r *http.Request, headers http.Header) bool {
	ir := r.Header.Get("If-Range")
	if ir == "" {
		return true
	}
	// ETag form: If-Range requires a strong validator match. Stored ETags are
	// strong, so a weak ("W/"-prefixed) If-Range never matches.
	if strings.HasPrefix(ir, "\"") || strings.HasPrefix(ir, "W/") {
		etag := headers.Get(ETagHeader)
		return etag != "" && ir == etag
	}
	// HTTP-date form: matches when equal to Last-Modified.
	lastModified := headers.Get("Last-Modified")
	if lastModified == "" {
		return false
	}
	irTime, irErr := http.ParseTime(ir)
	lmTime, lmErr := http.ParseTime(lastModified)
	if irErr != nil || lmErr != nil {
		return false
	}
	return irTime.Equal(lmTime)
}

// parseContentLength extracts the object size from the stored Content-Length
// header.
func parseContentLength(headers http.Header) (int64, bool) {
	cl := headers.Get("Content-Length")
	if cl == "" {
		return 0, false
	}
	size, err := strconv.ParseInt(cl, 10, 64)
	if err != nil || size < 0 {
		return 0, false
	}
	return size, true
}

// parseSingleRange parses a single-range RFC 7233 "Range: bytes=..." header
// against the object size.
//
// ok reports whether spec is a usable single byte range (false for multi-range
// or malformed specs, which callers serve in full). When ok, satisfiable
// reports whether the range overlaps the object; an unsatisfiable range yields
// a 416. When both are true, start and end are the inclusive byte bounds.
func parseSingleRange(spec string, size int64) (start, end int64, satisfiable, ok bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(spec, prefix) {
		return 0, 0, false, false
	}
	spec = spec[len(prefix):]
	if strings.Contains(spec, ",") {
		return 0, 0, false, false // multi-range unsupported
	}
	before, after, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false, false
	}
	startStr := strings.TrimSpace(before)
	endStr := strings.TrimSpace(after)

	if startStr == "" {
		// Suffix form "bytes=-N": the final N bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false, false
		}
		if n > size {
			n = size
		}
		if n == 0 {
			// Zero final bytes, or a zero-length object: nothing to serve.
			return 0, 0, false, true // satisfiable=false → 416
		}
		return size - n, size - 1, true, true
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false, false
	}
	if start >= size {
		return 0, 0, false, true // satisfiable=false → 416
	}
	end = size - 1
	if endStr != "" {
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < start {
			return 0, 0, false, false
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, true, true
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
