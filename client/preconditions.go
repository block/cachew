package client

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/alecthomas/errors"
)

// ETagKey is the HTTP header key used to store the ETag.
const ETagKey = "ETag"

// ErrNotModified is returned when an If-None-Match precondition is satisfied,
// indicating the resource has not changed since the supplied ETag. Over HTTP
// this corresponds to 304 Not Modified.
var ErrNotModified = errors.New("not modified")

// ErrPreconditionFailed is returned when an If-Match precondition is not met.
// Over HTTP this corresponds to 412 Precondition Failed.
var ErrPreconditionFailed = errors.New("precondition failed")

// ErrRangeNotSatisfiable is returned when a Range precondition cannot be
// satisfied against the stored object. Over HTTP this corresponds to 416 Range
// Not Satisfiable.
var ErrRangeNotSatisfiable = errors.New("range not satisfiable")

// RequestOptions holds conditional-request parameters. It is the single
// representation shared by the client wire protocol, the cache backends, and
// the server handlers.
type RequestOptions struct {
	// IfMatch is the If-Match precondition. Evaluation fails with
	// ErrPreconditionFailed if the stored ETag does not match.
	IfMatch string
	// IfNoneMatch is the If-None-Match precondition. Evaluation reports
	// ErrNotModified when the stored ETag matches.
	IfNoneMatch string
	// Range is a raw HTTP Range header value (e.g. "bytes=0-499"). Only a
	// single byte range is supported; multi-range or invalid specifiers are
	// ignored and the full representation is served.
	Range string
	// IfRange gates Range on the stored ETag: the range is only applied when
	// IfRange matches the stored ETag, otherwise the full representation is
	// served. Only the entity-tag form is supported.
	IfRange string
}

// RequestOption configures conditional request parameters.
type RequestOption func(*RequestOptions)

// IfMatch sets the If-Match precondition.
func IfMatch(etag string) RequestOption {
	return func(o *RequestOptions) { o.IfMatch = etag }
}

// IfNoneMatch sets the If-None-Match precondition.
func IfNoneMatch(etag string) RequestOption {
	return func(o *RequestOptions) { o.IfNoneMatch = etag }
}

// Range requests a single half-open byte range [start, end) from Open. A
// negative end means "to the end of the object" (its Content-Length). For
// example Range(0, 500) requests the first 500 bytes and Range(0, -1) the whole
// object. Open returns the matching bytes with a Content-Range header, or
// ErrRangeNotSatisfiable if the range lies outside the object.
func Range(start, end int64) RequestOption {
	spec := formatByteRange(start, end)
	return func(o *RequestOptions) { o.Range = spec }
}

// formatByteRange renders a half-open [start, end) range as an HTTP byte-range
// specifier. A negative end yields an open-ended range to the end of the object.
func formatByteRange(start, end int64) string {
	if end < 0 {
		return fmt.Sprintf("bytes=%d-", start)
	}
	return fmt.Sprintf("bytes=%d-%d", start, end-1)
}

// IfRange sets the If-Range precondition: the Range is only honoured when etag
// matches the stored ETag, otherwise the full representation is served.
func IfRange(etag string) RequestOption {
	return func(o *RequestOptions) { o.IfRange = etag }
}

// NewRequestOptions applies opts and returns the resulting RequestOptions.
func NewRequestOptions(opts ...RequestOption) RequestOptions {
	var o RequestOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Check evaluates the preconditions against the stored ETag. It returns
// ErrNotModified for a satisfied If-None-Match, ErrPreconditionFailed for a
// failed If-Match, or nil when all preconditions pass.
func (o RequestOptions) Check(etag string) error {
	if o.IfMatch != "" && (etag == "" || !etagListMatches(o.IfMatch, etag)) {
		return ErrPreconditionFailed
	}
	if o.IfNoneMatch != "" && etag != "" && etagListMatches(o.IfNoneMatch, etag) {
		return ErrNotModified
	}
	return nil
}

// applyToRequest sets the conditional headers on an outgoing request.
func (o RequestOptions) applyToRequest(req *http.Request) {
	if o.IfMatch != "" {
		req.Header.Set("If-Match", o.IfMatch)
	}
	if o.IfNoneMatch != "" {
		req.Header.Set("If-None-Match", o.IfNoneMatch)
	}
	if o.Range != "" {
		req.Header.Set("Range", o.Range)
	}
	if o.IfRange != "" {
		req.Header.Set("If-Range", o.IfRange)
	}
}

// RangeOutcome classifies how a Range request should be answered.
type RangeOutcome int

const (
	// RangeFull indicates the full representation should be served (no Range,
	// an unmatched If-Range, or an unsupported/invalid specifier).
	RangeFull RangeOutcome = iota
	// RangePartial indicates a single satisfiable byte range.
	RangePartial
	// RangeNotSatisfiable indicates the range lies outside the object.
	RangeNotSatisfiable
)

// ResolveRange evaluates the Range/If-Range options against an object of the
// given size and ETag. On RangePartial it returns the [start, start+length)
// window to serve.
func (o RequestOptions) ResolveRange(size int64, etag string) (start, length int64, outcome RangeOutcome) {
	if o.Range == "" {
		return 0, size, RangeFull
	}
	// If-Range only applies the range when its validator matches the stored
	// ETag; otherwise the client is told to serve the full representation.
	if o.IfRange != "" && o.IfRange != etag {
		return 0, size, RangeFull
	}
	return resolveByteRange(o.Range, size)
}

// resolveByteRange parses a single HTTP byte-range specifier against size.
// Multi-range and syntactically invalid specifiers yield RangeFull so the
// caller serves the full representation.
func resolveByteRange(spec string, size int64) (start, length int64, outcome RangeOutcome) {
	const prefix = "bytes="
	if !strings.HasPrefix(spec, prefix) {
		return 0, size, RangeFull
	}
	spec = strings.TrimSpace(spec[len(prefix):])
	if spec == "" || strings.ContainsRune(spec, ',') {
		return 0, size, RangeFull
	}
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, size, RangeFull
	}
	startStr = strings.TrimSpace(startStr)
	endStr = strings.TrimSpace(endStr)

	if startStr == "" {
		// Suffix range "-N": the final N bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return 0, size, RangeFull
		}
		if n <= 0 || size == 0 {
			return 0, size, RangeNotSatisfiable
		}
		if n > size {
			n = size
		}
		return size - n, n, RangePartial
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, size, RangeFull
	}
	if start >= size {
		return 0, size, RangeNotSatisfiable
	}
	if endStr == "" {
		// Open range "START-": to the end of the object.
		return start, size - start, RangePartial
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, size, RangeFull
	}
	if end >= size {
		end = size - 1
	}
	return start, end - start + 1, RangePartial
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
