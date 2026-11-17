package client

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/alecthomas/errors"
)

// ETagKey is the HTTP header key used to store the ETag.
const ETagKey = "ETag"

// ErrNotModified is returned when an If-None-Match precondition matches the stored ETag (HTTP 304 Not Modified).
var ErrNotModified = errors.New("not modified")

// ErrPreconditionFailed is returned when an If-Match precondition is not met (HTTP 412 Precondition Failed).
var ErrPreconditionFailed = errors.New("precondition failed")

// ErrRangeNotSatisfiable is returned when a Range lies outside the stored object (HTTP 416 Range Not Satisfiable).
var ErrRangeNotSatisfiable = errors.New("range not satisfiable")

// RequestOptions holds conditional-request parameters.
//
// It is the single representation shared by the client wire protocol, the cache backends, and the server handlers.
type RequestOptions struct {
	// IfMatch is the If-Match precondition; a non-matching stored ETag fails with ErrPreconditionFailed.
	IfMatch string
	// IfNoneMatch is the If-None-Match precondition; a matching stored ETag reports ErrNotModified.
	IfNoneMatch string
	// Range is a raw HTTP Range header value (e.g. "bytes=0-499").
	//
	// Only a single byte range is supported; multi-range or invalid specifiers serve the full representation.
	Range string
	// IfRange applies Range only when it matches the stored ETag, otherwise the full representation is served.
	//
	// Only the entity-tag form is supported.
	IfRange string
	// ETag is the raw ETag to store on Create, formatted as a strong HTTP ETag over HTTP and in cache metadata.
	ETag string
	// ETagSet reports whether ETag was explicitly configured.
	ETagSet bool
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

// Range requests a single half-open byte range [start, end) from Open; a negative end reads to the end.
//
// For example Range(0, 500) requests the first 500 bytes. Open returns the bytes with a Content-Range header,
// or ErrRangeNotSatisfiable if the range lies outside the object.
func Range(start, end int64) RequestOption {
	spec := formatByteRange(start, end)
	return func(o *RequestOptions) { o.Range = spec }
}

// formatByteRange renders a half-open [start, end) range as an HTTP byte-range specifier.
//
// A negative end yields an open-ended range to the end of the object.
func formatByteRange(start, end int64) string { //nolint:privatedoc // documents a non-obvious contract
	if end < 0 {
		return fmt.Sprintf("bytes=%d-", start)
	}
	return fmt.Sprintf("bytes=%d-%d", start, end-1)
}

// IfRange sets the If-Range precondition, so Range applies only when etag matches the stored ETag.
func IfRange(etag string) RequestOption {
	return func(o *RequestOptions) { o.IfRange = etag }
}

// WithETag sets the raw ETag value to store on Create.
func WithETag(etag string) RequestOption {
	return func(o *RequestOptions) {
		o.ETag = etag
		o.ETagSet = true
	}
}

// NewRequestOptions applies opts and returns the resulting RequestOptions.
func NewRequestOptions(opts ...RequestOption) RequestOptions {
	var o RequestOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Check evaluates the preconditions against the stored ETag.
//
// It returns ErrNotModified for a satisfied If-None-Match, ErrPreconditionFailed for a failed If-Match, or nil.
func (o RequestOptions) Check(etag string) error {
	if o.IfMatch != "" && (etag == "" || !etagListMatches(o.IfMatch, etag)) {
		return ErrPreconditionFailed
	}
	if o.IfNoneMatch != "" && etag != "" && etagListMatches(o.IfNoneMatch, etag) {
		return ErrNotModified
	}
	return nil
}

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
	// RangeFull serves the full representation: no Range, an unmatched If-Range, or an invalid specifier.
	RangeFull RangeOutcome = iota
	// RangePartial indicates a single satisfiable byte range.
	RangePartial
	// RangeNotSatisfiable indicates the range lies outside the object.
	RangeNotSatisfiable
)

// IfRangeMisses reports whether Range is dropped for the full representation because If-Range does not match etag.
func (o RequestOptions) IfRangeMisses(etag string) bool {
	return o.Range != "" && o.IfRange != "" && o.IfRange != etag
}

// ResolveRange evaluates Range and If-Range against an object of the given size and ETag.
//
// On RangePartial it returns the [start, start+length) window to serve.
func (o RequestOptions) ResolveRange(size int64, etag string) (start, length int64, outcome RangeOutcome) {
	if o.Range == "" || o.IfRangeMisses(etag) {
		return 0, size, RangeFull
	}
	return resolveByteRange(o.Range, size)
}

// resolveByteRange parses a single HTTP byte-range specifier against size.
//
// Multi-range and invalid specifiers yield RangeFull so the caller serves the full representation.
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

// etagListMatches reports whether etag matches an If-Match or If-None-Match header value.
//
// The value may be a comma-separated list or "*". Stored ETags are always strong, so weak
// comparison is not required.
func etagListMatches(headerValue, etag string) bool { //nolint:privatedoc // explains why
	for candidate := range strings.SplitSeq(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

// ValidateRawETag verifies that etag is an unquoted cache ETag value.
func ValidateRawETag(etag string) error {
	if etag == "" {
		return errors.New("etag must not be empty")
	}
	for _, r := range etag {
		if r > unicode.MaxASCII || !isRawETagChar(r) {
			return errors.Errorf("etag %q contains invalid character %q", etag, r)
		}
	}
	return nil
}

func isRawETagChar(r rune) bool {
	return 'a' <= r && r <= 'z' ||
		'A' <= r && r <= 'Z' ||
		'0' <= r && r <= '9' ||
		strings.ContainsRune("._-:/+=", r)
}

// FormatETag formats a raw ETag value as a strong HTTP ETag.
func FormatETag(etag string) (string, error) {
	if err := ValidateRawETag(etag); err != nil {
		return "", errors.WithStack(err)
	}
	return `"` + etag + `"`, nil
}

// RawETagFromHeader extracts a raw ETag value from a strong HTTP ETag header.
func RawETagFromHeader(etag string) (string, error) {
	if len(etag) < 2 || etag[0] != '"' || etag[len(etag)-1] != '"' {
		return "", errors.Errorf("etag header %q must be a strong quoted etag", etag)
	}
	raw := etag[1 : len(etag)-1]
	if err := ValidateRawETag(raw); err != nil {
		return "", errors.WithStack(err)
	}
	return raw, nil
}
