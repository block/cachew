package client

import (
	"net/http"
	"strings"

	"github.com/alecthomas/errors"
)

// ErrNotModified is returned when an If-None-Match precondition is satisfied,
// indicating the resource has not changed since the supplied ETag. Over HTTP
// this corresponds to 304 Not Modified.
var ErrNotModified = errors.New("not modified")

// ErrPreconditionFailed is returned when an If-Match precondition is not met.
// Over HTTP this corresponds to 412 Precondition Failed.
var ErrPreconditionFailed = errors.New("precondition failed")

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
