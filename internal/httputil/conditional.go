package httputil

import (
	"io"
	"maps"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// ConditionalOptions extracts conditional and range options from r to forward to a cache Open or Stat.
//
// Open honours Range and If-Range; Stat ignores them.
func ConditionalOptions(r *http.Request) []client.RequestOption {
	var opts []client.RequestOption
	if v := r.Header.Get("If-Match"); v != "" {
		opts = append(opts, client.IfMatch(v))
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		opts = append(opts, client.IfNoneMatch(v))
	}
	// Forward Range verbatim so the cache can honour forms client.Range doesn't model, e.g. "bytes=-N".
	if v := r.Header.Get("Range"); v != "" {
		opts = append(opts, func(o *client.RequestOptions) { o.Range = v })
	}
	if v := r.Header.Get("If-Range"); v != "" {
		opts = append(opts, client.IfRange(v))
	}
	return opts
}

// CheckConditionals evaluates RFC 7232 If-Match and If-None-Match headers on r against etag.
//
// It returns 0 if all pass, 412 for a failed If-Match, or 304 for a satisfied If-None-Match. It is for callers
// that serve a body directly rather than via ServeCacheHit.
func CheckConditionals(r *http.Request, etag string) int {
	switch err := client.NewRequestOptions(ConditionalOptions(r)...).Check(etag); {
	case errors.Is(err, client.ErrNotModified):
		return http.StatusNotModified
	case errors.Is(err, client.ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	default:
		return 0
	}
}

// ServeOption configures ServeCacheHit.
type ServeOption func(*serveConfig)

type serveConfig struct {
	decorate func(http.ResponseWriter, http.Header)
}

// WithResponseDecorator runs fn after stored headers are copied and before a 200, 206 or 416 status is written.
//
// It lets endpoints that delegate ranges to the cache adjust headers such as Content-Type uniformly. fn gets
// the stored headers, e.g. to branch on Content-Range, and must not write the body or call WriteHeader.
func WithResponseDecorator(fn func(w http.ResponseWriter, stored http.Header)) ServeOption {
	return func(c *serveConfig) { c.decorate = fn }
}

// ServeCacheHit writes the outcome of a cache Open (headers, body and openErr) to w.
//
// nil streams and always closes body, ErrNotModified writes 304 with headers, and ErrPreconditionFailed 412.
// Other errors return handled=false for the caller to map; n is the body bytes written.
func ServeCacheHit(w http.ResponseWriter, headers http.Header, body io.ReadCloser, openErr error, opts ...ServeOption) (handled bool, n int64, err error) {
	var cfg serveConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	decorate := func() {
		if cfg.decorate != nil {
			cfg.decorate(w, headers)
		}
	}

	switch {
	case openErr == nil:
		maps.Copy(w.Header(), headers)
		w.Header().Set("Accept-Ranges", "bytes")
		decorate()
		// A Content-Range set by the cache signals a satisfied byte range.
		if headers.Get("Content-Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		}
		var copyErr error
		n, copyErr = io.Copy(w, body)
		return true, n, errors.Wrap(errors.Join(copyErr, body.Close()), "serve cache hit")

	case errors.Is(openErr, client.ErrNotModified):
		maps.Copy(w.Header(), headers)
		w.WriteHeader(http.StatusNotModified)
		return true, 0, nil

	case errors.Is(openErr, client.ErrPreconditionFailed):
		w.WriteHeader(http.StatusPreconditionFailed)
		return true, 0, nil

	case errors.Is(openErr, client.ErrRangeNotSatisfiable):
		maps.Copy(w.Header(), headers)
		w.Header().Set("Accept-Ranges", "bytes")
		decorate()
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true, 0, nil

	default:
		return false, 0, nil
	}
}

// ServeCacheStat answers a HEAD request from a cache Stat outcome, like ServeCacheHit without a body.
//
// Success writes 200 and ErrNotModified 304, both with headers; ErrPreconditionFailed writes 412. Other
// errors return handled=false for the caller to map.
func ServeCacheStat(w http.ResponseWriter, headers http.Header, statErr error) (handled bool) {
	switch {
	case statErr == nil:
		maps.Copy(w.Header(), headers)
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		return true

	case errors.Is(statErr, client.ErrNotModified):
		maps.Copy(w.Header(), headers)
		w.WriteHeader(http.StatusNotModified)
		return true

	case errors.Is(statErr, client.ErrPreconditionFailed):
		w.WriteHeader(http.StatusPreconditionFailed)
		return true

	default:
		return false
	}
}
