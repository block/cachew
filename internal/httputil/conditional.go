package httputil

import (
	"io"
	"maps"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// ConditionalOptions extracts conditional-request and range options from an
// incoming request, for forwarding to a cache Open or Stat. Range/If-Range are
// honoured by Open and ignored by Stat.
func ConditionalOptions(r *http.Request) []client.RequestOption {
	var opts []client.RequestOption
	if v := r.Header.Get("If-Match"); v != "" {
		opts = append(opts, client.IfMatch(v))
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		opts = append(opts, client.IfNoneMatch(v))
	}
	// Forward the client's Range header verbatim so the cache can honour forms
	// the typed client.Range API doesn't model (e.g. suffix "bytes=-N").
	if v := r.Header.Get("Range"); v != "" {
		opts = append(opts, func(o *client.RequestOptions) { o.Range = v })
	}
	if v := r.Header.Get("If-Range"); v != "" {
		opts = append(opts, client.IfRange(v))
	}
	return opts
}

// CheckConditionals evaluates RFC 7232 If-Match and If-None-Match precondition
// headers on r against etag. It returns 0 when all preconditions pass,
// otherwise the HTTP status the caller should send: 412 Precondition Failed for
// a failed If-Match, or 304 Not Modified for a satisfied If-None-Match. It is
// for callers that serve a body directly (not via ServeCacheHit) and need the
// status code.
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

// WithResponseDecorator registers fn to run after the stored headers have been
// copied to the response but before the status line is written, on the 200, 206
// and 416 paths. It lets endpoints that delegate range/conditional resolution to
// the cache still override or augment the response (e.g. force a Content-Type or
// advertise endpoint-specific metadata) uniformly across full and partial
// responses. fn is given the stored headers (so it can branch on, say, a
// Content-Range) and must not write the body or call WriteHeader.
func WithResponseDecorator(fn func(w http.ResponseWriter, stored http.Header)) ServeOption {
	return func(c *serveConfig) { c.decorate = fn }
}

// ServeCacheHit writes the outcome of a cache Open to w. headers and body are
// the Open return values and openErr its error. It handles the success and
// conditional cases: a nil error streams the body (always closing it), a
// satisfied If-None-Match (ErrNotModified) writes 304 with the stored headers,
// and a failed If-Match (ErrPreconditionFailed) writes 412. It returns
// handled=false for any other error (e.g. os.ErrNotExist) so the caller can map
// it to its own status, and n is the number of body bytes written (0 for
// bodiless responses).
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

// ServeCacheStat answers a metadata-only (HEAD) request from the outcome of a
// cache Stat. It mirrors ServeCacheHit without a body: success writes 200 with
// the stored headers, ErrNotModified writes 304 with headers, and
// ErrPreconditionFailed writes 412. It returns handled=false for any other
// error so the caller can map it to its own status.
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
