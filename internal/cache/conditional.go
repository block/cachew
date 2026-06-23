package cache

import (
	"net/http"

	"github.com/alecthomas/errors"
)

// conditionalShortCircuit evaluates conditional opts against the stored ETag in
// headers and normalises the stored metadata for serving. A nil error means the
// object should be served normally. A non-nil error short-circuits the request:
// ErrNotModified is returned together with headers (so callers can surface a 304
// with the stored validators), while ErrPreconditionFailed is returned with nil
// headers.
//
// Content-Range is a per-response framing header that must never originate from
// stored metadata, so it is dropped here: this runs for both Stat and Open, so
// Stat never advertises a range and Open only carries one when rangeShortCircuit
// sets it for an actual partial response.
func conditionalShortCircuit(headers http.Header, opts []Option) (http.Header, error) {
	headers.Del("Content-Range")
	err := errors.WithStack(NewRequestOptions(opts...).Check(headers.Get(ETagKey)))
	if errors.Is(err, ErrNotModified) {
		return headers, err
	}
	return nil, err
}
