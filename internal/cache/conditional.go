package cache

import (
	"net/http"

	"github.com/alecthomas/errors"
)

// conditionalShortCircuit evaluates conditional opts against the stored ETag in
// headers. A nil error means the object should be served normally. A non-nil
// error short-circuits the request: ErrNotModified is returned together with
// headers (so callers can surface a 304 with the stored validators), while
// ErrPreconditionFailed is returned with nil headers.
func conditionalShortCircuit(headers http.Header, opts []Option) (http.Header, error) {
	err := errors.WithStack(NewRequestOptions(opts...).Check(headers.Get(ETagKey)))
	if errors.Is(err, ErrNotModified) {
		return headers, err
	}
	return nil, err
}
