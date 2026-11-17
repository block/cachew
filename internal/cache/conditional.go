package cache

import (
	"net/http"

	"github.com/alecthomas/errors"
)

// conditionalShortCircuit evaluates conditional opts against the stored ETag and normalises headers for serving.
//
// ErrNotModified returns headers for a 304; ErrPreconditionFailed returns nil headers. Content-Range
// is dropped because it is per-response framing that must never come from stored metadata.
func conditionalShortCircuit(headers http.Header, opts []Option) (http.Header, error) { //nolint:privatedoc // explains why
	headers.Del("Content-Range")
	err := errors.WithStack(NewRequestOptions(opts...).Check(headers.Get(ETagKey)))
	if errors.Is(err, ErrNotModified) {
		return headers, err
	}
	return nil, err
}
