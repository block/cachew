package cache

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alecthomas/errors"
)

// rangeShortCircuit resolves Range/If-Range opts against an object of the given
// size and the stored ETag in headers. On a satisfiable single range it sets
// Content-Range, rewrites Content-Length to the range length, and returns the
// [start, start+length) window with ok=true. When no range applies it returns
// ok=false (serve the full object). An unsatisfiable range sets
// Content-Range: bytes */size and returns ErrRangeNotSatisfiable.
func rangeShortCircuit(headers http.Header, size int64, opts []Option) (start, length int64, ok bool, err error) {
	s, l, outcome := NewRequestOptions(opts...).ResolveRange(size, headers.Get(ETagKey))
	switch outcome {
	case RangePartial:
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, s+l-1, size))
		headers.Set("Content-Length", strconv.FormatInt(l, 10))
		return s, l, true, nil

	case RangeNotSatisfiable:
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		// The 416 response carries no body; drop the full-size Content-Length
		// the backend set so clients don't wait for bytes that never arrive.
		headers.Del("Content-Length")
		return 0, 0, false, ErrRangeNotSatisfiable

	case RangeFull:
	}
	return 0, size, false, nil
}

// limitedReadCloser serves the first length bytes of a reader while delegating
// Close to the underlying closer.
type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func newLimitedReadCloser(rc io.ReadCloser, length int64) io.ReadCloser {
	return limitedReadCloser{Reader: io.LimitReader(rc, length), closer: rc}
}

func (l limitedReadCloser) Close() error { return errors.Wrap(l.closer.Close(), "close range reader") }
