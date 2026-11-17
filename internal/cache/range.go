package cache

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alecthomas/errors"
)

// rangeShortCircuit resolves Range and If-Range opts against the object's size and stored ETag.
//
// A satisfiable single range sets Content-Range and Content-Length and returns ok=true; ok=false
// means serve the full object. An unsatisfiable range returns ErrRangeNotSatisfiable.
func rangeShortCircuit(headers http.Header, size int64, opts []Option) (start, length int64, ok bool, err error) { //nolint:privatedoc // documents a non-obvious contract
	s, l, outcome := NewRequestOptions(opts...).ResolveRange(size, headers.Get(ETagKey))
	switch outcome {
	case RangePartial:
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, s+l-1, size))
		headers.Set("Content-Length", strconv.FormatInt(l, 10))
		return s, l, true, nil

	case RangeNotSatisfiable:
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		// The 416 response carries no body.
		//
		// Drop the full-size Content-Length the backend set so clients don't wait for bytes that never arrive.
		headers.Del("Content-Length")
		return 0, 0, false, ErrRangeNotSatisfiable

	case RangeFull:
	}
	return 0, size, false, nil
}

// limitedReadCloser serves the first length bytes of a reader while delegating Close to the underlying closer.
type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func newLimitedReadCloser(rc io.ReadCloser, length int64) io.ReadCloser {
	return limitedReadCloser{Reader: io.LimitReader(rc, length), closer: rc}
}

func (l limitedReadCloser) Close() error { return errors.Wrap(l.closer.Close(), "close range reader") }

// WriteTo hands w the underlying *io.LimitedReader rather than this wrapper.
//
// net/http passes it to the connection's ReadFrom, which recognises a LimitedReader over an *os.File and serves a
// disk range with sendfile(2).
func (l limitedReadCloser) WriteTo(w io.Writer) (int64, error) {
	n, err := io.Copy(w, l.Reader)
	return n, errors.Wrap(err, "copy range")
}
