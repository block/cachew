package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// objectWindow adapts a byte window of a pinned object revision to
// [client.RangeReader], letting ParallelGet drive parallel range downloads.
// Range offsets are relative to the window; openRange receives absolute
// object offsets and must pin every request to the window's revision, so
// discovery and sub-range requests can never splice revisions. Response
// headers are synthesized from the pinned values: the backend enforces the
// real preconditions, surfacing violations as read errors. The window's
// identity is bound at construction, so the Key argument is ignored.
type objectWindow struct {
	openRange func(ctx context.Context, start, length int64) (io.ReadCloser, error)
	start     int64 // window offset within the object
	length    int64 // window size in bytes
	etag      string
}

// newCacheObjectWindow returns a window over [start, start+length) of the
// pinned revision of key in c, served by ETag-conditional ranged Opens.
func newCacheObjectWindow(c client.RangeReader, key Key, start, length int64, etag string) *objectWindow {
	return &objectWindow{
		openRange: func(ctx context.Context, start, length int64) (io.ReadCloser, error) {
			rc, _, err := c.Open(ctx, key, Range(start, start+length), IfMatch(etag))
			return rc, errors.WithStack(err)
		},
		start:  start,
		length: length,
		etag:   etag,
	}
}

func (w *objectWindow) Open(ctx context.Context, _ Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	start, length, outcome := NewRequestOptions(opts...).ResolveRange(w.length, w.etag)
	headers := http.Header{}
	if w.etag != "" {
		headers.Set(ETagKey, w.etag)
	}
	switch outcome {
	case RangeNotSatisfiable:
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", w.length))
		return nil, headers, errors.WithStack(ErrRangeNotSatisfiable)
	case RangePartial:
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, w.length))
	case RangeFull:
	}
	rc, err := w.openRange(ctx, w.start+start, length)
	if err != nil {
		return nil, nil, err
	}
	return rc, headers, nil
}
