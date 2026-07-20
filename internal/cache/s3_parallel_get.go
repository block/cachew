package cache

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/client"
)

// minRangePartSize is the smallest per-request part worth fetching in
// parallel for ranged reads; below this, per-request overhead outweighs the
// bandwidth gained from additional streams.
const minRangePartSize int64 = 4 << 20

// parallelGetReader returns a reader for a whole S3 object, fetched with
// parallel range-GET requests via [client.ParallelGetReader]. Objects that
// fit in one chunk use a single GetObject.
func (s *S3) parallelGetReader(ctx context.Context, bucket, objectName string, size int64, etag string) (io.ReadCloser, error) {
	chunkSize := int64(s.config.DownloadPartSizeMB) << 20                                      // #nosec G115 -- DownloadPartSizeMB is a small operator-supplied tuning value.
	if concurrency := int(s.config.DownloadConcurrency); size > chunkSize && concurrency > 1 { // #nosec G115 -- DownloadConcurrency is a small operator-supplied tuning value.
		window := &s3ObjectWindow{s3: s, bucket: bucket, objectName: objectName, start: 0, length: size, etag: etag}
		return client.ParallelGetReader(ctx, window, Key{}, chunkSize, concurrency) //nolint:wrapcheck
	}
	obj, err := s.client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Errorf("failed to get object: %w", err)
	}
	return &s3Reader{obj: obj}, nil
}

// rangedGetReader serves [start, start+length) of an S3 object. Large ranges
// are split into parallel sub-range requests because a single S3 stream is
// limited to a fraction of the available bandwidth; small ranges use a single
// request. Sub-range parts scale down below the configured part size (to a
// floor of minRangePartSize) so that ranges smaller than one configured part
// still fan out across multiple streams.
func (s *S3) rangedGetReader(ctx context.Context, bucket, objectName string, start, length int64, etag string) (io.ReadCloser, error) {
	concurrency := int64(s.config.DownloadConcurrency) // #nosec G115 -- DownloadConcurrency is a small operator-supplied tuning value.
	if length < 2*minRangePartSize || concurrency <= 1 {
		return s.rangeGetReader(ctx, bucket, objectName, start, length, etag)
	}
	chunkSize := (length + concurrency - 1) / concurrency
	chunkSize = min(max(chunkSize, minRangePartSize), int64(s.config.DownloadPartSizeMB)<<20) // #nosec G115 -- DownloadPartSizeMB is a small operator-supplied tuning value.
	window := &s3ObjectWindow{s3: s, bucket: bucket, objectName: objectName, start: start, length: length, etag: etag}
	return client.ParallelGetReader(ctx, window, Key{}, chunkSize, int(concurrency)) //nolint:wrapcheck
}

// rangeGetReader returns an io.ReadCloser for a single byte range of an S3
// object, pinned to etag (when non-empty) so the read sees a consistent
// object revision.
func (s *S3) rangeGetReader(ctx context.Context, bucket, objectName string, start, length int64, etag string) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(start, start+length-1); err != nil {
		return nil, errors.Errorf("set range %d-%d: %w", start, start+length-1, err)
	}
	if etag != "" {
		if err := opts.SetMatchETag(etag); err != nil {
			return nil, errors.Errorf("set etag %s: %w", etag, err)
		}
	}
	obj, err := s.client.GetObject(ctx, bucket, objectName, opts)
	if err != nil {
		return nil, errors.Errorf("failed to get object range: %w", err)
	}
	return &s3Reader{obj: obj}, nil
}

// s3ObjectWindow adapts a byte window of a pinned S3 object revision to
// [client.RangeReader], letting ParallelGet drive parallel S3 downloads.
// Range offsets are relative to the window, and every request carries an
// If-Match for the pinned etag regardless of the supplied options, so
// discovery and sub-range requests can never splice revisions. Response
// headers are synthesized from the pinned values: minio enforces the real
// preconditions (SetRange, SetMatchETag) at the protocol level, surfacing
// violations as read errors. The window's identity is bound in the struct, so
// the Key argument is ignored.
type s3ObjectWindow struct {
	s3         *S3
	bucket     string
	objectName string
	start      int64 // window offset within the object
	length     int64 // window size in bytes
	etag       string
}

func (w *s3ObjectWindow) Open(ctx context.Context, _ Key, opts ...Option) (io.ReadCloser, http.Header, error) {
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
	rc, err := w.s3.rangeGetReader(ctx, w.bucket, w.objectName, w.start+start, length, w.etag)
	if err != nil {
		return nil, nil, err
	}
	return rc, headers, nil
}
