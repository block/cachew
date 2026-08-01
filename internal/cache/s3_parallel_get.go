package cache

import (
	"context"
	"io"

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
		window := s.objectWindow(bucket, objectName, 0, size, etag)
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
	window := s.objectWindow(bucket, objectName, start, length, etag)
	return client.ParallelGetReader(ctx, window, Key{}, chunkSize, int(concurrency)) //nolint:wrapcheck
}

// objectWindow returns a window over [start, start+length) of the pinned S3
// object revision, served by direct sub-range GETs so chunk requests bypass
// S3.Open's per-call stat and range policy.
func (s *S3) objectWindow(bucket, objectName string, start, length int64, etag string) *objectWindow {
	return &objectWindow{
		openRange: func(ctx context.Context, start, length int64) (io.ReadCloser, error) {
			return s.rangeGetReader(ctx, bucket, objectName, start, length, etag)
		},
		start:  start,
		length: length,
		etag:   etag,
	}
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
