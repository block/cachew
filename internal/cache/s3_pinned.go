package cache

import (
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
)

var _ PinnedRangeCache = (*S3)(nil)

// Pin returns an opaque revision token (the S3 ETag) for the object, so that
// subsequent range reads can be pinned to this exact revision.
func (s *S3) Pin(ctx context.Context, key Key) (PinnedObject, error) {
	objInfo, headers, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return PinnedObject{}, err
	}
	headers.Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))
	return PinnedObject{
		Pin:     pinETagPrefix + objInfo.ETag,
		Size:    objInfo.Size,
		Headers: headers,
	}, nil
}

// OpenPinnedRange serves bytes [start, min(end, total-1)] from the revision
// identified by pin. It stats the object first so a stale pin (the object was
// overwritten) fails closed with ErrPinStale before any data is written, and an
// out-of-range start returns ErrRangeNotSatisfiable. A SetMatchETag guard on the
// GET still covers the narrow stat→get race.
func (s *S3) OpenPinnedRange(ctx context.Context, key Key, pin string, start, end int64) (io.ReadCloser, int64, error) {
	etag, ok := strings.CutPrefix(pin, pinETagPrefix)
	if !ok {
		return nil, 0, errors.Errorf("unsupported pin token %q", pin)
	}

	objInfo, _, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	if objInfo.ETag != etag {
		return nil, 0, ErrPinStale
	}
	if start >= objInfo.Size {
		return nil, objInfo.Size, ErrRangeNotSatisfiable
	}
	if end >= objInfo.Size {
		end = objInfo.Size - 1
	}

	objectName := s.keyToPath(s.namespace, key)
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(start, end); err != nil {
		return nil, 0, errors.Errorf("set range %d-%d: %w", start, end, err)
	}
	if err := opts.SetMatchETag(etag); err != nil {
		return nil, 0, errors.Errorf("set etag %s: %w", etag, err)
	}

	// GetObject is lazy and must not be Stat()'d here: calling Stat() after
	// SetRange makes minio re-fetch the whole object. A 412 from the race guard
	// surfaces on Read and is mapped to ErrPinStale by s3Reader.Read.
	obj, err := s.client.GetObject(ctx, s.config.Bucket, objectName, opts)
	if err != nil {
		return nil, 0, errors.Errorf("get pinned range: %w", err)
	}
	return &s3Reader{obj: obj}, objInfo.Size, nil
}
