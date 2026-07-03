package client

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

// RangeReader is the subset of cache operations ParallelGet needs: a
// conditional, Range-capable Open. Both *Client and the cache.Cache interface
// satisfy it, so ParallelGet can drive either a remote cache server or a local
// backend.
type RangeReader interface {
	Open(ctx context.Context, key Key, opts ...RequestOption) (io.ReadCloser, http.Header, error)
}

// ParallelGet downloads an object from any Range-capable RangeReader into dst,
// fetching it in chunkSize-byte chunks concurrently (up to concurrency requests
// in flight) and writing each chunk at its offset via dst.WriteAt. Latency-bound
// backends such as a remote cache can saturate bandwidth with overlapping reads.
//
// The first chunk is fetched with a ranged Open, whose response yields both the
// total size (from Content-Range) and the object's ETag; every remaining chunk
// is then requested with IfMatch pinned to that ETag. If the object changes
// mid-download, the chunk is rejected with a bodiless ErrPreconditionFailed
// (412) and ParallelGet returns an error rather than splicing bytes from two
// revisions; a server that ignores If-Match is caught by verifying each chunk's
// response ETag. A missing or truncated chunk is likewise reported as an error,
// so a partially written dst must be discarded by the caller on failure. An
// object with no ETag to pin to cannot be kept revision-safe across chunks, so
// it falls back to a single full read instead of parallelising. A concurrency of
// 1 likewise reads the whole object in one request, since chunking a single
// worker would only serialise ranged GETs for no benefit.
//
// dst is written via concurrent WriteAt calls at non-overlapping offsets; the
// caller owns dst's lifecycle (open, close, cleanup) and need not pre-size it,
// as WriteAt extends the destination.
func ParallelGet(ctx context.Context, c RangeReader, key Key, dst io.WriterAt, chunkSize int64, concurrency int) error {
	if chunkSize <= 0 {
		return errors.Errorf("parallel get: chunk size must be positive, got %d", chunkSize)
	}
	concurrency = max(concurrency, 1)

	if concurrency == 1 {
		return fullRead(ctx, c, key, dst)
	}

	rc, headers, err := c.Open(ctx, key, Range(0, chunkSize))
	if errors.Is(err, ErrRangeNotSatisfiable) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "parallel get: open first chunk")
	}

	etag := headers.Get(ETagKey)
	total, hasRange := parseContentRangeTotal(headers.Get("Content-Range"))

	firstLen := min(chunkSize, total)
	if !hasRange {
		firstLen = -1
	}
	if !hasRange || total <= chunkSize {
		return errors.Wrap(writeChunkAt(dst, 0, firstLen, rc), "parallel get")
	}

	if etag == "" {
		if err := rc.Close(); err != nil {
			return errors.Wrap(err, "parallel get: close discovery reader")
		}
		return fullRead(ctx, c, key, dst)
	}

	numChunks := int((total + chunkSize - 1) / chunkSize)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	eg.Go(func() error { return writeChunkAt(dst, 0, firstLen, rc) })
	for seq := 1; seq < numChunks; seq++ {
		if egCtx.Err() != nil {
			break
		}
		start := int64(seq) * chunkSize
		end := min(start+chunkSize, total)
		eg.Go(func() error { return fetchChunk(egCtx, c, key, dst, start, end, etag) })
	}
	return errors.Wrap(eg.Wait(), "parallel get")
}

func fullRead(ctx context.Context, c RangeReader, key Key, dst io.WriterAt) error {
	rc, _, err := c.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "parallel get: full read")
	}
	return errors.Wrap(writeChunkAt(dst, 0, -1, rc), "parallel get")
}

func fetchChunk(ctx context.Context, c RangeReader, key Key, dst io.WriterAt, start, end int64, etag string) error {
	rc, headers, err := c.Open(ctx, key, Range(start, end), IfMatch(etag))
	if errors.Is(err, ErrPreconditionFailed) {
		return errors.Errorf("open range %d-%d: object changed during read: %w", start, end, err)
	}
	if err != nil {
		return errors.Errorf("open range %d-%d: %w", start, end, err)
	}
	if got := headers.Get(ETagKey); got != etag {
		return errors.Join(
			errors.Errorf("object changed during read at offset %d: etag %q != %q", start, got, etag),
			rc.Close(),
		)
	}
	return writeChunkAt(dst, start, end-start, rc)
}

func writeChunkAt(dst io.WriterAt, off, want int64, src io.ReadCloser) error {
	n, copyErr := io.Copy(io.NewOffsetWriter(dst, off), src)
	if err := errors.Join(copyErr, src.Close()); err != nil {
		return errors.Errorf("write chunk at offset %d: %w", off, err)
	}
	if want >= 0 && n != want {
		return errors.Errorf("short chunk at offset %d: wrote %d of %d bytes", off, n, want)
	}
	return nil
}

func parseContentRangeTotal(contentRange string) (total int64, ok bool) {
	_, size, found := strings.Cut(contentRange, "/")
	if !found {
		return 0, false
	}
	total, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return 0, false
	}
	return total, true
}
