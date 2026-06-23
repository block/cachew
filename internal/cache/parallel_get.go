package cache

import (
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

// ParallelGet downloads an object from any Range-capable Cache into dst, fetching
// it in chunkSize-byte chunks concurrently (up to concurrency requests in
// flight) and writing each chunk at its offset via dst.WriteAt. Latency-bound
// backends such as a remote cache can saturate bandwidth with overlapping reads.
//
// The first chunk is fetched with a ranged Open, whose response yields both the
// total size (from Content-Range) and the object's ETag; every remaining chunk
// is then requested with IfRange pinned to that ETag. If the object changes
// mid-download, a chunk's ETag will differ and ParallelGet returns an error
// rather than splicing bytes from two revisions. A missing or truncated chunk
// is likewise reported as an error, so a partially written dst must be discarded
// by the caller on failure.
//
// dst is written via concurrent WriteAt calls at non-overlapping offsets; the
// caller owns dst's lifecycle (open, close, cleanup) and need not pre-size it,
// as WriteAt extends the destination.
func ParallelGet(ctx context.Context, c Cache, key Key, dst io.WriterAt, chunkSize int64, concurrency int) error {
	if chunkSize <= 0 {
		return errors.Errorf("parallel get: chunk size must be positive, got %d", chunkSize)
	}
	concurrency = max(concurrency, 1)

	// Discovery: the first ranged Open delivers chunk zero and reveals the total
	// size and ETag used to pin the rest.
	rc, headers, err := c.Open(ctx, key, Range(0, chunkSize))
	if errors.Is(err, ErrRangeNotSatisfiable) {
		return nil // Empty object: nothing to write.
	}
	if err != nil {
		return errors.Wrap(err, "parallel get: open first chunk")
	}

	etag := headers.Get(ETagKey)
	total, hasRange := parseContentRangeTotal(headers.Get("Content-Range"))

	n, copyErr := io.Copy(io.NewOffsetWriter(dst, 0), rc)
	if err := errors.Join(copyErr, rc.Close()); err != nil {
		return errors.Wrap(err, "parallel get: write first chunk")
	}

	// Without a Content-Range the backend ignored the range and returned the
	// whole object in one response, so there is nothing left to fetch.
	if !hasRange {
		return nil
	}
	if firstLen := min(chunkSize, total); n != firstLen {
		return errors.Errorf("parallel get: short first chunk: wrote %d of %d bytes", n, firstLen)
	}
	if total <= chunkSize {
		return nil
	}

	numChunks := int((total + chunkSize - 1) / chunkSize)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	for seq := 1; seq < numChunks; seq++ {
		// Stop scheduling once a chunk has failed and cancelled the group.
		if egCtx.Err() != nil {
			break
		}
		start := int64(seq) * chunkSize
		end := min(start+chunkSize, total)
		eg.Go(func() error { return fetchChunk(egCtx, c, key, dst, start, end, etag) })
	}
	return errors.Wrap(eg.Wait(), "parallel get")
}

// fetchChunk opens the [start, end) range pinned to etag and writes it at start.
// An ETag change (the object was rewritten mid-download) or a short read is
// reported as an error.
func fetchChunk(ctx context.Context, c Cache, key Key, dst io.WriterAt, start, end int64, etag string) error {
	rc, headers, err := c.Open(ctx, key, Range(start, end), IfRange(etag))
	if err != nil {
		return errors.Errorf("open range %d-%d: %w", start, end, err)
	}
	defer rc.Close() //nolint:errcheck // Read-only body; write/copy errors below are authoritative.

	if got := headers.Get(ETagKey); got != etag {
		return errors.Errorf("object changed during read at offset %d: etag %q != %q", start, got, etag)
	}

	want := end - start
	n, err := io.Copy(io.NewOffsetWriter(dst, start), rc)
	if err != nil {
		return errors.Errorf("write range %d-%d: %w", start, end, err)
	}
	if n != want {
		return errors.Errorf("short chunk at offset %d: wrote %d of %d bytes", start, n, want)
	}
	return nil
}

// parseContentRangeTotal extracts the total size from a Content-Range value of
// the form "bytes start-end/total". It returns ok=false when the header is
// absent or unparseable.
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
