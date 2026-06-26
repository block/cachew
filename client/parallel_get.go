package client

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

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

// ParallelGet downloads an object from a Range-capable RangeReader, fetching it
// in chunkSize-byte chunks concurrently (up to concurrency requests in flight)
// and handing each chunk to sink, which decides where the bytes land. Latency-
// bound backends such as a remote cache can saturate bandwidth with overlapping
// reads.
//
// Pass a StreamSink to reassemble the in-order byte stream for a streaming
// consumer (run ParallelGet in a goroutine and read the sink concurrently), or a
// DiskSink to scatter chunks to their offsets in a file (ParallelGet may then run
// synchronously). The engine itself is agnostic to the destination.
//
// The first chunk is fetched with a ranged Open, whose response yields the total
// size (from Content-Range) and the object's ETag; every later chunk is then
// requested with IfRange pinned to that ETag. If the object changes mid-download
// a chunk's ETag differs and ParallelGet returns an error rather than splicing
// bytes from two revisions. An object with no ETag to pin to, a backend that
// ignores ranges, an object that fits within the first chunk, or a concurrency
// of 1 all fall back to a single full read handed to the sink as one stream
// (offset 0, length < 0).
func ParallelGet(ctx context.Context, c RangeReader, key Key, sink ChunkSink, chunkSize int64, concurrency int) error {
	// A single worker gains nothing from chunking — it would only serialise
	// ranged GETs — so skip discovery entirely and read the object in one
	// revision-consistent request. chunkSize is unused on this path.
	if max(concurrency, 1) == 1 {
		return fullRead(ctx, c, key, sink)
	}
	if chunkSize <= 0 {
		return errors.Errorf("parallel get: chunk size must be positive, got %d", chunkSize)
	}

	// Discovery: the first ranged Open delivers chunk zero and reveals the total
	// size and ETag used to pin the rest.
	rc, headers, err := c.Open(ctx, key, Range(0, chunkSize))
	if errors.Is(err, ErrRangeNotSatisfiable) {
		return nil // Empty object: nothing to place.
	}
	if err != nil {
		return errors.Wrap(err, "parallel get: open first chunk")
	}

	etag := headers.Get(ETagKey)
	total, hasRange := parseContentRangeTotal(headers.Get("Content-Range"))

	// A backend that ignored the range, or an object that fits in the first
	// chunk, is delivered whole by this response: hand it to the sink as a single
	// stream. A negative length tells the sink the size is unknown and the body
	// must be streamed, not buffered.
	if !hasRange {
		return errors.Wrap(sink.Place(ctx, 0, -1, rc), "parallel get")
	}
	if total <= chunkSize {
		return errors.Wrap(sink.Place(ctx, 0, total, rc), "parallel get")
	}

	// Subsequent chunks pin the discovery ETag via IfRange. Without a validator
	// there is nothing to pin to (IfRange("") is a no-op), so chunks could be
	// spliced across a rewrite undetected. Objects stored before ETags were
	// recorded fall here, so fall back to a single, revision-consistent read.
	if etag == "" {
		if err := rc.Close(); err != nil {
			return errors.Wrap(err, "parallel get: close discovery reader")
		}
		return fullRead(ctx, c, key, sink)
	}

	numChunks := (total + chunkSize - 1) / chunkSize
	eg, egCtx := errgroup.WithContext(ctx)

	// Workers pull sequence numbers and fetch the remaining chunks. The sink
	// bounds how far ahead they run — a StreamSink blocks Place once it is a
	// window ahead of the reader — so peak memory stays bounded regardless of
	// object size.
	var nextSeq atomic.Int64
	nextSeq.Store(1)
	worker := func() error {
		for {
			if egCtx.Err() != nil {
				return errors.WithStack(egCtx.Err())
			}
			seq := nextSeq.Add(1) - 1
			if seq >= numChunks {
				return nil
			}
			start := seq * chunkSize
			end := min(start+chunkSize, total)
			body, h, err := c.Open(egCtx, key, Range(start, end), IfRange(etag))
			if err != nil {
				return errors.Errorf("parallel get: open range %d-%d: %w", start, end, err)
			}
			if got := h.Get(ETagKey); got != etag {
				return errors.Join(
					errors.Errorf("parallel get: object changed during read at offset %d: etag %q != %q", start, got, etag),
					body.Close(),
				)
			}
			if err := sink.Place(egCtx, start, end-start, body); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	// One worker places chunk zero from the already-open discovery body, then
	// joins the pool, so the number of concurrent range requests stays at
	// concurrency rather than concurrency+1. The discovery body was opened with
	// the parent ctx and so would not unblock if a sibling chunk fails and
	// cancels egCtx, so close it on egCtx cancellation; the Place path closes rc
	// on success, and a double Close is harmless.
	eg.Go(func() error {
		stop := context.AfterFunc(egCtx, func() { _ = rc.Close() }) //nolint:errcheck
		err := sink.Place(egCtx, 0, min(chunkSize, total), rc)
		stop()
		if err != nil {
			return errors.WithStack(err)
		}
		return worker()
	})
	for range concurrency - 1 {
		eg.Go(worker)
	}
	return errors.Wrap(eg.Wait(), "parallel get")
}

// fullRead downloads the entire object in a single request and hands it to the
// sink as one stream. It is used when chunking adds no value (a single worker) or
// cannot be made revision-safe (no ETag, or a backend that ignores ranges). The
// body is a single consistent revision whose length is unknown up front, so it is
// placed with a negative length.
func fullRead(ctx context.Context, c RangeReader, key Key, sink ChunkSink) error {
	rc, _, err := c.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "parallel get: full read")
	}
	return errors.Wrap(sink.Place(ctx, 0, -1, rc), "parallel get")
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
