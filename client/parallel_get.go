package client

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

// chunkRetryAttempts bounds retries when a chunk response ignores the
// requested range, e.g. a load-balanced replica that cannot serve the range;
// discovery already proved range support, so a retry may route to a capable
// replica.
const chunkRetryAttempts = 4

// RangeReader is the subset of cache operations ParallelGet needs: a
// conditional, Range-capable Open. Both *Client and cache.Cache satisfy it.
type RangeReader interface {
	Open(ctx context.Context, key Key, opts ...RequestOption) (io.ReadCloser, http.Header, error)
}

// ParallelGet downloads an object in chunkSize-byte chunks with up to
// concurrency requests in flight, writing each chunk at its offset via
// dst.WriteAt. dst may block writes to apply backpressure (e.g. a bounded
// reordering buffer feeding a stream): chunk dispatch never runs more than
// 2*concurrency chunks past the oldest incomplete chunk, so a dst window of
// 2*concurrency*chunkSize is never exceeded and blocked writes always drain.
//
// Chunks are pinned to the discovery response's ETag so a mid-download rewrite
// is rejected rather than spliced. No ETag, a range-ignoring backend, an
// object that fits in the first chunk, or concurrency 1 all fall back to a
// single full read. On error dst is left partially written and must be
// discarded by the caller.
func ParallelGet(ctx context.Context, c RangeReader, key Key, dst io.WriterAt, chunkSize int64, concurrency int) error {
	if max(concurrency, 1) == 1 {
		return fullRead(ctx, c, key, dst)
	}
	if err := validateParallelParams(chunkSize, concurrency); err != nil {
		return err
	}

	rc, headers, etag, total, hasRange, err := discoverFirstChunk(ctx, c, key, chunkSize)
	if err != nil {
		return err
	}
	if rc == nil {
		notifyDiscovery(c, headers)
		return nil // Empty object: nothing to write.
	}

	// Range ignored or the object fits in the first chunk: this response
	// already carries the whole object (negative length = unknown size).
	if !hasRange {
		notifyDiscovery(c, headers)
		return errors.Wrap(writeChunkAt(dst, 0, -1, rc), "parallel get")
	}
	if total <= chunkSize {
		notifyDiscovery(c, headers)
		return errors.Wrap(writeChunkAt(dst, 0, total, rc), "parallel get")
	}

	// Without an ETag there is nothing to pin chunks to, so a rewrite could be
	// spliced undetected; fall back to a single revision-consistent read.
	// fullRead reports its own response's headers: this response's bytes are
	// discarded, so its metadata must not be recorded either.
	if etag == "" {
		if err := rc.Close(); err != nil {
			return errors.Wrap(err, "parallel get: close discovery reader")
		}
		return fullRead(ctx, c, key, dst)
	}
	notifyDiscovery(c, headers)

	numChunks := 1 + (total-1)/chunkSize // overflow-safe ceil; total > chunkSize here
	eg, egCtx := errgroup.WithContext(ctx)

	// Bound dispatch run-ahead: a chunk may start only while it is within
	// 2*concurrency of the oldest incomplete chunk. A dst whose WriteAt blocks
	// (bounded window, no ctx) therefore cannot deadlock when a sibling chunk
	// fails: every dispatched write fits its window once the stream drains to
	// the gap, so blocked writes complete and eg.Wait returns.
	gate := newDispatchGate(int64(2 * concurrency))

	var nextSeq atomic.Int64
	nextSeq.Store(1)
	worker := func() error {
		for {
			seq := nextSeq.Add(1) - 1
			if seq >= numChunks {
				return nil
			}
			if err := gate.wait(egCtx, seq); err != nil {
				return err
			}
			start := seq * chunkSize
			end := min(start+chunkSize, total)
			body, err := openChunk(egCtx, c, key, etag, start, end)
			if err != nil {
				return err
			}
			if err := writeChunkAt(dst, start, end-start, body); err != nil {
				return errors.WithStack(err)
			}
			gate.complete(seq)
		}
	}

	// The first worker writes chunk zero from the already-open discovery body,
	// keeping in-flight requests at concurrency rather than concurrency+1. rc
	// was opened with the parent ctx, so close it on egCtx cancellation or a
	// failed sibling would leave the copy blocked; double Close is harmless.
	eg.Go(func() error {
		stop := context.AfterFunc(egCtx, func() { _ = rc.Close() }) //nolint:errcheck
		err := writeChunkAt(dst, 0, min(chunkSize, total), rc)
		stop()
		if err != nil {
			return errors.WithStack(err)
		}
		gate.complete(0)
		return worker()
	})
	for range min(int64(concurrency), numChunks) - 1 {
		eg.Go(worker)
	}
	return errors.Wrap(eg.Wait(), "parallel get")
}

// ParallelGetReader returns a reader over an object downloaded by ParallelGet,
// reassembling concurrent chunk writes into a sequential stream with bounded
// buffering (2*concurrency chunk-sized pages). Download errors surface on
// Read; Close cancels any in-flight requests.
func ParallelGetReader(ctx context.Context, c RangeReader, key Key, chunkSize int64, concurrency int) (io.ReadCloser, error) {
	concurrency = max(concurrency, 1) // ParallelGet treats <= 1 as a single-stream read.
	if err := validateParallelParams(chunkSize, concurrency); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	buf := newStreamBuffer(chunkSize, concurrency)
	go func() {
		err := ParallelGet(ctx, c, key, buf, chunkSize, concurrency)
		cancel()
		buf.closeWrite(err)
	}()
	return &cancelReadCloser{ReadCloser: buf, cancel: cancel}, nil
}

// discoverFirstChunk opens chunk zero with a ranged request, revealing the
// total size and the ETag that pin the remaining chunks. A response that
// ignores the range is accepted as the single-stream fallback rather than
// retried: on a cold miss the server streams a freshly generated object, and
// closing that body would abort and repeat the expensive generation. A nil rc
// with nil err means the object is empty.
func discoverFirstChunk(ctx context.Context, c RangeReader, key Key, chunkSize int64) (rc io.ReadCloser, headers http.Header, etag string, total int64, hasRange bool, err error) {
	rc, headers, err = c.Open(ctx, key, Range(0, chunkSize))
	if errors.Is(err, ErrRangeNotSatisfiable) {
		return nil, headers, "", 0, false, nil
	}
	if err != nil {
		return nil, nil, "", 0, false, errors.Wrap(err, "parallel get: open first chunk")
	}
	etag = headers.Get(ETagKey)
	total, hasRange = parseContentRangeTotal(headers.Get("Content-Range"))
	return rc, headers, etag, total, hasRange, nil
}

// discoveryObserver is implemented by RangeReaders that need the headers of
// the response ParallelGet accepts as the object's source of truth, e.g. to
// surface application metadata carried on the response. Discarded
// range-ignoring attempts are never observed.
type discoveryObserver interface {
	observeDiscovery(http.Header)
}

func notifyDiscovery(c RangeReader, headers http.Header) {
	if o, ok := c.(discoveryObserver); ok && headers != nil {
		o.observeDiscovery(headers)
	}
}

// maxParallelGetConcurrency bounds the buffering window (2*concurrency
// chunk-sized pages) so a misconfigured concurrency cannot exhaust memory
// before the download starts.
const maxParallelGetConcurrency = 1024

// validateParallelParams rejects parameters whose buffering-window arithmetic
// (2*concurrency pages of chunkSize bytes) would overflow or allocate
// unboundedly.
func validateParallelParams(chunkSize int64, concurrency int) error {
	if chunkSize <= 0 {
		return errors.Errorf("parallel get: chunk size must be positive, got %d", chunkSize)
	}
	if concurrency > maxParallelGetConcurrency {
		return errors.Errorf("parallel get: concurrency %d exceeds maximum %d", concurrency, maxParallelGetConcurrency)
	}
	if window := 2 * int64(concurrency); chunkSize > math.MaxInt64/window {
		return errors.Errorf("parallel get: chunk size %d with concurrency %d overflows the buffer window", chunkSize, concurrency)
	}
	return nil
}

// dispatchGate bounds chunk dispatch to a window past the oldest incomplete
// chunk, so every in-flight chunk's write offset stays within window chunks of
// the stream's contiguous frontier. done is a ring: the gate only ever tracks
// chunks in [low, low+window), so cells cannot alias.
type dispatchGate struct {
	window int64

	mu   sync.Mutex
	wake chan struct{} // closed and replaced when low advances
	done []bool        // completion of chunk seq at done[seq%window]
	low  int64         // oldest incomplete chunk
}

func newDispatchGate(window int64) *dispatchGate {
	return &dispatchGate{window: window, wake: make(chan struct{}), done: make([]bool, window)}
}

// wait blocks until seq is within the dispatch window or ctx is cancelled.
func (g *dispatchGate) wait(ctx context.Context, seq int64) error {
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	g.mu.Lock()
	for seq-g.low >= g.window {
		ch := g.wake
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		}
		g.mu.Lock()
	}
	g.mu.Unlock()
	return nil
}

// complete marks seq fully written, advancing the window over any
// contiguously completed chunks.
func (g *dispatchGate) complete(seq int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.done[seq%g.window] = true
	if seq != g.low {
		return
	}
	for g.done[g.low%g.window] {
		g.done[g.low%g.window] = false
		g.low++
	}
	close(g.wake)
	g.wake = make(chan struct{})
}

// openChunk fetches one chunk's range pinned to the discovery ETag.
// Range-ignoring responses are retried, as each attempt may route to a
// different replica; an ETag mismatch or precondition failure is fatal since
// the object changed mid-download.
func openChunk(ctx context.Context, c RangeReader, key Key, etag string, start, end int64) (io.ReadCloser, error) {
	for attempt := 0; ; attempt++ {
		body, h, err := c.Open(ctx, key, Range(start, end), IfMatch(etag))
		if errors.Is(err, ErrPreconditionFailed) {
			return nil, errors.Errorf("parallel get: open range %d-%d: object changed during read: %w", start, end, err)
		}
		if err != nil {
			return nil, errors.Errorf("parallel get: open range %d-%d: %w", start, end, err)
		}
		// Check Content-Range before trusting the ETag: a range-ignoring
		// response often carries no ETag.
		if cr := h.Get("Content-Range"); !strings.HasPrefix(cr, "bytes "+strconv.FormatInt(start, 10)+"-") {
			if attempt < chunkRetryAttempts-1 {
				if err := body.Close(); err != nil {
					return nil, errors.Wrap(err, "parallel get: close range-ignoring chunk reader")
				}
				continue
			}
			return nil, errors.Join(
				errors.Errorf("parallel get: server ignored range %d-%d (Content-Range %q)", start, end, cr),
				body.Close(),
			)
		}
		if got := h.Get(ETagKey); got != etag {
			return nil, errors.Join(
				errors.Errorf("parallel get: object changed during read at offset %d: etag %q != %q", start, got, etag),
				body.Close(),
			)
		}
		return body, nil
	}
}

// fullRead downloads the object in a single request, writing sequentially from
// offset zero with an unknown length.
func fullRead(ctx context.Context, c RangeReader, key Key, dst io.WriterAt) error {
	rc, headers, err := c.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "parallel get: full read")
	}
	notifyDiscovery(c, headers)
	return errors.Wrap(writeChunkAt(dst, 0, -1, rc), "parallel get")
}

// writeChunkAt copies src to its offset in dst, closing src. length < 0 means
// "write the whole body" (unknown size); otherwise the copy is capped at
// length so an overlong body can never write into another chunk's range, and
// short or overlong bodies are reported as errors.
func writeChunkAt(dst io.WriterAt, off, length int64, src io.ReadCloser) error {
	w := io.NewOffsetWriter(dst, off)
	if length < 0 {
		_, err := io.Copy(w, src)
		return errors.Join(errors.Wrap(err, "write chunk"), src.Close())
	}
	n, err := io.Copy(w, io.LimitReader(src, length))
	if err != nil {
		return errors.Join(errors.Errorf("write chunk at offset %d: %w", off, err), src.Close())
	}
	if n != length {
		return errors.Join(errors.Errorf("chunk at offset %d: wrote %d of %d bytes", off, n, length), src.Close())
	}
	if overlong(src) {
		return errors.Join(errors.Errorf("chunk at offset %d: read more than the expected %d bytes", off, length), src.Close())
	}
	return errors.WithStack(src.Close())
}

// overlong reports whether r has bytes left, detecting a body longer than the
// requested chunk without buffering the excess.
func overlong(r io.Reader) bool {
	var probe [1]byte
	n, _ := io.ReadFull(r, probe[:]) //nolint:errcheck // any byte past the chunk is overlong, regardless of the error
	return n > 0
}

// parseContentRangeTotal extracts the total from "bytes start-end/total",
// returning ok=false when the header is absent or unparseable.
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
