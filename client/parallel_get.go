package client

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"github.com/jpillora/backoff"
	"golang.org/x/sync/errgroup"
)

// chunkRetryAttempts bounds retries when a chunk response ignores the
// requested range, e.g. a load-balanced replica that cannot serve the range;
// discovery already proved range support, so a retry may route to a capable
// replica.
const chunkRetryAttempts = 4

// chunkResumeAttempts bounds how often one chunk is reopened after its
// response breaks mid-body or fails transiently. Proxies cut individual
// responses when they drain or restart; resuming the chunk from the failed
// offset, pinned to the discovery ETag, keeps the rest of the download.
const chunkResumeAttempts = 3

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
// is rejected rather than spliced, which also makes it safe to resume a chunk
// whose response breaks mid-body or fails transiently from the offset it
// reached. No ETag, a range-ignoring backend, an
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
		_, err := writeChunkAt(dst, 0, -1, rc)
		return errors.Wrap(err, "parallel get")
	}
	if total <= chunkSize {
		notifyDiscovery(c, headers)
		if etag != "" {
			return errors.Wrap(fetchChunk(ctx, c, key, etag, dst, 0, total, rc), "parallel get")
		}
		_, err := writeChunkAt(dst, 0, total, rc)
		return errors.Wrap(err, "parallel get")
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
			if err := fetchChunk(egCtx, c, key, etag, dst, start, end, nil); err != nil {
				return err
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
		err := fetchChunk(egCtx, c, key, etag, dst, 0, min(chunkSize, total), rc)
		stop()
		if err != nil {
			return err
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
	for attempt := 0; ; attempt++ {
		rc, headers, err = c.Open(ctx, key, Range(0, chunkSize))
		if err == nil || attempt == chunkResumeAttempts || !resumable(err) || !waitToResume(ctx, attempt) {
			break
		}
	}
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

// fetchChunk writes [start, end) to dst from body, or from a fresh pinned
// request when body is nil. A broken response or transient reopen failure
// resumes from the last written offset, so a proxy cutting one response costs
// the rest of that chunk rather than the whole download.
func fetchChunk(ctx context.Context, c RangeReader, key Key, etag string, dst io.WriterAt, start, end int64, body io.ReadCloser) error {
	for attempt := 0; ; attempt++ {
		var err error
		if body == nil {
			body, err = openChunk(ctx, c, key, etag, start, end)
		}
		if err == nil {
			var n int64
			n, err = writeChunkAt(dst, start, end-start, body)
			start += n
			body = nil
			if err == nil {
				return nil
			}
		}
		if attempt == chunkResumeAttempts || !resumable(err) || !waitToResume(ctx, attempt) {
			return errors.WithStack(err)
		}
	}
}

// waitToResume sleeps before resume attempt+1, reporting false if ctx ends
// first. The backoff keeps a reconnect from racing the endpoint removal of a
// draining proxy.
func waitToResume(ctx context.Context, attempt int) bool {
	if ctx.Err() != nil {
		return false
	}
	delay := (&backoff.Backoff{Min: 100 * time.Millisecond, Max: time.Second, Factor: 2, Jitter: true}).ForAttempt(float64(attempt))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// bodyError marks a chunk response that failed or ended early mid-body, as
// opposed to a failed write to dst, which a new request cannot fix.
type bodyError struct{ error }

func (e bodyError) Unwrap() error { return e.error }

// resumable reports whether a fresh request may complete a failed chunk: the
// response broke mid-body, or the request failed in transport or with a server
// error. A changed object, a missing object, or a client error is final.
func resumable(err error) bool {
	var broken bodyError
	var status *HTTPStatusError
	var transport *url.Error
	switch {
	case errors.As(err, &broken):
		return true
	case errors.Is(err, ErrPreconditionFailed):
		return false
	case errors.As(err, &status):
		return status.StatusCode >= http.StatusInternalServerError
	default:
		return errors.As(err, &transport)
	}
}

// readErrRecorder remembers a read failure so a copy error can be attributed
// to the response rather than to the destination.
type readErrRecorder struct {
	r   io.Reader
	err error
}

func (r *readErrRecorder) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}
	return n, err //nolint:wrapcheck // io.Reader contract: io.EOF must not be wrapped
}

// fullRead downloads the object in a single request, writing sequentially from
// offset zero with an unknown length.
func fullRead(ctx context.Context, c RangeReader, key Key, dst io.WriterAt) error {
	rc, headers, err := c.Open(ctx, key)
	if err != nil {
		return errors.Wrap(err, "parallel get: full read")
	}
	notifyDiscovery(c, headers)
	_, err = writeChunkAt(dst, 0, -1, rc)
	return errors.Wrap(err, "parallel get")
}

// writeChunkAt copies src to its offset in dst, closing src, and returns the
// bytes written. length < 0 means "write the whole body" (unknown size);
// otherwise the copy is capped at length so an overlong body can never write
// into another chunk's range, and short or overlong bodies are reported as
// errors. A response that fails or ends short is reported as a bodyError.
func writeChunkAt(dst io.WriterAt, off, length int64, src io.ReadCloser) (int64, error) {
	w := io.NewOffsetWriter(dst, off)
	body := &readErrRecorder{r: src}
	if length < 0 {
		n, err := io.Copy(w, body)
		return n, errors.Join(errors.Wrap(err, "write chunk"), src.Close())
	}
	n, err := io.Copy(w, io.LimitReader(body, length))
	if err != nil {
		err = errors.Errorf("write chunk at offset %d: %w", off, err)
		if body.err != nil {
			err = bodyError{err}
		}
		return n, errors.Join(err, src.Close())
	}
	if n != length {
		return n, errors.Join(bodyError{errors.Errorf("chunk at offset %d: wrote %d of %d bytes", off, n, length)}, src.Close())
	}
	if overlong(src) {
		return n, errors.Join(errors.Errorf("chunk at offset %d: read more than the expected %d bytes", off, length), src.Close())
	}
	return n, errors.WithStack(src.Close())
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
