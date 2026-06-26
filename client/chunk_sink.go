package client

import (
	"context"
	"io"
	"sync"

	"github.com/alecthomas/errors"
)

// ChunkSink is the destination ParallelGet places fetched chunks into. The
// engine calls Place once per chunk, concurrently from up to `concurrency`
// goroutines, with the chunk's absolute byte offset and an open body holding
// exactly length bytes (length < 0 means "read the whole body", used for the
// single-stream fallback when the object cannot be chunked). Place must read the
// chunk from body and close body. Implementations own where the bytes land and
// may block in Place to bound memory; a blocked Place must abort when ctx is
// cancelled.
//
// Two implementations cover the cases in this package: StreamSink reassembles
// the in-order byte stream for a streaming consumer, and DiskSink scatters
// chunks to their offsets in a file.
type ChunkSink interface {
	Place(ctx context.Context, off, length int64, body io.ReadCloser) error
}

// StreamSink is a ChunkSink that reorders concurrently-fetched chunks back into
// the original byte stream, exposed via Read. Chunks land in a fixed arena of
// 2*concurrency reusable slots indexed by chunk number, so a slow consumer
// applies backpressure to the fetchers (capping memory) instead of letting
// fetched-but-unread chunks pile up. The doubled slot count lets the fetchers
// run a full window ahead of the consumer rather than stalling on it.
//
// A StreamSink must be read concurrently while ParallelGet runs — the fetchers
// block once they get a window ahead of the reader, so a caller that does not
// read will deadlock. After the download finishes the caller signals completion
// with Done; Read then drains the remaining buffered chunks and returns io.EOF,
// or the download error.
type StreamSink struct {
	chunkSize int64
	n         int // slot count = 2*concurrency

	mu      sync.Mutex
	cond    *sync.Cond    // signals Read that a chunk was deposited (or Done)
	advance chan struct{} // closed and replaced when readSeq advances, waking blocked Place
	bufs    [][]byte      // n reusable backing buffers, indexed by seq%n (nil until first use)
	ready   []bool        // ready[slot] => bufs[slot] holds the chunk for its current seq
	readSeq int64         // sequence number of the chunk Read is emitting next
	cur     []byte        // chunk currently being emitted (aliases bufs[readSeq%n])
	curPos  int

	passthru io.ReadCloser // set in single-stream fallback mode (length < 0)
	done     bool
	err      error
	closed   bool
}

// NewStreamSink returns a StreamSink sized for the given chunk size and download
// concurrency. It holds up to 2*concurrency chunk buffers, giving the fetchers a
// full window of run-ahead over the consumer while capping peak memory at
// 2*concurrency*chunkSize. Buffers are allocated lazily, so a small object never
// reserves the full window.
func NewStreamSink(chunkSize int64, concurrency int) *StreamSink {
	n := 2 * max(concurrency, 1)
	s := &StreamSink{
		chunkSize: chunkSize,
		n:         n,
		bufs:      make([][]byte, n),
		ready:     make([]bool, n),
		advance:   make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Place reads the chunk into its slot and queues it for in-order delivery to
// Read. It blocks until the chunk is within one window of the read cursor
// (backpressure from a slow consumer) and aborts if ctx is cancelled. A negative
// length switches to pass-through mode: the whole body is handed to Read
// directly, since a single-stream fallback has unknown size and must not be
// buffered.
func (s *StreamSink) Place(ctx context.Context, off, length int64, body io.ReadCloser) error {
	if length < 0 {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return errors.Join(errors.New("stream sink closed"), body.Close())
		}
		s.passthru = body
		s.cond.Broadcast()
		s.mu.Unlock()
		return nil
	}

	seq := off / s.chunkSize
	slot := int(seq % int64(s.n))

	// Admission: a chunk may only occupy its slot once the previous occupant
	// (seq-n) has been read, i.e. once seq is within n of the read cursor. This
	// bounds run-ahead and guarantees no other in-flight chunk maps to this slot,
	// so the in-order chunk's slot is always reserved for it.
	s.mu.Lock()
	for seq >= s.readSeq+int64(s.n) {
		if s.closed {
			s.mu.Unlock()
			return errors.Join(errors.New("stream sink closed"), body.Close())
		}
		ch := s.advance
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return errors.Join(errors.WithStack(ctx.Err()), body.Close())
		}
		s.mu.Lock()
	}
	buf := s.bufs[slot]
	s.mu.Unlock()

	if int64(cap(buf)) < length {
		buf = make([]byte, s.chunkSize)
	}
	buf = buf[:length]
	if err := readChunk(off, buf, body); err != nil {
		return err
	}

	s.mu.Lock()
	// A Close racing the body read above leaves no reader to drain this slot;
	// drop the chunk rather than mark it ready. readChunk already closed body.
	if s.closed {
		s.mu.Unlock()
		return errors.New("stream sink closed")
	}
	s.bufs[slot] = buf
	s.ready[slot] = true
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

// Read emits the reassembled object in order. It blocks until the next chunk is
// available, returning io.EOF once every chunk has been read and Done has been
// called, or the download error reported to Done.
func (s *StreamSink) Read(p []byte) (int, error) {
	s.mu.Lock()
	for {
		if s.passthru != nil {
			body := s.passthru
			s.mu.Unlock()
			return body.Read(p) //nolint:wrapcheck // must return io.EOF verbatim for io.ReadAll
		}
		if s.cur != nil {
			n := copy(p, s.cur[s.curPos:])
			s.curPos += n
			if s.curPos >= len(s.cur) {
				// Chunk fully emitted: free its slot and advance, waking any Place
				// blocked waiting for this slot's window to open.
				slot := int(s.readSeq % int64(s.n))
				s.ready[slot] = false
				s.readSeq++
				s.cur = nil
				s.curPos = 0
				close(s.advance)
				s.advance = make(chan struct{})
			}
			s.mu.Unlock()
			return n, nil
		}
		slot := int(s.readSeq % int64(s.n))
		if s.ready[slot] {
			s.cur = s.bufs[slot]
			s.curPos = 0
			continue
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return 0, err
		}
		if s.done {
			s.mu.Unlock()
			return 0, io.EOF
		}
		// Closed mid-download with no terminal status: stop rather than block
		// forever on cond, since the fetchers are being torn down.
		if s.closed {
			s.mu.Unlock()
			return 0, errors.WithStack(io.ErrClosedPipe)
		}
		s.cond.Wait()
	}
}

// Done signals that no further chunks will be placed. err is the download
// outcome (nil on success); it is surfaced to Read after the buffered chunks
// drain.
func (s *StreamSink) Done(err error) {
	s.mu.Lock()
	s.done = true
	if err != nil && s.err == nil {
		s.err = err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// Close releases the sink, unblocking any in-flight Place and closing the
// pass-through body if one is set. The arena buffers are released to the garbage
// collector. Cancelling the download itself is the caller's responsibility (see
// OpenGitSnapshotParallel).
func (s *StreamSink) Close() error {
	s.mu.Lock()
	s.closed = true
	body := s.passthru
	s.passthru = nil
	close(s.advance)
	s.advance = make(chan struct{})
	s.cond.Broadcast()
	s.mu.Unlock()
	if body != nil {
		return errors.WithStack(body.Close())
	}
	return nil
}

// DiskSink is a ChunkSink that writes each chunk straight to its offset in an
// io.WriterAt such as an *os.File. io.WriterAt permits concurrent
// non-overlapping writes, so chunks are scattered to disk as they arrive with no
// reordering and negligible memory — the right sink for seekable destinations
// such as cache-to-cache backfill. Unlike StreamSink it needs no concurrent
// reader, so ParallelGet may run to completion synchronously. On error the
// destination is left partially written and must be discarded by the caller.
type DiskSink struct{ W io.WriterAt }

// Place streams the chunk straight to its offset in the underlying WriterAt.
func (d DiskSink) Place(_ context.Context, off, length int64, body io.ReadCloser) error {
	dst := io.NewOffsetWriter(d.W, off)
	if length < 0 {
		_, err := io.Copy(dst, body)
		return errors.Join(errors.Wrap(err, "write chunk"), body.Close())
	}
	n, err := io.Copy(dst, io.LimitReader(body, length))
	if err != nil {
		return errors.Join(errors.Errorf("write chunk at offset %d: %w", off, err), body.Close())
	}
	if n != length {
		return errors.Join(errors.Errorf("chunk at offset %d: wrote %d of %d bytes", off, n, length), body.Close())
	}
	if overlong(body) {
		return errors.Join(errors.Errorf("chunk at offset %d: read more than the expected %d bytes", off, length), body.Close())
	}
	return errors.WithStack(body.Close())
}

// readChunk fills buf from body (reading exactly len(buf) bytes) and closes
// body. A body shorter than buf (a truncated chunk) or longer than buf (a
// backend that ignored the range) is reported as an error.
func readChunk(off int64, buf []byte, body io.ReadCloser) error {
	if _, err := io.ReadFull(body, buf); err != nil {
		return errors.Join(errors.Errorf("read chunk at offset %d: %w", off, err), body.Close())
	}
	if overlong(body) {
		return errors.Join(errors.Errorf("chunk at offset %d: read more than the expected %d bytes", off, len(buf)), body.Close())
	}
	return errors.WithStack(body.Close())
}

// overlong reports whether r has any bytes left, used to detect a body longer
// than the requested chunk without buffering the excess.
func overlong(r io.Reader) bool {
	var probe [1]byte
	n, _ := io.ReadFull(r, probe[:]) //nolint:errcheck // any byte past the chunk is overlong, regardless of the error
	return n > 0
}
