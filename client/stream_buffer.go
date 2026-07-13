package client

import (
	"cmp"
	"io"
	"slices"
	"sync"

	"github.com/alecthomas/errors"
)

// streamBuffer reorders concurrent WriteAt calls back into a sequential byte
// stream readable via Read, buffering in memory. A fixed ring of 2*concurrency
// chunk-sized pages bounds memory: WriteAt blocks while its target page is
// more than 2*concurrency pages past the page being read, applying
// backpressure to fetchers, so peak buffering is capped at
// 2*concurrency*chunkSize regardless of object size.
//
// WriteAt carries no context, so a blocked write is only released by reader
// progress, Close, or closeWrite. ParallelGet guarantees release by never
// dispatching a chunk more than 2*concurrency past the oldest incomplete one:
// every in-flight write fits the window once the reader drains to the frontier.
type streamBuffer struct {
	chunkSize int64
	n         int64 // page count = 2*concurrency

	mu       sync.Mutex
	cond     *sync.Cond
	pages    [][]byte        // n lazily allocated chunk-sized pages, indexed by (off/chunkSize)%n
	pending  []writeInterval // written ranges not yet contiguous with the frontier
	frontier int64           // contiguous bytes written from offset 0
	readOff  int64           // next byte Read will emit
	closed   bool            // write side finished
	err      error           // surfaced to Read once buffered bytes drain
	rclosed  bool            // read side closed
}

type writeInterval struct{ start, end int64 }

// newStreamBuffer returns a streamBuffer with a ring of 2*concurrency pages
// of chunkSize bytes each, allocated lazily.
func newStreamBuffer(chunkSize int64, concurrency int) *streamBuffer {
	n := int64(2 * max(concurrency, 1))
	s := &streamBuffer{
		chunkSize: chunkSize,
		n:         n,
		pages:     make([][]byte, n),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// WriteAt implements io.WriterAt. Writes at non-overlapping offsets may run
// concurrently; a write beyond the window blocks until the reader catches up.
func (s *streamBuffer) WriteAt(p []byte, off int64) (int, error) {
	written := 0
	for len(p) > 0 {
		n, err := s.writeSome(p, off)
		written += n
		off += int64(n)
		p = p[n:]
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// writeSome copies as much of p as fits the current page, blocking until the
// page's ring slot is free. The window is page-granular: a slot is reusable
// only once the reader has moved wholly past its previous page, otherwise a
// write one ring ahead would clobber the unread tail of a partially read page.
func (s *streamBuffer) writeSome(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for off/s.chunkSize >= s.readOff/s.chunkSize+s.n {
		if s.rclosed || s.closed {
			return 0, errors.WithStack(io.ErrClosedPipe)
		}
		s.cond.Wait()
	}
	if s.rclosed || s.closed {
		return 0, errors.WithStack(io.ErrClosedPipe)
	}
	n := min(int64(len(p)), s.chunkSize-off%s.chunkSize)
	slot := (off / s.chunkSize) % s.n
	if s.pages[slot] == nil {
		s.pages[slot] = make([]byte, s.chunkSize)
	}
	copy(s.pages[slot][off%s.chunkSize:], p[:n])
	s.extendLocked(off, off+n)
	s.cond.Broadcast()
	return int(n), nil
}

// Read emits bytes in offset order as the contiguous write frontier advances,
// blocking until data arrives or the write side finishes. After closeWrite it
// drains the remaining buffered bytes, then returns the recorded error or
// io.EOF.
func (s *streamBuffer) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.rclosed {
			return 0, errors.WithStack(io.ErrClosedPipe)
		}
		if s.readOff < s.frontier {
			break
		}
		if s.closed {
			if s.err != nil {
				return 0, s.err
			}
			return 0, io.EOF
		}
		s.cond.Wait()
	}
	n := min(int64(len(p)), s.frontier-s.readOff, s.chunkSize-s.readOff%s.chunkSize)
	slot := (s.readOff / s.chunkSize) % s.n
	copy(p[:n], s.pages[slot][s.readOff%s.chunkSize:][:n])
	s.readOff += n
	s.cond.Broadcast()
	return int(n), nil
}

// closeWrite marks the write side finished; err (nil on success) is surfaced
// to Read after the buffered bytes drain. A nil err with incomplete coverage
// from offset 0 is reported as an error so a gap can never silently truncate
// the stream.
func (s *streamBuffer) closeWrite(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && len(s.pending) > 0 {
		err = errors.Errorf("stream buffer: write coverage gap at offset %d", s.frontier)
	}
	s.closed = true
	if s.err == nil {
		s.err = err
	}
	s.cond.Broadcast()
}

// Close releases the buffer, unblocking any blocked WriteAt or Read.
// Cancelling the download itself is the caller's responsibility.
func (s *streamBuffer) Close() error {
	s.mu.Lock()
	s.rclosed = true
	s.cond.Broadcast()
	s.mu.Unlock()
	return nil
}

// extendLocked records [start, end) as written and advances the contiguous
// frontier over any pending intervals it now reaches. Callers must hold mu.
func (s *streamBuffer) extendLocked(start, end int64) {
	if start == s.frontier && len(s.pending) == 0 {
		s.frontier = end
		return
	}
	s.pending = append(s.pending, writeInterval{start, end})
	slices.SortFunc(s.pending, func(a, b writeInterval) int { return cmp.Compare(a.start, b.start) })
	merged := s.pending[:1]
	for _, iv := range s.pending[1:] {
		if last := &merged[len(merged)-1]; iv.start <= last.end {
			last.end = max(last.end, iv.end)
		} else {
			merged = append(merged, iv)
		}
	}
	s.pending = merged
	for len(s.pending) > 0 && s.pending[0].start <= s.frontier {
		s.frontier = max(s.frontier, s.pending[0].end)
		s.pending = s.pending[1:]
	}
}
