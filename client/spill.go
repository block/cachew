package client

import (
	"cmp"
	"context"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/alecthomas/errors"
)

const (
	// spillPunchAlign keeps hole boundaries at a multiple of any plausible
	// filesystem block size, as required by fallocate and F_PUNCHHOLE.
	spillPunchAlign int64 = 1 << 20
	// spillPunchThreshold batches hole punching to one syscall per few
	// megabytes consumed rather than one per read.
	spillPunchThreshold int64 = 4 << 20

	spillReadBufferSize = 256 << 10
)

// spillBuffer reorders concurrent WriteAt calls into a sequential byte stream
// through an unlinked temporary file, so a parallel ranged download can feed a
// pipe without holding chunks in RAM. streamTo follows the contiguous write
// frontier and punches holes in consumed regions (best effort), keeping the
// file's disk footprint near the gap between download and consumption.
type spillBuffer struct {
	f    *os.File
	name string

	mu       sync.Mutex
	changed  chan struct{}   // closed and replaced whenever frontier or closed changes
	pending  []spillInterval // written ranges not yet contiguous with the frontier
	frontier int64           // contiguous bytes written from offset 0
	closed   bool
	writeErr error
}

type spillInterval struct{ start, end int64 }

func newSpillBuffer(dir string) (*spillBuffer, error) {
	f, err := os.CreateTemp(dir, ".cachew-spill-*")
	if err != nil {
		return nil, errors.Wrap(err, "create spill file")
	}
	// Unlink immediately so the kernel reclaims the space even if the process
	// dies mid-download; Close removes it again for platforms where unlinking
	// an open file fails.
	_ = os.Remove(f.Name())
	return &spillBuffer{f: f, name: f.Name(), changed: make(chan struct{})}, nil
}

// Close releases the spill file. Callers must not call WriteAt or streamTo
// after Close.
func (s *spillBuffer) Close() error {
	err := s.f.Close()
	if rmErr := os.Remove(s.name); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
		err = errors.Join(err, rmErr)
	}
	return errors.WithStack(err)
}

// WriteAt implements io.WriterAt. Writes at non-overlapping offsets may run
// concurrently.
func (s *spillBuffer) WriteAt(p []byte, off int64) (int, error) {
	n, err := s.f.WriteAt(p, off)
	if n > 0 {
		s.extend(off, off+int64(n))
	}
	return n, errors.WithStack(err)
}

// closeWrite marks the write side finished. A nil err with incomplete
// coverage from offset 0 is reported as an error so a gap can never silently
// truncate the stream. It returns the error recorded for the reader.
func (s *spillBuffer) closeWrite(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && len(s.pending) > 0 {
		err = errors.Errorf("spill: write coverage gap at offset %d", s.frontier)
	}
	s.closed = true
	s.writeErr = err
	s.wake()
	return err
}

// streamTo copies bytes to w in offset order as the contiguous write frontier
// advances, punching holes in consumed regions when the platform supports it.
// It returns nil once the write side is closed and all contiguous bytes have
// been flushed; a write-side error also ends the stream with nil, since the
// writer reports its own failure.
func (s *spillBuffer) streamTo(ctx context.Context, w io.Writer) error {
	buf := make([]byte, spillReadBufferSize)
	var readOff, punched int64
	punchable := true
	for {
		frontier, done, err := s.waitFrontier(ctx, readOff)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		for readOff < frontier {
			n := int(min(frontier-readOff, int64(len(buf))))
			if _, err := s.f.ReadAt(buf[:n], readOff); err != nil {
				return errors.Wrap(err, "read spill file")
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return errors.Wrap(err, "write stream")
			}
			readOff += int64(n)
		}
		if aligned := readOff &^ (spillPunchAlign - 1); punchable && aligned-punched >= spillPunchThreshold {
			// Best effort: on filesystems without hole support the spill
			// costs no more disk than a plain temporary file.
			punchable = punchHole(s.f, punched, aligned-punched) == nil
			if punchable {
				punched = aligned
			}
		}
	}
}

// waitFrontier blocks until the frontier moves past readOff, the write side
// closes, or ctx is cancelled. done reports that no more bytes will arrive.
func (s *spillBuffer) waitFrontier(ctx context.Context, readOff int64) (frontier int64, done bool, err error) {
	s.mu.Lock()
	for s.frontier == readOff && !s.closed {
		ch := s.changed
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, false, errors.WithStack(ctx.Err())
		}
		s.mu.Lock()
	}
	frontier, closed, writeErr := s.frontier, s.closed, s.writeErr
	s.mu.Unlock()
	return frontier, (frontier == readOff && closed) || writeErr != nil, nil
}

// extend records [start, end) as written and advances the contiguous frontier
// over any pending intervals it now reaches.
func (s *spillBuffer) extend(start, end int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, spillInterval{start, end})
	slices.SortFunc(s.pending, func(a, b spillInterval) int { return cmp.Compare(a.start, b.start) })
	merged := s.pending[:1]
	for _, iv := range s.pending[1:] {
		if last := &merged[len(merged)-1]; iv.start <= last.end {
			last.end = max(last.end, iv.end)
		} else {
			merged = append(merged, iv)
		}
	}
	s.pending = merged
	advanced := false
	for len(s.pending) > 0 && s.pending[0].start <= s.frontier {
		s.frontier = max(s.frontier, s.pending[0].end)
		s.pending = s.pending[1:]
		advanced = true
	}
	if advanced {
		s.wake()
	}
}

// wake must be called with mu held.
func (s *spillBuffer) wake() {
	close(s.changed)
	s.changed = make(chan struct{})
}
