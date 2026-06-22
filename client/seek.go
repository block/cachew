package client

import (
	"io"

	"github.com/alecthomas/errors"
)

// SeekReadCloser implements io.ReadSeekCloser for a stream that is opened once,
// at a chosen offset, and then read sequentially (e.g. an HTTP body backed by a
// ranged GET, or an object-store ranged read).
//
// Seeks are free: any number may choose the start offset before the first Read,
// including io.SeekEnd (resolved against the known size), and they perform no
// I/O — they only update and report the position. The first Read opens the
// underlying stream once, at the final offset, via the supplied open function.
// Once reading has begun Seek returns an error.
type SeekReadCloser struct {
	size    int64
	open    func(offset int64) (io.ReadCloser, error)
	offset  int64
	rc      io.ReadCloser
	started bool
	closed  bool
}

// NewSeekReadCloser returns a SeekReadCloser of the given stream size. open is
// called once, on the first Read, to open the underlying stream at the seeked
// offset.
func NewSeekReadCloser(size int64, open func(offset int64) (io.ReadCloser, error)) *SeekReadCloser {
	return &SeekReadCloser{size: size, open: open}
}

func (s *SeekReadCloser) Seek(offset int64, whence int) (int64, error) {
	if s.started {
		return 0, errors.New("seek after read is not supported")
	}
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.offset + offset
	case io.SeekEnd:
		abs = s.size + offset
	default:
		return 0, errors.Errorf("seek: invalid whence %d", whence)
	}
	// Per io.Seeker, seeking before the start is an error, but seeking to or
	// past the end is allowed and returns the requested offset; only subsequent
	// I/O is affected (see Read).
	if abs < 0 {
		return 0, errors.New("seek: negative position")
	}
	s.offset = abs
	return abs, nil
}

func (s *SeekReadCloser) Read(p []byte) (int, error) {
	if s.closed {
		return 0, errors.New("read after close")
	}
	if !s.started {
		s.started = true
		// A start at or past EOF has nothing to read, and a ranged open there
		// would be invalid (start at or beyond the last byte), so leave rc nil
		// and report io.EOF — the implementation-dependent I/O the io.Seeker
		// contract permits for a past-end seek.
		if s.size < 0 || s.offset < s.size {
			rc, err := s.open(s.offset)
			if err != nil {
				return 0, err
			}
			s.rc = rc
		}
	}
	if s.rc == nil {
		return 0, io.EOF
	}
	n, err := s.rc.Read(p)
	return n, err //nolint:wrapcheck // preserve io.EOF and underlying errors verbatim
}

func (s *SeekReadCloser) Close() error {
	s.closed = true
	if s.rc != nil {
		return errors.WithStack(s.rc.Close())
	}
	return nil
}
