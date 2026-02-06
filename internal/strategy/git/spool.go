package git

import (
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/errors"
)

// ErrSpoolFailed is returned by ServeTo when the spool failed before any
// headers were written to the client, allowing the caller to fall back to
// upstream.
var ErrSpoolFailed = errors.New("spool failed before response started")

// ResponseSpool captures a single HTTP response (headers + body) to a file on disk,
// allowing one writer and multiple concurrent readers. Readers follow the writer,
// blocking when caught up until the write completes.
type ResponseSpool struct {
	mu       sync.Mutex
	cond     *sync.Cond
	filePath string
	file     *os.File
	status   int
	headers  http.Header
	written  int64
	complete bool
	err      error
	readers  sync.WaitGroup
}

func NewResponseSpool(filePath string) (*ResponseSpool, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o750); err != nil {
		return nil, errors.Wrap(err, "create spool directory")
	}
	f, err := os.Create(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "create spool file")
	}
	rs := &ResponseSpool{
		filePath: filePath,
		file:     f,
	}
	rs.cond = sync.NewCond(&rs.mu)
	return rs, nil
}

func (rs *ResponseSpool) CaptureHeader(status int, header http.Header) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.status = status
	rs.headers = header.Clone()
	rs.cond.Broadcast()
}

func (rs *ResponseSpool) Write(data []byte) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.err != nil {
		return rs.err
	}
	n, err := rs.file.Write(data)
	rs.written += int64(n)
	if err != nil {
		rs.err = errors.Wrap(err, "write to spool file")
	}
	rs.cond.Broadcast()
	return rs.err
}

func (rs *ResponseSpool) MarkComplete() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.complete {
		return
	}
	rs.complete = true
	rs.err = errors.Join(rs.err, rs.file.Close())
	rs.cond.Broadcast()
}

func (rs *ResponseSpool) MarkError(err error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.complete {
		return
	}
	rs.err = errors.Join(err, rs.file.Close())
	rs.complete = true
	rs.cond.Broadcast()
}

func (rs *ResponseSpool) Failed() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.err != nil
}

// ServeTo streams the spooled response to w, blocking when caught up to the writer.
func (rs *ResponseSpool) ServeTo(w http.ResponseWriter) error {
	rs.readers.Add(1)
	defer rs.readers.Done()

	// Wait for headers to be available.
	rs.mu.Lock()
	for rs.status == 0 && rs.err == nil {
		rs.cond.Wait()
	}
	if rs.err != nil && rs.status == 0 {
		rs.mu.Unlock()
		return ErrSpoolFailed
	}
	status := rs.status
	headers := rs.headers.Clone()
	rs.mu.Unlock()

	maps.Copy(w.Header(), headers)
	w.WriteHeader(status)

	f, err := os.Open(rs.filePath)
	if err != nil {
		return errors.Wrap(err, "open spool file for reading")
	}
	defer f.Close()

	buf := make([]byte, 32*1024)
	var offset int64
	for {
		rs.mu.Lock()
		for offset >= rs.written && !rs.complete && rs.err == nil {
			rs.cond.Wait()
		}
		written := rs.written
		complete := rs.complete
		spoolErr := rs.err
		rs.mu.Unlock()

		// Read all available data up to `written`.
		for offset < written {
			toRead := min(written-offset, int64(len(buf)))
			n, readErr := f.ReadAt(buf[:toRead], offset)
			if n > 0 {
				if _, writeErr := w.Write(buf[:n]); writeErr != nil {
					return errors.Wrap(writeErr, "write to client from spool")
				}
				offset += int64(n)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
			if readErr != nil && readErr != io.EOF {
				return errors.Wrap(readErr, "read spool")
			}
		}

		if complete && offset >= written {
			if spoolErr != nil {
				return spoolErr
			}
			return nil
		}
	}
}

// WaitForReaders blocks until all active spool readers have finished.
func (rs *ResponseSpool) WaitForReaders() {
	rs.readers.Wait()
}

// SpoolTeeWriter wraps an http.ResponseWriter to capture the response into a spool
// while simultaneously streaming it to the original client.
type SpoolTeeWriter struct {
	inner       http.ResponseWriter
	spool       *ResponseSpool
	wroteHeader bool
}

// NewSpoolTeeWriter creates a new SpoolTeeWriter that tees writes to both the
// inner ResponseWriter and the given spool.
func NewSpoolTeeWriter(inner http.ResponseWriter, spool *ResponseSpool) *SpoolTeeWriter {
	return &SpoolTeeWriter{inner: inner, spool: spool}
}

func (w *SpoolTeeWriter) Header() http.Header {
	return w.inner.Header()
}

func (w *SpoolTeeWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if code >= 200 && code < 300 {
		w.spool.CaptureHeader(code, w.inner.Header())
	} else {
		w.spool.MarkError(errors.Errorf("upstream returned status %d", code))
	}
	w.inner.WriteHeader(code)
}

func (w *SpoolTeeWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if err := w.spool.Write(data); err != nil {
		// Spool write failed; still try to serve the client.
		n, writeErr := w.inner.Write(data)
		return n, errors.Wrap(writeErr, "write to client")
	}
	n, err := w.inner.Write(data)
	if err != nil {
		err = errors.Wrap(err, "write to client")
		w.spool.MarkError(err)
	}
	return n, err
}

func (w *SpoolTeeWriter) Flush() {
	if f, ok := w.inner.(http.Flusher); ok {
		f.Flush()
	}
}

// RepoSpools manages all response spools for a single repository.
type RepoSpools struct {
	mu     sync.Mutex
	dir    string
	spools map[string]*ResponseSpool
	closed atomic.Bool
}

func NewRepoSpools(dir string) *RepoSpools {
	return &RepoSpools{
		dir:    dir,
		spools: make(map[string]*ResponseSpool),
	}
}

// GetOrCreate returns an existing spool for the key, or creates a new one.
// isWriter is true if the caller created the spool and should act as the writer.
func (rp *RepoSpools) GetOrCreate(key string) (spool *ResponseSpool, isWriter bool, err error) {
	if rp.closed.Load() {
		return nil, false, errors.New("repo spools closed")
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()

	if s, exists := rp.spools[key]; exists {
		return s, false, nil
	}

	s, err := NewResponseSpool(filepath.Join(rp.dir, key+".spool"))
	if err != nil {
		return nil, false, err
	}
	rp.spools[key] = s
	return s, true, nil
}

// Close marks the repo spools as closed, waits for all readers to finish,
// and removes spool files from disk.
func (rp *RepoSpools) Close() error {
	rp.closed.Store(true)

	rp.mu.Lock()
	spools := make([]*ResponseSpool, 0, len(rp.spools))
	for _, s := range rp.spools {
		spools = append(spools, s)
	}
	rp.mu.Unlock()

	for _, s := range spools {
		s.WaitForReaders()
	}
	return errors.Wrap(os.RemoveAll(rp.dir), "remove spool directory")
}
