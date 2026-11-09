package spool

import (
	"context"
	"io"
	"maps"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
)

// ErrNotShared reports that a response cannot be served from the spool. Nothing
// has been written to the client, so the caller must serve the request itself.
var ErrNotShared = errors.New("response not shared")

// ErrUncommitted reports a fill that stored a cache write and returned without
// calling Writer.Commit, which would otherwise lose the entry silently.
var ErrUncommitted = errors.New("spool fill returned without committing its cache write")

// Retention controls whether a Group keeps a successful response after its fill
// ends.
type Retention int

const (
	// ReleaseCompleted drops a successful response when its fill ends, for
	// callers that cache it so later requests hit the cache instead.
	ReleaseCompleted Retention = iota
	// RetainCompleted keeps successful responses until the Group is closed, for
	// callers with nowhere else to serve them from.
	RetainCompleted
)

// FillFunc produces the shared response into w. ctx keeps the starting request's
// values but not its cancellation; it ends at Registry.Close or FillTimeout.
type FillFunc func(ctx context.Context, w *Writer) error

// Result describes how Serve answered a request.
type Result struct {
	// Filled is true when this request started the fill.
	Filled bool
	// Status is the status code written to the client, or 0 if none was.
	Status int
	// Wait is how long the request waited for the response headers.
	Wait time.Duration
	// Bytes is the number of body bytes written to the client.
	Bytes int64
}

// Group coalesces fills by key.
type Group struct {
	registry  *Registry
	retention Retention
	mu        sync.Mutex
	entries   map[string]*entry
	closed    bool
}

// Serve answers req with the shared response for key. On ErrNotShared nothing was
// written; any other error means a truncated body and the caller must abort.
func (g *Group) Serve(w http.ResponseWriter, req *http.Request, key string, fill FillFunc) (Result, error) {
	e, filled := g.join(req, key, fill)
	if e == nil {
		return Result{}, ErrNotShared
	}
	defer e.leave()
	return e.serve(w, req, filled)
}

// Close drops every response in the group. Requests already streaming one
// finish undisturbed; later Serve calls return ErrNotShared.
func (g *Group) Close() {
	g.mu.Lock()
	g.closed = true
	entries := g.entries
	g.entries = nil
	g.mu.Unlock()
	for _, e := range entries {
		e.markRemoved()
	}
}

func (g *Group) join(req *http.Request, key string, fill FillFunc) (*entry, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil, false
	}
	if e, ok := g.entries[key]; ok {
		if e.addReader() {
			return e, false
		}
		e.markRemoved()
	}
	e := &entry{
		group:   g,
		key:     key,
		leader:  &http.Request{Method: req.Method, Header: req.Header.Clone()},
		changed: make(chan struct{}),
		readers: 1,
	}
	g.entries[key] = e
	g.registry.start(e, req, fill)
	return e, true
}

func (g *Group) remove(e *entry) {
	g.mu.Lock()
	if g.entries[e.key] == e {
		delete(g.entries, e.key)
	}
	g.mu.Unlock()
	e.markRemoved()
}

// entry is one shared response. Its source and file are released once the
// entry has left its group, its fill has finished and no request is reading it.
type entry struct {
	group *Group
	key   string
	// leader is the request that started the fill, reduced to the fields the
	// reuse rules read so it can outlive that request.
	leader *http.Request

	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every state change
	status  int
	header  http.Header
	shared  bool
	// expires is when a shared error stops accepting readers, counted from receipt
	// of its headers; zero for every other response.
	expires time.Time
	source  io.ReaderAt
	release func() error
	written int64
	done    bool
	err     error
	readers int
	removed bool
}

func (e *entry) notifyLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

// addReader joins a request to e, refusing once a shared error has expired so the
// caller starts a fresh fill instead.
func (e *entry) addReader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.expires.IsZero() && !time.Now().Before(e.expires) {
		return false
	}
	e.readers++
	return true
}

func (e *entry) leave() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.readers--
	e.releaseIfUnusedLocked()
}

func (e *entry) markRemoved() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removed = true
	e.releaseIfUnusedLocked()
}

func (e *entry) releaseIfUnusedLocked() {
	if !e.removed || !e.done || e.readers > 0 || e.release == nil {
		return
	}
	release := e.release
	e.release = nil
	if err := release(); err != nil {
		e.group.registry.onReleaseError(errors.Wrap(err, "release spooled response"))
	}
}

func (e *entry) publishHeader(status int, header http.Header) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status, e.header = status, header
	if status >= 200 && status < 300 {
		e.shared = true
	} else {
		window := e.group.registry.config.ErrorWindow
		lifetime, shared := httputil.ReuseLifetime(e.leader, status, header, window)
		e.shared = shared
		if shared {
			e.expires = time.Now().Add(min(lifetime, window))
		}
	}
	e.notifyLocked()
}

func (e *entry) setSource(source io.ReaderAt, release func() error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.source, e.release = source, release
}

func (e *entry) advance(n int) {
	if n == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.written += int64(n)
	e.notifyLocked()
}

// finish commits or aborts the fill's cache write, publishes the outcome and
// decides how long the group keeps the response.
func (e *entry) finish(w *Writer, fillErr error) {
	fillErr = w.abortUncommitted(fillErr)
	if fillErr == nil && w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}

	e.mu.Lock()
	retain, forever := e.retentionLocked(fillErr)
	e.mu.Unlock()
	// Leave the group before publishing completion so no new request joins a
	// response that is about to be dropped.
	if !forever && retain <= 0 {
		e.group.remove(e)
	}

	e.mu.Lock()
	e.done, e.err = true, fillErr
	e.notifyLocked()
	e.releaseIfUnusedLocked()
	e.mu.Unlock()

	if retain > 0 {
		time.AfterFunc(retain, func() { e.group.remove(e) })
	}
}

func (e *entry) retentionLocked(fillErr error) (retain time.Duration, forever bool) {
	switch {
	case fillErr != nil, e.status == 0:
		return 0, false
	case e.status >= 200 && e.status < 300:
		return 0, e.group.retention == RetainCompleted
	case e.shared:
		return time.Until(e.expires), false
	default:
		return 0, false
	}
}

// serve streams the response to w. It reads source only below written, which
// the fill advances after each write lands, so it never sees unwritten bytes.
func (e *entry) serve(w http.ResponseWriter, req *http.Request, filled bool) (Result, error) {
	result := Result{Filled: filled}
	start := time.Now()
	headerSent := false
	var offset int64
	for {
		e.mu.Lock()
		status, header, shared := e.status, e.header, e.shared
		source, written, done, fillErr, changed := e.source, e.written, e.done, e.err, e.changed
		e.mu.Unlock()

		if !headerSent && status == 0 && done {
			return result, ErrNotShared
		}
		if !headerSent && status != 0 {
			result.Wait = time.Since(start)
			if !filled && !e.reusableBy(req, status, header, shared) {
				return result, ErrNotShared
			}
			maps.Copy(w.Header(), header)
			w.WriteHeader(status)
			result.Status = status
			headerSent = true
		}
		if headerSent && written > offset {
			n, err := io.Copy(w, io.NewSectionReader(source, offset, written-offset))
			offset += n
			result.Bytes += n
			if err == nil && offset < written {
				err = io.ErrUnexpectedEOF
			}
			if err != nil {
				return result, errors.Wrap(err, "write spooled response")
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			continue
		}
		if headerSent && done {
			return result, errors.Wrap(fillErr, "spool fill failed")
		}
		select {
		case <-req.Context().Done():
			return result, errors.WithStack(req.Context().Err())
		case <-changed:
		}
	}
}

// reusableBy reports whether a follower may reuse the response. Errors are also
// checked against the follower, so its credentials and varied headers count.
func (e *entry) reusableBy(req *http.Request, status int, header http.Header, shared bool) bool {
	if !shared {
		return false
	}
	if status >= 200 && status < 300 {
		return true
	}
	if _, ok := httputil.ReuseLifetime(req, status, header, e.group.registry.config.ErrorWindow); !ok {
		return false
	}
	return httputil.VaryMatches(header, e.leader.Header, req.Header)
}

// Writer receives a fill's response so existing forwarding code can write into it.
// It must only be used by the fill it was passed to.
type Writer struct {
	entry  *entry
	dir    string
	header http.Header
	status int
	// cache is the Store destination; nil once the write has been abandoned.
	cache cache.Writer
	// follow is true when readers follow the cache write instead of a spool file.
	follow    bool
	stored    bool
	committed bool
	cacheErr  error
	file      *os.File
}

var _ http.ResponseWriter = (*Writer)(nil)
var _ http.Flusher = (*Writer)(nil)

func (w *Writer) Header() http.Header { return w.header }

// WriteHeader publishes status and a snapshot of the headers to readers.
// Informational statuses are not forwarded.
func (w *Writer) WriteHeader(status int) {
	if w.status != 0 || status < http.StatusOK {
		return
	}
	w.status = status
	w.entry.publishHeader(status, w.header.Clone())
}

// Store takes ownership of cw, committing it if the fill succeeds. It must follow
// a 2xx WriteHeader and precede the first Write.
func (w *Writer) Store(cw cache.Writer) error {
	if w.status < 200 || w.status >= 300 || w.file != nil || w.stored || w.committed {
		err := errors.Errorf("spool store requires a 2xx status and no body, got status %d", w.status)
		return errors.Join(err, cw.Abort(err))
	}
	w.cache, w.stored = cw, true
	pending, ok := cw.(cache.PendingReader)
	if !ok {
		return nil
	}
	source, err := pending.OpenPending()
	if errors.Is(err, cache.ErrPendingReadUnsupported) {
		return nil
	}
	if err != nil {
		w.cache, w.stored = nil, false
		return errors.Join(errors.Wrap(err, "open pending cache object"), cw.Abort(err))
	}
	w.follow = true
	w.entry.setSource(source, source.Close)
	return nil
}

// Write appends p to the response. A cache failure fails the response only when
// readers follow the cache write; otherwise the cache write is abandoned.
func (w *Writer) Write(p []byte) (int, error) {
	if w.committed {
		return 0, errors.New("spool write after commit")
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.follow {
		n, err := w.cache.Write(p)
		w.entry.advance(n)
		if err != nil {
			return n, errors.Wrap(err, "write cache")
		}
		return n, nil
	}
	if err := w.openFile(); err != nil {
		return 0, err
	}
	n, err := w.file.Write(p)
	w.entry.advance(n)
	if err != nil {
		return n, errors.Wrap(err, "write spool file")
	}
	if w.cache != nil {
		if _, err := w.cache.Write(p); err != nil {
			w.cacheErr = errors.Join(err, w.cache.Abort(err))
			w.cache = nil
		}
	}
	return n, nil
}

// Flush is a no-op: readers see each Write as soon as it returns.
func (w *Writer) Flush() {}

func (w *Writer) openFile() error {
	if w.file != nil {
		return nil
	}
	f, err := os.CreateTemp(w.dir, "*"+fileSuffix)
	if err != nil {
		return errors.Wrap(err, "create spool file")
	}
	w.file = f
	w.entry.setSource(f, func() error {
		return errors.Join(f.Close(), os.Remove(f.Name())) //nolint:gosec // Created by CreateTemp in the spool directory.
	})
	return nil
}

// Commit stores the body in the cache and returns any cache failure, including
// an abandoned write. Call it after the last Write; it is a no-op without Store.
func (w *Writer) Commit() error {
	if w.committed {
		return nil
	}
	w.committed = true
	err := w.cacheErr
	if w.cache != nil {
		err = errors.Join(err, w.cache.Close())
	}
	if err != nil {
		return errors.Wrap(err, "commit cache write")
	}
	return nil
}

func (w *Writer) abortUncommitted(fillErr error) error {
	if !w.stored || w.committed {
		return fillErr
	}
	if fillErr == nil {
		fillErr = errors.WithStack(ErrUncommitted)
	}
	err := errors.Join(fillErr, w.cacheErr)
	if w.cache != nil {
		err = errors.Join(err, w.cache.Abort(fillErr))
	}
	return err
}
