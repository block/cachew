package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// The Tiered cache combines multiple caches.
//
// It is not directly selectable from configuration, but instead is automatically used if multiple caches are
// configured.
type Tiered struct {
	caches    []Cache
	namespace Namespace
	// backfills coordinates lower-tier backfills: it deduplicates concurrent
	// backfills of the same key and lets a Create cancel an in-flight backfill.
	// It is a shared pointer so namespaced views observe the same state.
	backfills *backfillManager
}

// MaybeNewTiered creates a [Tiered] cache if multiple are provided, or if there is only one it will return that cache.
//
// If no caches are passed it will panic.
func MaybeNewTiered(ctx context.Context, caches []Cache) Cache {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing tiered cache", "tiers", len(caches))
	if len(caches) == 0 {
		panic("Tiered cache requires at least one backing cache")
	}
	if len(caches) == 1 {
		return caches[0]
	}
	return Tiered{caches: caches, backfills: newBackfillManager()}
}

// backfillKey identifies an in-flight backfill, scoped by namespace so the same
// key in different namespaces does not collide.
type backfillKey struct {
	namespace Namespace
	key       Key
}

// backfillManager tracks in-flight lower-tier backfills so that at most one runs
// per key and a write to the key can cancel it.
type backfillManager struct {
	mu       sync.Mutex
	nextID   uint64
	inFlight map[backfillKey]backfillEntry
}

// backfillEntry is a registered backfill. The id distinguishes successive
// backfills of the same key so a finishing backfill cannot deregister a newer
// one that took its slot after a cancel.
type backfillEntry struct {
	id     uint64
	cancel context.CancelFunc
}

func newBackfillManager() *backfillManager {
	return &backfillManager{inFlight: map[backfillKey]backfillEntry{}}
}

// acquire registers an in-flight backfill, returning its id and true, or false
// if one is already running for the key.
func (m *backfillManager) acquire(namespace Namespace, key Key, cancel context.CancelFunc) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bk := backfillKey{namespace, key}
	if _, ok := m.inFlight[bk]; ok {
		return 0, false
	}
	m.nextID++
	m.inFlight[bk] = backfillEntry{id: m.nextID, cancel: cancel}
	return m.nextID, true
}

// release deregisters the backfill with the given id, but only if it is still
// the registered one — a newer backfill (different id) is left untouched.
func (m *backfillManager) release(namespace Namespace, key Key, id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bk := backfillKey{namespace, key}
	if entry, ok := m.inFlight[bk]; ok && entry.id == id {
		delete(m.inFlight, bk)
	}
}

// cancel stops any in-flight backfill for the key, used when the key is being
// overwritten so a stale backfill cannot clobber the new data.
func (m *backfillManager) cancel(namespace Namespace, key Key) {
	m.mu.Lock()
	bk := backfillKey{namespace, key}
	entry, ok := m.inFlight[bk]
	delete(m.inFlight, bk)
	m.mu.Unlock()
	if ok {
		entry.cancel()
	}
}

var _ Cache = (*Tiered)(nil)

// Close all underlying caches.
func (t Tiered) Close() error {
	wg := sync.WaitGroup{}
	errs := make([]error, len(t.caches))
	for i, cache := range t.caches {
		wg.Go(func() { errs[i] = errors.WithStack(cache.Close()) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Create a new object. All underlying caches will be written to in sequence.
func (t Tiered) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (Writer, error) {
	// A write supersedes any in-flight backfill of the same key.
	t.backfills.cancel(t.namespace, key)

	// The first error will cancel all outstanding writes.
	ctx, cancel := context.WithCancelCause(ctx)

	tw := &tieredWriter{
		writers:   make([]Writer, len(t.caches)),
		cancel:    cancel,
		backfills: t.backfills,
		namespace: t.namespace,
		key:       key,
	}
	// Note: we can't use errgroup here because we do not want to cancel the context on Wait().
	wg := sync.WaitGroup{}
	for i, cache := range t.caches {
		wg.Go(func() {
			w, err := cache.Create(ctx, key, headers, ttl)
			if err != nil {
				cancel(err)
			}
			tw.writers[i] = w
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return tw, nil

	case <-ctx.Done():
		return nil, errors.WithStack(context.Cause(ctx))
	}
}

// Delete from all underlying caches. All errors are returned.
func (t Tiered) Delete(ctx context.Context, key Key) error {
	wg := sync.WaitGroup{}
	errs := make([]error, len(t.caches))
	for i, cache := range t.caches {
		wg.Go(func() { errs[i] = errors.WithStack(cache.Delete(ctx, key)) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Stat returns headers from the first cache that succeeds.
//
// If all caches fail, all errors are returned.
func (t Tiered) Stat(ctx context.Context, key Key) (http.Header, error) {
	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		headers, err := c.Stat(ctx, key)
		errs[i] = err
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, errors.WithStack(err)
		}
		return headers, nil
	}
	return nil, errors.Join(errs...)
}

// Open returns a reader from the first cache that succeeds.
//
// When a higher tier hits but lower tiers missed, the lowest tier is backfilled
// so that subsequent Opens are served locally. A full read (0, -1) backfills by
// teeing bytes as the caller reads them; a partial read triggers a background
// full backfill instead, since a fragment must not be stored as the whole
// object. At most one backfill runs per key, and a concurrent Create cancels it.
//
// If all caches fail, all errors are returned.
func (t Tiered) Open(ctx context.Context, key Key, start, end int64) (io.ReadCloser, http.Header, error) {
	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		r, headers, err := c.Open(ctx, key, start, end)
		errs[i] = err
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if i > 0 {
			if start == 0 && end == -1 {
				r = t.backfillReader(ctx, key, r, headers, t.caches[0])
			} else {
				t.startBackgroundBackfill(ctx, key, t.caches[i], t.caches[0])
			}
		}
		return r, headers, nil
	}
	return nil, nil, errors.Join(errs...)
}

// backfillReader wraps src so that every byte read is also written to dst.
// On successful close the dst entry becomes available for future reads.
// On error or partial read the dst entry is discarded per the Cache contract
// (the context is cancelled, causing the writer to discard on Close).
//
// If a backfill is already in flight for the key, src is returned unwrapped.
func (t Tiered) backfillReader(ctx context.Context, key Key, src io.ReadCloser, headers http.Header, dst Cache) io.ReadCloser {
	logger := logging.FromContext(ctx)
	// Use a cancellable context so we can abort the write on failure.
	// The Cache contract guarantees that cancelled-context writes are discarded.
	writeCtx, cancel := context.WithCancel(ctx)
	id, ok := t.backfills.acquire(t.namespace, key, cancel)
	if !ok {
		cancel()
		return src
	}
	w, err := dst.Create(writeCtx, key, headers, 0) // 0 → use the cache's max TTL
	if err != nil {
		cancel()
		t.backfills.release(t.namespace, key, id)
		logger.WarnContext(ctx, "Tier backfill: failed to create writer, skipping", "error", err)
		return src
	}
	release := func() { t.backfills.release(t.namespace, key, id) }
	return newBackfillReadCloser(ctx, src, w, cancel, release)
}

// startBackgroundBackfill copies the full object from src into dst in the
// background, so a partial read still populates the lowest tier with the whole
// object. It is deduplicated and cancellable via the backfill manager. The
// background work uses a context detached from the request so it outlives it.
func (t Tiered) startBackgroundBackfill(ctx context.Context, key Key, src, dst Cache) {
	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	id, ok := t.backfills.acquire(t.namespace, key, cancel)
	if !ok {
		cancel()
		return
	}
	logger := logging.FromContext(ctx)
	go func() {
		defer cancel()
		defer t.backfills.release(t.namespace, key, id)

		r, headers, err := src.Open(bgCtx, key, 0, -1)
		if err != nil {
			logger.WarnContext(bgCtx, "Tier backfill: full open failed", "key", key, "error", err)
			return
		}
		defer r.Close()

		err = WriteFunc(bgCtx, dst, key, headers, 0, func(w io.Writer) error {
			_, err := io.Copy(w, r)
			return errors.WithStack(err)
		})
		if err != nil {
			logger.WarnContext(bgCtx, "Tier backfill: write failed", "key", key, "error", err)
		}
	}()
}

// backfillReadCloser tees reads from src into dst asynchronously. Chunks are
// sent to a background goroutine via a buffered channel so the Read path is
// never blocked by disk I/O (up to ~32 MB of buffer). The dst entry is committed
// ONLY when the full source is consumed (src returns io.EOF). An early Close
// (caller abandoned the read), a mid-stream read error, or a backfill write
// failure leaves completed=false and cancels the write, so a partial object is
// never committed as if it were whole. Reads are never affected.
type backfillReadCloser struct {
	src       io.ReadCloser
	ch        chan []byte
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan error
	closed    bool
	completed bool
	closeMu   sync.Mutex
}

const backfillBufSize = 128 // number of chunks buffered (~32 MB at 256 KB each)

func newBackfillReadCloser(ctx context.Context, src io.ReadCloser, dst io.WriteCloser, cancel context.CancelFunc, release func()) *backfillReadCloser {
	ch := make(chan []byte, backfillBufSize)
	done := make(chan error, 1)
	b := &backfillReadCloser{src: src, ch: ch, ctx: ctx, cancel: cancel, done: done}
	go func() {
		// Always release the writeCtx; on the success path nothing else calls
		// cancel, and cancelling after a committed write is a no-op.
		defer cancel()
		defer release()
		var err error
		for chunk := range ch {
			if err == nil {
				if _, wErr := dst.Write(chunk); wErr != nil {
					logging.FromContext(ctx).WarnContext(ctx, "Tier backfill: write failed, abandoning", "error", wErr)
					err = wErr
					cancel()
					// Keep draining the channel so the producer isn't blocked.
				}
			}
		}
		closeErr := dst.Close()
		switch {
		case err != nil:
			done <- err
		case closeErr != nil:
			cancel()
			done <- closeErr
		default:
			done <- nil
		}
	}()
	return b
}

func (b *backfillReadCloser) closeChan() {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	// Commit only if the whole source was read; otherwise cancel so the partial
	// write is discarded per the Cache contract.
	if !b.completed {
		b.cancel()
	}
	close(b.ch)
}

func (b *backfillReadCloser) Read(p []byte) (int, error) {
	n, err := b.src.Read(p)
	if n > 0 {
		b.closeMu.Lock()
		if !b.closed {
			// Copy the data — p is reused by the caller.
			chunk := make([]byte, n)
			copy(chunk, p[:n])
			select {
			case b.ch <- chunk:
			default:
				// Buffer full — abandon backfill (discard, never commit).
				b.closed = true
				b.cancel()
				close(b.ch)
			}
		}
		b.closeMu.Unlock()
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			b.closeMu.Lock()
			b.completed = true
			b.closeMu.Unlock()
		}
		b.closeChan()
	}
	return n, err //nolint:wrapcheck // must return unwrapped io.EOF per io.Reader contract
}

func (b *backfillReadCloser) Close() error {
	srcErr := b.src.Close()
	b.closeChan()
	// Wait for the background writer to finish.
	bgErr := <-b.done
	if srcErr != nil || bgErr != nil {
		b.cancel()
		return errors.WithStack(srcErr)
	}
	return nil
}

func (t Tiered) String() string {
	names := make([]string, len(t.caches))
	for i, c := range t.caches {
		names[i] = c.String()
	}
	return "tiered:" + strings.Join(names, ",")
}

func (t Tiered) Stats(ctx context.Context) (Stats, error) {
	var combined Stats
	for _, c := range t.caches {
		s, err := c.Stats(ctx)
		if errors.Is(err, ErrStatsUnavailable) {
			continue
		}
		if err != nil {
			return Stats{}, errors.Wrap(err, c.String())
		}
		combined.Objects += s.Objects
		combined.Size += s.Size
		combined.Capacity += s.Capacity
	}
	return combined, nil
}

type tieredWriter struct {
	writers   []Writer
	cancel    context.CancelCauseFunc
	closed    bool
	aborted   bool
	backfills *backfillManager
	namespace Namespace
	key       Key
}

var _ Writer = (*tieredWriter)(nil)

func (t *tieredWriter) Abort(err error) error {
	t.aborted = true
	t.cancel(err)
	return t.Close()
}

// Close all writers and return all errors.
func (t *tieredWriter) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true

	wg := sync.WaitGroup{}
	errs := make([]error, len(t.writers))
	for i, cache := range t.writers {
		wg.Go(func() { errs[i] = errors.WithStack(cache.Close()) })
	}
	wg.Wait()

	// A committed write supersedes any backfill that registered while we were
	// writing, so it cannot clobber the freshly written data. Aborted writes
	// discard their data and leave any backfill to restore the prior object.
	if !t.aborted {
		t.backfills.cancel(t.namespace, t.key)
	}
	return errors.Join(errs...)
}

func (t *tieredWriter) Write(p []byte) (n int, err error) {
	for _, cache := range t.writers {
		n, err = cache.Write(p)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				t.cancel(err)
			}
			return n, errors.WithStack(err)
		}
	}
	return
}

// Namespace creates a namespaced view of the tiered cache.
// All underlying caches are also namespaced.
func (t Tiered) Namespace(namespace Namespace) Cache {
	namespaced := make([]Cache, len(t.caches))
	for i, c := range t.caches {
		namespaced[i] = c.Namespace(namespace)
	}
	return Tiered{caches: namespaced, namespace: namespace, backfills: t.backfills}
}

// ListNamespaces returns unique namespaces from all underlying caches.
func (t Tiered) ListNamespaces(ctx context.Context) ([]string, error) {
	namespaceSet := make(map[string]bool)
	for _, c := range t.caches {
		namespaces, err := c.ListNamespaces(ctx)
		if err != nil && !errors.Is(err, ErrStatsUnavailable) {
			return nil, errors.WithStack(err)
		}
		for _, ns := range namespaces {
			namespaceSet[ns] = true
		}
	}

	namespaces := make([]string, 0, len(namespaceSet))
	for ns := range namespaceSet {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	return namespaces, nil
}
