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
	// copying deduplicates background tier-0 warming copies by namespace/key.
	// It is shared by pointer across namespaced views (see Namespace) so dedup
	// spans requests, since Namespace returns a fresh Tiered value per request.
	copying *sync.Map
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
	return Tiered{caches: caches, copying: &sync.Map{}}
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
	// The first error will cancel all outstanding writes.
	ctx, cancel := context.WithCancelCause(ctx)

	tw := &tieredWriter{writers: make([]Writer, len(t.caches)), cancel: cancel}
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
// When a higher tier hits but lower tiers missed, the returned reader
// transparently backfills the lowest tier as the caller reads, so that
// subsequent Opens are served locally.
//
// If all caches fail, all errors are returned.
func (t Tiered) Open(ctx context.Context, key Key) (io.ReadSeekCloser, http.Header, error) {
	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		r, headers, err := c.Open(ctx, key)
		errs[i] = err
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		if i > 0 {
			r = t.backfillReader(ctx, key, r, headers, c, t.caches[0])
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
// from is the tier that produced src; on a ranged read the tee is abandoned and
// a one-shot background full copy from that tier warms dst instead.
func (t Tiered) backfillReader(ctx context.Context, key Key, src io.ReadSeekCloser, headers http.Header, from, dst Cache) io.ReadSeekCloser {
	logger := logging.FromContext(ctx)
	// Use a cancellable context so we can abort the write on failure.
	// The Cache contract guarantees that cancelled-context writes are discarded.
	writeCtx, cancel := context.WithCancel(ctx)
	w, err := dst.Create(writeCtx, key, headers, 0) // 0 → use the cache's max TTL
	if err != nil {
		cancel()
		logger.WarnContext(ctx, "Tier backfill: failed to create writer, skipping", "error", err)
		return src
	}
	trigger := func() { t.triggerBackfillCopy(ctx, key, from, dst) }
	return newBackfillReadCloser(ctx, src, w, cancel, trigger)
}

// triggerBackfillCopy starts a background, request-independent full copy of key
// from the hitting tier into dst, deduplicated per namespace/key so concurrent
// range readers trigger at most one copy. Warming is best-effort: errors are
// logged, not returned.
func (t Tiered) triggerBackfillCopy(ctx context.Context, key Key, from, dst Cache) {
	dedupKey := string(t.namespace) + "/" + key.String()
	if _, loaded := t.copying.LoadOrStore(dedupKey, struct{}{}); loaded {
		return
	}
	bgCtx := context.WithoutCancel(ctx)
	logger := logging.FromContext(bgCtx)
	go func() {
		defer t.copying.Delete(dedupKey)
		rc, headers, err := from.Open(bgCtx, key)
		if err != nil {
			logger.WarnContext(bgCtx, "Tier backfill copy: open failed", "key", key, "error", err)
			return
		}
		defer rc.Close() //nolint:errcheck
		err = WriteFunc(bgCtx, dst, key, headers, 0, func(w io.Writer) error {
			_, cErr := io.Copy(w, rc)
			return errors.WithStack(cErr)
		})
		if err != nil {
			logger.WarnContext(bgCtx, "Tier backfill copy: write failed", "key", key, "error", err)
		}
	}()
}

// backfillReadCloser tees reads from src into dst asynchronously. Chunks are
// sent to a background goroutine via a buffered channel so the Read path is
// never blocked by disk I/O (up to ~32 MB of buffer). If the full stream is
// consumed and Close completes without error, dst is closed normally
// (committing the cached entry). On any write failure the backfill is
// abandoned but reads continue unaffected.
type backfillReadCloser struct {
	src          io.ReadSeekCloser
	ch           chan []byte
	ctx          context.Context
	cancel       context.CancelFunc
	done         chan error
	closed       bool
	closeMu      sync.Mutex
	pos          int64  // current logical position (final pre-read seek, then bytes read)
	started      bool   // true once the first Read has occurred
	completed    bool   // true once the source has been fully read to EOF
	teeAbandoned bool   // true once the tee is given up (ranged read or buffer overflow)
	trigger      func() // fires a one-shot background full copy to warm tier 0
}

const backfillBufSize = 128 // number of chunks buffered (~32 MB at 256 KB each)

func newBackfillReadCloser(ctx context.Context, src io.ReadSeekCloser, dst io.WriteCloser, cancel context.CancelFunc, trigger func()) *backfillReadCloser {
	ch := make(chan []byte, backfillBufSize)
	done := make(chan error, 1)
	b := &backfillReadCloser{src: src, ch: ch, ctx: ctx, cancel: cancel, done: done, trigger: trigger}
	go func() {
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
	if !b.closed {
		b.closed = true
		close(b.ch)
	}
}

func (b *backfillReadCloser) Read(p []byte) (int, error) {
	if !b.started {
		b.started = true
		// A read beginning at a non-zero offset cannot tee usefully (it would
		// write only a slice to tier 0), so give up the tee immediately. Tier 0
		// is instead warmed by the background full copy triggered on Close.
		if b.pos != 0 {
			b.abandon()
		}
	}
	n, err := b.src.Read(p)
	if n > 0 {
		b.pos += int64(n)
		b.closeMu.Lock()
		if !b.closed {
			// Copy the data — p is reused by the caller.
			chunk := make([]byte, n)
			copy(chunk, p[:n])
			select {
			case b.ch <- chunk:
			default:
				// Buffer full — give up the tee.
				b.closed = true
				b.teeAbandoned = true
				close(b.ch)
				b.cancel()
			}
		}
		b.closeMu.Unlock()
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			b.completed = true
		}
		b.closeChan()
	}
	return n, err //nolint:wrapcheck // must return unwrapped io.EOF per io.Reader contract
}

// Seek positions the source before the first read; any number of seeks
// (including io.SeekEnd) are allowed until then, after which it returns an
// error. How tier 0 is warmed — the cheap tee versus a background full copy —
// is decided on Close based on how much of the object was actually consumed.
func (b *backfillReadCloser) Seek(offset int64, whence int) (int64, error) {
	if b.started {
		return 0, errors.New("seek after read is not supported")
	}
	abs, err := b.src.Seek(offset, whence)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	b.pos = abs
	return abs, nil
}

// abandon gives up the tee and cancels the tier-0 write so its partial entry is
// discarded.
func (b *backfillReadCloser) abandon() {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if !b.closed {
		b.closed = true
		b.teeAbandoned = true
		close(b.ch)
		b.cancel()
	}
}

func (b *backfillReadCloser) Close() error {
	// The cheap tee warms tier 0 only when the whole object was streamed
	// sequentially from offset 0. For any other consumption — a ranged read
	// (including a zero-based prefix range such as bytes=0-1023), a partial
	// read, or a buffer-overflow abandon — discard any partial tier-0 write and,
	// if a read actually began, warm tier 0 with a one-shot background full copy
	// instead. A close-without-read (e.g. a 304 short-circuit) warms nothing.
	teeCommitted := b.completed && !b.teeAbandoned
	if !teeCommitted {
		b.cancel()
		if b.started {
			b.trigger()
		}
	}
	srcErr := b.src.Close()
	b.closeChan()
	// Wait for the background writer to finish. Its error (a best-effort warming
	// failure, already logged) is intentionally not propagated.
	<-b.done
	return errors.WithStack(srcErr)
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
	writers []Writer
	cancel  context.CancelCauseFunc
	closed  bool
}

var _ Writer = (*tieredWriter)(nil)

func (t *tieredWriter) Abort(err error) error {
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
	return Tiered{caches: namespaced, namespace: namespace, copying: t.copying}
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
