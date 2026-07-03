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
	"github.com/block/cachew/internal/metadatadb"
)

// The Tiered cache combines multiple caches.
//
// It is not directly selectable from configuration, but instead is automatically used if multiple caches are
// configured.
type Tiered struct {
	caches    []Cache
	metadata  *metadatadb.Store
	etags     *metadatadb.Map[Key, string]
	namespace Namespace
}

// MaybeNewTiered creates a [Tiered] cache from one or more caches.
//
// If no caches are passed it will panic.
func MaybeNewTiered(ctx context.Context, caches []Cache, metadata *metadatadb.Store) Cache {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing tiered cache", "tiers", len(caches))
	if len(caches) == 0 {
		panic("Tiered cache requires at least one backing cache")
	}
	if len(caches) == 1 {
		return authoritativeCache{Cache: caches[0]}
	}
	if metadata == nil {
		panic("Tiered cache requires a metadata store")
	}
	return Tiered{caches: caches, metadata: metadata, etags: tieredETags(metadata, "")}
}

type authoritativeCache struct {
	Cache
}

func (c authoritativeCache) Invalidate(context.Context, Key) error {
	return nil
}

func (c authoritativeCache) Namespace(namespace Namespace) Cache {
	return authoritativeCache{Cache: c.Cache.Namespace(namespace)}
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
func (t Tiered) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	rawETag, quotedETag, err := createETag(opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	createOpts := []Option{WithETag(rawETag)}

	replaceETag, err := t.replacementETag(ctx, key, quotedETag)
	if err != nil {
		return nil, err
	}

	// The first error will cancel all outstanding writes.
	ctx, cancel := context.WithCancelCause(ctx)

	tw := &tieredWriter{
		writers:     make([]Writer, len(t.caches)),
		cancel:      cancel,
		etags:       t.etags,
		key:         key,
		etag:        quotedETag,
		replaceETag: replaceETag,
	}
	// Note: we can't use errgroup here because we do not want to cancel the context on Wait().
	wg := sync.WaitGroup{}
	for i, cache := range t.caches {
		wg.Go(func() {
			w, err := cache.Create(ctx, key, headers, ttl, createOpts...)
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

func (t Tiered) replacementETag(ctx context.Context, key Key, newETag string) (bool, error) {
	headers, err := t.caches[len(t.caches)-1].Stat(ctx, key)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	case err != nil:
		return false, errors.WithStack(err)
	default:
		return headers.Get(ETagKey) != newETag, nil
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
	err := errors.Join(errs...)
	if err == nil {
		err = errors.Wrap(t.etags.Delete(key), "delete tiered etag")
	}
	return err
}

// Invalidate evicts stale local copies from every non-authoritative tier.
// The final tier is authoritative by construction, so invalidation leaves it
// intact even if that backend's own Invalidate method would remove the object.
func (t Tiered) Invalidate(ctx context.Context, key Key) error {
	if len(t.caches) <= 1 {
		return nil
	}
	wg := sync.WaitGroup{}
	errs := make([]error, len(t.caches)-1)
	for i, cache := range t.caches[:len(t.caches)-1] {
		wg.Go(func() { errs[i] = errors.WithStack(cache.Invalidate(ctx, key)) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Stat returns headers from the first cache that succeeds.
//
// A tier that fails an If-Match precondition holds a different version of the
// object, not a definitive answer: deeper tiers are consulted for the version
// the validator names, and ErrPreconditionFailed is only returned when none
// holds it. A tier that errored while being probed takes precedence, so
// outages are not misreported as missing versions.
//
// If all caches fail, all errors are returned.
func (t Tiered) Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error) {
	rejected := false
	var probeErrs []error
	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		headers, err := c.Stat(ctx, key, opts...)
		errs[i] = err
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case errors.Is(err, ErrPreconditionFailed):
			rejected = true
			continue
		case err != nil && !errors.Is(err, ErrNotModified) && rejected:
			probeErrs = append(probeErrs, errors.WithStack(err))
			continue
		case err != nil && !errors.Is(err, ErrNotModified):
			return headers, errors.WithStack(err)
		}
		if i < len(t.caches)-1 && t.invalidateStale(ctx, c, key, headers) {
			continue
		}
		// Any other outcome (success, ErrNotModified, or a hard error) is
		// definitive for this tier; surface it with its headers.
		return headers, errors.WithStack(err)
	}
	if len(probeErrs) > 0 {
		return nil, errors.Join(probeErrs...)
	}
	if rejected {
		return nil, errors.WithStack(ErrPreconditionFailed)
	}
	return nil, errors.Join(errs...)
}

// Open returns a reader from the first cache that succeeds.
// When a higher tier hits but lower tiers missed, the returned reader
// transparently backfills the lowest tier as the caller reads, so that
// subsequent Opens are served locally.
//
// A tier that holds a different version than the request's validators name —
// a failed If-Match, or an If-Range miss — is not definitive: deeper tiers are
// consulted for the named version, so a replica whose local tier has diverged
// can still satisfy a pinned request from a shared tier. When no tier holds
// it, the first tier's outcome stands: the full representation for an If-Range
// miss (per RFC 9110), ErrPreconditionFailed for a failed If-Match. A tier
// that errored while being probed takes precedence over both, so outages are
// not misreported as missing versions.
//
// If all caches fail, all errors are returned.
func (t Tiered) Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	ro := NewRequestOptions(opts...)
	// A Range request yields a partial body, which must never be backfilled
	// into a lower tier as if it were the whole object.
	partial := ro.Range != ""

	// The first tier whose version missed If-Range supplies the full-body
	// fallback, served only if no deeper tier holds the pinned version.
	var fallback io.ReadCloser
	var fallbackHeaders http.Header
	var probeErrs []error
	rejected := false // a tier failed If-Match; deeper tiers may hold the named version

	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		r, headers, err := c.Open(ctx, key, opts...)
		errs[i] = err
		switch {
		case errors.Is(err, os.ErrNotExist):
			continue
		case errors.Is(err, ErrPreconditionFailed):
			rejected = true
			continue
		case t.invalidateStaleConditional(ctx, i, c, key, headers, err, errs):
			continue
		case errors.Is(err, ErrNotModified), errors.Is(err, ErrRangeNotSatisfiable):
			// This tier's version satisfies the request's validator, so the
			// outcome is definitive. Surface headers so callers can build the
			// conditional response. No body to backfill.
			if fallback != nil {
				discardTieredReader(ctx, key, fallback)
			}
			return nil, headers, errors.WithStack(err)
		case err != nil:
			// A hard error is definitive when no earlier tier produced a
			// servable outcome. Otherwise defer it: a deeper tier may still
			// satisfy the validator, but if none does the error is surfaced in
			// preference to the degraded fallback/412.
			if fallback == nil && !rejected {
				return nil, headers, errors.WithStack(err)
			}
			probeErrs = append(probeErrs, errors.WithStack(err))
			continue
		case i < len(t.caches)-1 && t.invalidateStale(ctx, c, key, headers):
			discardTieredReader(ctx, key, r)
			continue
		case ro.IfRangeMisses(headers.Get(ETagKey)):
			// This tier holds a different version than the range is pinned to:
			// hold its full body as the fallback and probe deeper tiers.
			if fallback != nil {
				discardTieredReader(ctx, key, r)
				continue
			}
			fallback, fallbackHeaders = r, headers
			continue
		}
		if fallback != nil {
			discardTieredReader(ctx, key, fallback)
		}
		if i > 0 && !partial {
			r = t.backfillReader(ctx, key, r, headers, t.caches[0])
		}
		return r, headers, nil
	}
	if len(probeErrs) > 0 {
		if fallback != nil {
			probeErrs = append(probeErrs, fallback.Close())
		}
		return nil, nil, errors.Join(probeErrs...)
	}
	if fallback != nil {
		return fallback, fallbackHeaders, nil
	}
	if rejected {
		return nil, nil, errors.WithStack(ErrPreconditionFailed)
	}
	return nil, nil, errors.Join(errs...)
}

func discardTieredReader(ctx context.Context, key Key, r io.ReadCloser) {
	if err := r.Close(); err != nil {
		logging.FromContext(ctx).WarnContext(ctx, "Tiered: failed to close superseded reader", "key", key, "error", err)
	}
}

// backfillReader wraps src so that every byte read is also written to dst.
// On successful close the dst entry becomes available for future reads.
// On error or partial read the dst entry is discarded per the Cache contract
// (the context is cancelled, causing the writer to discard on Close).
func (t Tiered) backfillReader(ctx context.Context, key Key, src io.ReadCloser, headers http.Header, dst Cache) io.ReadCloser {
	logger := logging.FromContext(ctx)
	// Use a cancellable context so we can abort the write on failure.
	// The Cache contract guarantees that cancelled-context writes are discarded.
	writeCtx, cancel := context.WithCancel(ctx)
	createOpts := backfillCreateOptions(headers)
	w, err := dst.Create(writeCtx, key, headers, 0, createOpts...) // 0 → use the cache's max TTL
	if err != nil {
		cancel()
		logger.WarnContext(ctx, "Tier backfill: failed to create writer, skipping", "error", err)
		return src
	}
	return newBackfillReadCloser(ctx, src, w, cancel)
}

const tieredETagsMap = "cache-etags"

func tieredETags(metadata *metadatadb.Store, namespace Namespace) *metadatadb.Map[Key, string] {
	return metadatadb.NewMap[Key, string](metadata.Namespace(string(namespace)), tieredETagsMap)
}

func (t Tiered) invalidateStale(ctx context.Context, c Cache, key Key, headers http.Header) bool {
	want, ok := t.etags.Get(key)
	if !ok || want == headers.Get(ETagKey) {
		return false
	}
	if err := c.Invalidate(ctx, key); err != nil {
		logging.FromContext(ctx).WarnContext(ctx, "Tiered: failed to invalidate stale tier", "key", key, "error", err)
	}
	return true
}

func (t Tiered) invalidateStaleConditional(
	ctx context.Context,
	tier int,
	c Cache,
	key Key,
	headers http.Header,
	err error,
	errs []error,
) bool {
	if tier == len(t.caches)-1 || (!errors.Is(err, ErrNotModified) && !errors.Is(err, ErrRangeNotSatisfiable)) {
		return false
	}
	if !t.invalidateStale(ctx, c, key, headers) {
		return false
	}
	errs[tier] = os.ErrNotExist
	return true
}

func backfillCreateOptions(headers http.Header) []Option {
	rawETag, err := RawETagFromHeader(headers.Get(ETagKey))
	if err != nil {
		return nil
	}
	return []Option{WithETag(rawETag)}
}

// backfillReadCloser tees reads from src into dst asynchronously. Chunks are
// sent to a background goroutine via a buffered channel so the Read path is
// never blocked by disk I/O (up to ~32 MB of buffer). If the full stream is
// consumed and Close completes without error, dst is closed normally
// (committing the cached entry). On any write failure the backfill is
// abandoned but reads continue unaffected.
type backfillReadCloser struct {
	src     io.ReadCloser
	ch      chan []byte
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan error
	closed  bool
	closeMu sync.Mutex
}

const backfillBufSize = 128 // number of chunks buffered (~32 MB at 256 KB each)

func newBackfillReadCloser(ctx context.Context, src io.ReadCloser, dst io.WriteCloser, cancel context.CancelFunc) *backfillReadCloser {
	ch := make(chan []byte, backfillBufSize)
	done := make(chan error, 1)
	b := &backfillReadCloser{src: src, ch: ch, ctx: ctx, cancel: cancel, done: done}
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
				// Buffer full — abandon backfill.
				b.closed = true
				close(b.ch)
				b.cancel()
			}
		}
		b.closeMu.Unlock()
	}
	if err != nil {
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
	writers     []Writer
	cancel      context.CancelCauseFunc
	etags       *metadatadb.Map[Key, string]
	key         Key
	etag        string
	replaceETag bool
	closed      bool
	aborted     bool
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
	err := errors.Join(errs...)
	if err == nil && !t.aborted && t.replaceETag {
		err = errors.Wrap(t.etags.Set(t.key, t.etag), "set tiered etag")
	}
	return err
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
	return Tiered{caches: namespaced, metadata: t.metadata, etags: tieredETags(t.metadata, namespace), namespace: namespace}
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
