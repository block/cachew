package cache

import (
	"context"
	"io"
	"net/textproto"
	"os"
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
	caches []Cache
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
	return Tiered{caches}
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
func (t Tiered) Create(ctx context.Context, key Key, headers textproto.MIMEHeader, ttl time.Duration) (io.WriteCloser, error) {
	// The first error will cancel all outstanding writes.
	ctx, cancel := context.WithCancelCause(ctx)

	tw := tieredWriter{make([]io.WriteCloser, len(t.caches)), cancel}
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
func (t Tiered) Stat(ctx context.Context, key Key) (textproto.MIMEHeader, error) {
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
// If all caches fail, all errors are returned.
func (t Tiered) Open(ctx context.Context, key Key) (io.ReadCloser, textproto.MIMEHeader, error) {
	errs := make([]error, len(t.caches))
	for i, c := range t.caches {
		r, headers, err := c.Open(ctx, key)
		errs[i] = err
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return r, headers, nil
	}
	return nil, nil, errors.Join(errs...)
}

func (t Tiered) String() string {
	names := make([]string, len(t.caches))
	for i, c := range t.caches {
		names[i] = c.String()
	}
	return "tiered:" + strings.Join(names, ",")
}

type tieredWriter struct {
	writers []io.WriteCloser
	cancel  context.CancelCauseFunc
}

var _ io.WriteCloser = (*tieredWriter)(nil)

// Close all writers and return all errors.
func (t tieredWriter) Close() error {
	wg := sync.WaitGroup{}
	errs := make([]error, len(t.writers))
	for i, cache := range t.writers {
		wg.Go(func() { errs[i] = errors.WithStack(cache.Close()) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (t tieredWriter) Write(p []byte) (n int, err error) {
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
