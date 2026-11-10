package httputil

import (
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

// BackoffTransport shares upstream cooldowns across requests and redirect hops.
// Cooldowns are per origin and isolated by [BackoffScope].
type BackoffTransport struct {
	base  http.RoundTripper
	delay time.Duration
	mu    sync.Mutex
	until map[backoffKey]time.Time
}

type backoffKey struct {
	scope  *BackoffScope
	origin string
}

// NewBackoffTransport uses delay when a 429 has no valid Retry-After header.
func NewBackoffTransport(base http.RoundTripper, delay time.Duration) *BackoffTransport {
	return &BackoffTransport{base: base, delay: delay, until: map[backoffKey]time.Time{}}
}

// RoundTrip rejects requests to a cooling origin within their scope without retrying them.
func (t *BackoffTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	cooldown := backoffKeyFor(r)
	now := time.Now()
	t.mu.Lock()
	for k, until := range t.until {
		if !until.After(now) {
			delete(t.until, k)
		}
	}
	until := t.until[cooldown]
	t.mu.Unlock()
	if until.After(now) {
		t.recordCooldown(r, until)
		if r.Body != nil {
			_ = r.Body.Close()
		}
		body := "upstream rate limited; retry later\n"
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     "429 Too Many Requests",
			Header: http.Header{
				"Retry-After":   {strconv.FormatInt(int64(math.Ceil(until.Sub(now).Seconds())), 10)},
				"Cache-Control": {"no-store"},
				"Content-Type":  {"text/plain; charset=utf-8"},
			},
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: r,
		}, nil
	}
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		now = time.Now()
		var valid bool
		until, valid = retryAfter(resp.Header.Get("Retry-After"), now, t.delay)
		t.recordCooldown(r, until)
		// The cooldown ends the 429; a downstream cache must not keep it longer.
		resp.Header.Set("Cache-Control", "no-store")
		if !valid {
			resp.Header.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(until.Sub(now).Seconds())), 10))
		}
	}
	return resp, nil
}

func (t *BackoffTransport) recordCooldown(r *http.Request, until time.Time) {
	scope := backoffKeyFor(r).scope
	t.mu.Lock()
	defer t.mu.Unlock()
	for hop := r; hop != nil; {
		key := backoffKeyFor(hop)
		key.scope = scope
		if until.After(t.until[key]) {
			t.until[key] = until
		}
		if hop.Response == nil {
			break
		}
		hop = hop.Response.Request
	}
}

func backoffKeyFor(r *http.Request) backoffKey {
	port := r.URL.Port()
	if port == "" {
		if r.URL.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	scope, _ := r.Context().Value(backoffScopeKey{}).(*BackoffScope)
	return backoffKey{scope: scope, origin: strings.ToLower(r.URL.Scheme + "://" + net.JoinHostPort(r.URL.Hostname(), port))}
}

type backoffScopeKey struct{}

// BackoffScope isolates a strategy's per-origin cooldowns from other strategies.
type BackoffScope struct {
	base http.RoundTripper
}

// NewBackoffScope sends requests to base, or to http.DefaultTransport when base is nil.
func NewBackoffScope(base http.RoundTripper) *BackoffScope {
	return &BackoffScope{base: base}
}

// WithContext puts requests made with ctx in this scope, including requests
// sent by other clients through a [BackoffTransport].
func (s *BackoffScope) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, backoffScopeKey{}, s)
}

// RoundTrip sends r in this scope.
func (s *BackoffScope) RoundTrip(r *http.Request) (*http.Response, error) {
	base := s.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(r.WithContext(s.WithContext(r.Context())))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return resp, nil
}

func retryAfter(value string, now time.Time, fallback time.Duration) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 63); err == nil && seconds <= uint64(math.MaxInt64/int64(time.Second)) {
		return now.Add(time.Duration(seconds) * time.Second), true
	}
	if date, err := http.ParseTime(value); err == nil {
		return date, true
	}
	return now.Add(fallback), false
}
