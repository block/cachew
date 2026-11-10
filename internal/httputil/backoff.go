package httputil

import (
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

// BackoffTransport shares upstream cooldowns across requests and redirect hops.
// Cooldowns are keyed by outbound hostname, across strategies, schemes and ports.
type BackoffTransport struct {
	base  http.RoundTripper
	delay time.Duration
	mu    sync.Mutex
	until map[string]time.Time
}

// NewBackoffTransport uses delay when a 429 has no valid Retry-After header.
func NewBackoffTransport(base http.RoundTripper, delay time.Duration) *BackoffTransport {
	return &BackoffTransport{base: base, delay: delay, until: map[string]time.Time{}}
}

// RoundTrip rejects requests to a cooling hostname without retrying them.
func (t *BackoffTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	cooldown := strings.ToLower(r.URL.Hostname())
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
		t.mu.Lock()
		if until.After(t.until[cooldown]) {
			t.until[cooldown] = until
		}
		t.mu.Unlock()
		// The cooldown ends the 429; a downstream cache must not keep it longer.
		resp.Header.Set("Cache-Control", "no-store")
		if !valid {
			resp.Header.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(until.Sub(now).Seconds())), 10))
		}
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
