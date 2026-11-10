package httputil

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// BackoffTransport shares upstream cooldowns across requests and redirect hops.
// Cooldowns are keyed by outbound hostname, across strategies, schemes and
// ports, and by the scope set with [WithCooldownScope].
type BackoffTransport struct {
	base       http.RoundTripper
	delay      time.Duration
	maxDelay   time.Duration
	logger     *slog.Logger
	cooldowns  metric.Int64Counter
	rejections metric.Int64Counter
	mu         sync.Mutex
	until      map[string]time.Time
}

// NewBackoffTransport uses delay when a 429 has no valid Retry-After header,
// never cools down for longer than maxDelay, and reports cooldowns to logger,
// because outbound requests do not always carry a logger in their context.
func NewBackoffTransport(base http.RoundTripper, delay, maxDelay time.Duration, logger *slog.Logger) *BackoffTransport {
	meter := otel.Meter("cachew.upstream")
	cooldowns, err := meter.Int64Counter("cachew.upstream.cooldowns", metric.WithUnit("{cooldown}"),
		metric.WithDescription("Upstream 429 responses that started or extended a cooldown"))
	if err != nil {
		panic(err)
	}
	rejections, err := meter.Int64Counter("cachew.upstream.cooldown_rejections", metric.WithUnit("{request}"),
		metric.WithDescription("Outbound requests answered with a local 429 during a cooldown"))
	if err != nil {
		panic(err)
	}
	return &BackoffTransport{base: base, delay: delay, maxDelay: maxDelay, logger: logger,
		cooldowns: cooldowns, rejections: rejections, until: map[string]time.Time{}}
}

type noCooldownKey struct{}
type cooldownScopeKey struct{}

// WithCooldownScope gives requests their own cooldown on each host. Set it
// where a request gets a credential whose rate limit is separate from the
// host's other callers, such as a GitHub App installation token. Requests
// without a scope share their host's cooldown.
func WithCooldownScope(ctx context.Context, scope string) context.Context {
	return context.WithValue(ctx, cooldownScopeKey{}, scope)
}

// CooldownScope returns the scope set by [WithCooldownScope], if any.
func CooldownScope(ctx context.Context) (string, bool) {
	scope, ok := ctx.Value(cooldownScopeKey{}).(string)
	return scope, ok
}

// WithoutCooldown exempts requests that cachew forwards to one of its own
// routes. That route applies cooldowns to the real upstream, and a 429 from it
// must not cool down every other route behind the same address.
func WithoutCooldown(ctx context.Context) context.Context {
	return context.WithValue(ctx, noCooldownKey{}, true)
}

// RoundTrip rejects requests to a cooling hostname without retrying them.
func (t *BackoffTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if exempt, _ := r.Context().Value(noCooldownKey{}).(bool); exempt {
		resp, err := t.base.RoundTrip(r)
		return resp, errors.WithStack(err)
	}
	cooldown := strings.ToLower(r.URL.Hostname())
	scope, scoped := CooldownScope(r.Context())
	if scoped {
		cooldown += "#" + scope
	}
	attrs := metric.WithAttributes(attribute.Bool("scoped", scoped))
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
		t.rejections.Add(r.Context(), 1, attrs)
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
		until, valid = RetryAfterDeadline(resp.Header.Get("Retry-After"), now, t.delay)
		capped := until.After(now.Add(t.maxDelay))
		if capped {
			until = now.Add(t.maxDelay)
		}
		t.mu.Lock()
		previous := t.until[cooldown]
		if until.After(previous) {
			t.until[cooldown] = until
		}
		t.mu.Unlock()
		if until.After(previous) {
			event := "extended"
			if !previous.After(now) {
				event = "started"
			}
			t.cooldowns.Add(r.Context(), 1, metric.WithAttributes(attribute.Bool("scoped", scoped), attribute.String("event", event)))
			t.logger.WarnContext(r.Context(), "Upstream rate limited, cooling down", "host", r.URL.Hostname(), "scope", scope,
				"event", event, "until", until, "capped", capped)
		}
		// The cooldown ends the 429; a downstream cache must not keep it longer.
		resp.Header.Set("Cache-Control", "no-store")
		if !valid || capped {
			resp.Header.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(until.Sub(now).Seconds())), 10))
		}
	}
	return resp, nil
}

// RetryAfterDeadline parses Retry-After, using fallback when the value is invalid.
// The boolean reports whether the header was valid.
func RetryAfterDeadline(value string, now time.Time, fallback time.Duration) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseUint(value, 10, 63); err == nil && seconds <= uint64(math.MaxInt64/int64(time.Second)) {
		return now.Add(time.Duration(seconds) * time.Second), true
	}
	if date, err := http.ParseTime(value); err == nil {
		return date, true
	}
	return now.Add(fallback), false
}
