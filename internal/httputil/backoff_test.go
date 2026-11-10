package httputil_test

import (
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestBackoffTransport(t *testing.T) {
	for _, tt := range []struct {
		name       string
		retryAfter string
		delay      time.Duration
	}{
		{name: "Missing", delay: 7 * time.Second},
		{name: "Invalid", retryAfter: "later", delay: 7 * time.Second},
		{name: "Negative", retryAfter: "-3", delay: 7 * time.Second},
		{name: "Overflow", retryAfter: "9223372036854775807", delay: 7 * time.Second},
		{name: "Seconds", retryAfter: "2", delay: 2 * time.Second},
		{name: "Date", delay: 3 * time.Second},
		{name: "Zero", retryAfter: "0"},
		{name: "PastDate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				value := tt.retryAfter
				if tt.name == "Date" || tt.name == "PastDate" {
					value = time.Now().Add(tt.delay).UTC().Format(http.TimeFormat)
				}
				calls := 0
				base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {value}, "Cache-Control": {"public, max-age=3600"}},
						Body: io.NopCloser(strings.NewReader("origin limit")), Request: r}, nil
				})
				transport := httputil.NewBackoffTransport(base, 7*time.Second, time.Hour, slog.New(slog.DiscardHandler))
				client := &http.Client{Transport: transport}
				get := func(url string) *http.Response {
					req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
					assert.NoError(t, err)
					resp, err := client.Do(req)
					assert.NoError(t, err)
					assert.NoError(t, resp.Body.Close())
					return resp
				}
				initial := get("https://example.com/limited")
				wantHeader := value
				switch tt.name {
				case "Missing", "Invalid", "Negative", "Overflow":
					wantHeader = "7"
				}
				assert.Equal(t, wantHeader, initial.Header.Get("Retry-After"))
				assert.Equal(t, "no-store", initial.Header.Get("Cache-Control"))
				if tt.delay > 0 {
					resp := get("https://EXAMPLE.com:443/another-path")
					assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
					assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
					assert.Equal(t, int(tt.delay.Seconds()), mustSeconds(t, resp.Header.Get("Retry-After")))
					assert.Equal(t, 1, calls)
					get("https://other.example/ok")
					get("http://example.com/ok")
					get("https://example.com:444/ok")
					assert.Equal(t, 2, calls)
					time.Sleep(tt.delay - time.Nanosecond)
					assert.Equal(t, "1", get("https://example.com/near-expiry").Header.Get("Retry-After"))
					assert.Equal(t, 2, calls)
					time.Sleep(time.Nanosecond)
				}
				before := calls
				get("https://example.com/after-expiry")
				assert.Equal(t, before+1, calls)
			})
		})
	}
}

func mustSeconds(t *testing.T, value string) int {
	t.Helper()
	seconds, err := strconv.Atoi(value)
	assert.NoError(t, err)
	return seconds
}

func TestBackoffRedirectDestination(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(map[string]int)
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls[r.URL.Host]++
			resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header), Body: http.NoBody, Request: r}
			if r.URL.Host != "limited.example" {
				resp.StatusCode = http.StatusFound
				resp.Header.Set("Location", "https://limited.example/file")
			}
			return resp, nil
		})
		transport := httputil.NewBackoffTransport(base, time.Minute, time.Hour, slog.New(slog.DiscardHandler))
		client := &http.Client{Transport: transport}
		for _, host := range []string{"redirect.example", "redirect.example", "another.example", "another.example"} {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+host+"/file", nil)
			assert.NoError(t, err)
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			assert.Equal(t, "60", resp.Header.Get("Retry-After"))
		}
		assert.Equal(t, map[string]int{"redirect.example": 2, "another.example": 2, "limited.example": 1}, calls)
		time.Sleep(time.Minute)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://redirect.example/file", nil)
		assert.NoError(t, err)
		resp, err := client.Do(req)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
		assert.Equal(t, map[string]int{"redirect.example": 3, "another.example": 2, "limited.example": 2}, calls)
	})
}

func TestBackoffSharedClients(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := map[string]int{}
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls[r.URL.Host]++
			status := http.StatusOK
			if r.URL.Path == "/limited" {
				status = http.StatusTooManyRequests
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
		})
		transport := httputil.NewBackoffTransport(base, time.Minute, time.Hour, slog.New(slog.DiscardHandler))
		first := &http.Client{Transport: transport}
		second := &http.Client{Transport: transport}
		for _, tt := range []struct {
			client *http.Client
			url    string
			status int
		}{
			{first, "https://primary.example/limited", http.StatusTooManyRequests},
			{first, "https://PRIMARY.example:443/other", http.StatusTooManyRequests},
			{first, "https://mirror.example/other", http.StatusOK},
			{second, "https://primary.example/other", http.StatusTooManyRequests},
			{second, "http://primary.example:8080/other", http.StatusTooManyRequests},
			{second, "https://mirror.example/limited", http.StatusTooManyRequests},
			{first, "https://mirror.example/another", http.StatusTooManyRequests},
		} {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tt.url, nil)
			assert.NoError(t, err)
			resp, err := tt.client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.status, resp.StatusCode)
		}
		assert.Equal(t, map[string]int{"primary.example": 1, "mirror.example": 2}, calls)
	})
}

func TestBackoffCooldownScope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := map[string]int{}
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			scope, _ := httputil.CooldownScope(r.Context())
			calls[scope]++
			status := http.StatusOK
			if r.URL.Path == "/limited" {
				status = http.StatusTooManyRequests
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
		})
		client := &http.Client{Transport: httputil.NewBackoffTransport(base, time.Minute, time.Hour, slog.New(slog.DiscardHandler))}
		for _, tt := range []struct {
			path, scope string
			status      int
		}{
			{"/limited", "installation-a", http.StatusTooManyRequests},
			{"/other", "installation-a", http.StatusTooManyRequests},
			{"/other", "installation-b", http.StatusOK},
			{"/other", "", http.StatusOK},
			{"/limited", "", http.StatusTooManyRequests},
			{"/other", "", http.StatusTooManyRequests},
			{"/other", "installation-b", http.StatusOK},
		} {
			ctx := t.Context()
			if tt.scope != "" {
				ctx = httputil.WithCooldownScope(ctx, tt.scope)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example"+tt.path, nil)
			assert.NoError(t, err)
			req.Header.Set("Authorization", "Bearer rotating-"+strconv.Itoa(len(calls)))
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.status, resp.StatusCode, "%s %s", tt.scope, tt.path)
		}
		assert.Equal(t, map[string]int{"installation-a": 1, "installation-b": 2, "": 2}, calls)
	})
}

func TestBackoffScopeDroppedOnCrossHostRedirect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cdnCalls := 0
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "api.example" {
				return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://cdn.example/asset"}},
					Body: http.NoBody, Request: r}, nil
			}
			cdnCalls++
			return &http.Response{StatusCode: http.StatusTooManyRequests, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
		})
		client := &http.Client{Transport: httputil.NewBackoffTransport(base, time.Minute, time.Hour, slog.New(slog.DiscardHandler))}
		for _, scope := range []string{"installation-a", "installation-b"} {
			req, err := http.NewRequestWithContext(httputil.WithCooldownScope(t.Context(), scope), http.MethodGet, "https://api.example/asset", nil)
			assert.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+scope)
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode, scope)
		}
		assert.Equal(t, 1, cdnCalls, "the anonymous redirect hop shares the CDN host's cooldown")
	})
}

func TestBackoffMaxDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"86400"}}, Body: http.NoBody, Request: r}, nil
		})
		client := &http.Client{Transport: httputil.NewBackoffTransport(base, time.Minute, 10*time.Minute, slog.New(slog.DiscardHandler))}
		get := func() *http.Response {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/file", nil)
			assert.NoError(t, err)
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			return resp
		}
		assert.Equal(t, "600", get().Header.Get("Retry-After"))
		assert.Equal(t, "600", get().Header.Get("Retry-After"))
		assert.Equal(t, 1, calls)
		time.Sleep(10 * time.Minute)
		get()
		assert.Equal(t, 2, calls)
	})
}

func TestBackoffWithoutCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			status := http.StatusOK
			if r.URL.Path == "/limited" {
				status = http.StatusTooManyRequests
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: http.NoBody, Request: r}, nil
		})
		client := &http.Client{Transport: httputil.NewBackoffTransport(base, time.Minute, time.Hour, slog.New(slog.DiscardHandler))}
		for _, tt := range []struct {
			path   string
			exempt bool
			status int
		}{
			{"/limited", true, http.StatusTooManyRequests},
			{"/other", false, http.StatusOK},
			{"/limited", false, http.StatusTooManyRequests},
			{"/other", true, http.StatusOK},
			{"/other", false, http.StatusTooManyRequests},
		} {
			ctx := t.Context()
			if tt.exempt {
				ctx = httputil.WithoutCooldown(ctx)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:8080"+tt.path, nil)
			assert.NoError(t, err)
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.status, resp.StatusCode, "%s exempt=%v", tt.path, tt.exempt)
		}
		assert.Equal(t, 4, calls)
	})
}
