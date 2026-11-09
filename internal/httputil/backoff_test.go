package httputil_test

import (
	"io"
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
				transport := httputil.NewBackoffTransport(base, 7*time.Second)
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
					assert.Equal(t, 4, calls)
					time.Sleep(tt.delay - time.Nanosecond)
					assert.Equal(t, "1", get("https://example.com/near-expiry").Header.Get("Retry-After"))
					assert.Equal(t, 4, calls)
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
			if r.URL.Host == "redirect.example" {
				resp.StatusCode = http.StatusFound
				resp.Header.Set("Location", "https://limited.example/file")
			}
			return resp, nil
		})
		transport := httputil.NewBackoffTransport(base, time.Minute)
		for range 2 {
			client := &http.Client{Transport: transport}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://redirect.example/file", nil)
			assert.NoError(t, err)
			resp, err := client.Do(req)
			assert.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
			assert.Equal(t, "60", resp.Header.Get("Retry-After"))
		}
		assert.Equal(t, 2, calls["redirect.example"])
		assert.Equal(t, 1, calls["limited.example"])
	})
}
