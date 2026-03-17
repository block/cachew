package httputil_test

import (
	"net/http"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

func TestFilterHeaders(t *testing.T) {
	tests := []struct {
		name     string
		headers  http.Header
		skip     []string
		expected http.Header
	}{
		{
			name:     "Empty",
			headers:  http.Header{},
			skip:     httputil.TransportHeaders,
			expected: http.Header{},
		},
		{
			name: "TransportHeaders",
			headers: http.Header{
				"Content-Type":      {"application/json"},
				"Content-Length":    {"42"},
				"Date":              {"Mon, 01 Jan 2024 00:00:00 GMT"},
				"Transfer-Encoding": {"chunked"},
				"X-Custom":          {"value"},
			},
			skip: httputil.TransportHeaders,
			expected: http.Header{
				"Content-Type": {"application/json"},
				"X-Custom":     {"value"},
			},
		},
		{
			name: "HopByHopHeaders",
			headers: http.Header{
				"Accept":        {"text/html"},
				"Authorization": {"Bearer token"},
				"Connection":    {"keep-alive"},
				"Keep-Alive":    {"timeout=5"},
				"Host":          {"example.com"},
				"Upgrade":       {"websocket"},
			},
			skip: httputil.HopByHopHeaders,
			expected: http.Header{
				"Accept":        {"text/html"},
				"Authorization": {"Bearer token"},
			},
		},
		{
			name: "CaseInsensitive",
			headers: http.Header{
				"content-length": {"42"},
				"X-Custom":       {"value"},
			},
			skip: []string{"Content-Length"},
			expected: http.Header{
				"X-Custom": {"value"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := httputil.FilterHeaders(tt.headers, tt.skip...)
			assert.Equal(t, tt.expected, result)
		})
	}
}
