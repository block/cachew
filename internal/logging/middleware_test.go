package logging //nolint:testpackage

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		config     Config
		headers    map[string]string
		wantAttrs  map[string]string
		wantAbsent []string
	}{
		{
			name:   "NoHeadersConfigured",
			config: Config{},
		},
		{
			name:   "HeaderPresent",
			config: Config{Headers: map[string]string{"X-Request-ID": "request_id"}},
			headers: map[string]string{
				"X-Request-ID": "abc-123",
			},
			wantAttrs: map[string]string{"request_id": "abc-123"},
		},
		{
			name:       "HeaderMissing",
			config:     Config{Headers: map[string]string{"X-Request-ID": "request_id"}},
			wantAbsent: []string{"request_id"},
		},
		{
			name: "MixedPresentAndMissing",
			config: Config{Headers: map[string]string{
				"X-Request-ID": "request_id",
				"X-Trace-ID":   "trace_id",
			}},
			headers: map[string]string{
				"X-Request-ID": "abc-123",
			},
			wantAttrs:  map[string]string{"request_id": "abc-123"},
			wantAbsent: []string{"trace_id"},
		},
		{
			name: "MultipleHeadersPresent",
			config: Config{Headers: map[string]string{
				"X-Request-ID": "request_id",
				"X-Trace-ID":   "trace_id",
			}},
			headers: map[string]string{
				"X-Request-ID": "abc-123",
				"X-Trace-ID":   "def-456",
			},
			wantAttrs: map[string]string{
				"request_id": "abc-123",
				"trace_id":   "def-456",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == slog.TimeKey {
						return slog.Attr{}
					}
					return a
				},
			}))

			inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				FromContext(r.Context()).Info("test")
			})

			handler := Middleware(inner, tt.config)

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req = req.WithContext(ContextWithLogger(req.Context(), logger))
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			handler.ServeHTTP(httptest.NewRecorder(), req)

			var entry map[string]any
			assert.NoError(t, json.Unmarshal(buf.Bytes(), &entry))

			for attr, want := range tt.wantAttrs {
				got, ok := entry[attr].(string)
				assert.True(t, ok, "expected attribute %q to be a string", attr)
				assert.Equal(t, want, got)
			}
			for _, attr := range tt.wantAbsent {
				_, present := entry[attr]
				assert.False(t, present, "expected attribute %q to be absent", attr)
			}
		})
	}
}
