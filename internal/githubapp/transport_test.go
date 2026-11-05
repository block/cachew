package githubapp //nolint:testpackage // Reuse the metric reader and inspect emitted instruments.

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel/attribute"

	"github.com/block/cachew/internal/logging"
)

func TestAPIClient(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining string
		limit     string
		warn      bool
	}{
		{"Healthy", "4500", "5000", false},
		{"Low", "499", "5000", true},
		{"Boundary", "500", "5000", false},
		{"Exhausted", "0", "5000", true},
		{"Missing", "", "", false},
		{"MalformedRemaining", "broken", "5000", false},
		{"MalformedLimit", "0", "broken", false},
		{"ZeroLimit", "0", "0", false},
		{"Negative", "-1", "5000", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := newMetricReader(t)
			var logs bytes.Buffer
			ctx := logging.ContextWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Ratelimit-Remaining", tc.remaining)
				w.Header().Set("X-Ratelimit-Limit", tc.limit)
				_, _ = fmt.Fprint(w, "unchanged body")
			}))
			defer server.Close()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			assert.NoError(t, err)
			resp, err := NewAPIClient(server.Client()).Do(WithAPIMetrics(req, "fallback-org", "get_release"))
			assert.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, "unchanged body", string(body))
			assert.Equal(t, tc.warn, logs.Len() > 0)
			if tc.warn {
				assert.Contains(t, logs.String(), "GitHub API rate limit low")
			}
			points := collectMetrics(t, reader)["github.api_calls"]
			assert.Equal(t, 1, len(points))
			assert.Equal(t, int64(1), points[0].Value)
			assert.Equal(t, attribute.NewSet(attribute.String("owner", "fallback-org"),
				attribute.String("operation", "get_release")), points[0].Attributes)
			assert.Zero(t, len(collectMetrics(t, reader)["github.ratelimit_remaining"]))
		})
	}
}

func TestAPIClientSkipsUnmarkedRequestsAndRedirectedStorage(t *testing.T) {
	reader := newMetricReader(t)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "asset")
	}))
	defer storage.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, storage.URL, http.StatusFound)
	}))
	defer api.Close()
	client := NewAPIClient(api.Client())
	for _, marked := range []bool{false, true} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, api.URL, nil)
		assert.NoError(t, err)
		if marked {
			req = WithAPIMetrics(req, "squareup", "download_release_asset")
		}
		resp, err := client.Do(req)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
	}
	points := collectMetrics(t, reader)["github.api_calls"]
	assert.Equal(t, 1, len(points))
	assert.Equal(t, int64(1), points[0].Value)
}
