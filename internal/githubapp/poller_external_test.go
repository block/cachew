package githubapp_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
)

func TestRateLimitPollerBootstrapsFallback(t *testing.T) {
	polled := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /app/installations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[{"id":123,"account":{"login":"block"}}]`)
	})
	mux.HandleFunc("POST /app/installations/123/access_tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`)
	})
	mux.HandleFunc("GET /rate_limit", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer installation-token", r.Header.Get("Authorization"))
		_, _ = fmt.Fprint(w, `{"resources":{"core":{"remaining":5000,"limit":5000,"used":0}}}`)
		close(polled)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	manager := githubapp.NewTokenManagerForTest(t, []githubapp.Config{{}, {
		AppID: "111", PrivateKeyPath: generateTestKey(t), FallbackOrg: "block",
	}}, slog.Default(), server.URL, server.Client())
	ctx, cancel := context.WithCancel(logging.ContextWithLogger(t.Context(), slog.Default()))
	defer cancel()
	done := make(chan struct{})
	go func() {
		githubapp.NewRateLimitPoller(manager, time.Hour).Run(ctx, func(err error) { t.Error(err) })
		close(done)
	}()
	select {
	case <-polled:
	case <-time.After(3 * time.Second):
		t.Fatal("fallback was not discovered and polled before traffic")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("poller did not stop")
	}
}
