package accesslog_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/accesslog"
)

type recorder struct {
	events []accesslog.Event
}

func (r *recorder) Record(event accesslog.Event) { r.events = append(r.events, event) }

func TestMiddleware(t *testing.T) {
	rec := &recorder{}
	handler := accesslog.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hello")) //nolint:errcheck
	}), rec, accesslog.Config{Headers: map[string]string{"X-Client-Id": "client_id"}})

	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo?service=git-upload-pack", nil)
	req.Header.Set("X-Client-Id", "abc-123")
	req.Header.Set("User-Agent", "git/2.44.0")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	assert.Equal(t, 1, len(rec.events))
	event := rec.events[0]
	assert.Equal(t, http.MethodGet, event.Method)
	assert.Equal(t, "/git/github.com/org/repo", event.Path)
	assert.Equal(t, "service=git-upload-pack", event.Query)
	assert.Equal(t, http.StatusTeapot, event.Status)
	assert.Equal(t, 5, int(event.BytesSent))
	assert.Equal(t, "git/2.44.0", event.UserAgent)
	assert.Equal(t, map[string]string{"client_id": "abc-123"}, event.Headers)
	assert.False(t, event.Timestamp.IsZero())
}

func TestMiddlewareImplicitOK(t *testing.T) {
	rec := &recorder{}
	handler := accesslog.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok")) //nolint:errcheck
	}), rec, accesslog.Config{})

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	assert.Equal(t, 1, len(rec.events))
	assert.Equal(t, http.StatusOK, rec.events[0].Status)
	assert.Zero(t, rec.events[0].Headers)
}
