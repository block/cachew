package httputil_test

import (
	"net/http"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

type staticTransport struct{ status int }

func (s staticTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: s.status, Body: http.NoBody}, nil
}

func TestUpstreamTransportFollowsDefaultUntilSet(t *testing.T) {
	defaultTransport := http.DefaultTransport
	t.Cleanup(func() {
		http.DefaultTransport = defaultTransport //nolint:reassign
		httputil.SetUpstreamTransport(nil)
	})
	httputil.SetUpstreamTransport(nil)
	http.DefaultTransport = staticTransport{status: http.StatusTeapot} //nolint:reassign
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example.com/", nil)
	assert.NoError(t, err)

	resp, err := httputil.UpstreamClient().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)

	httputil.SetUpstreamTransport(staticTransport{status: http.StatusAccepted})
	resp, err = httputil.UpstreamClient().Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	assert.Equal[http.RoundTripper](t, staticTransport{status: http.StatusTeapot}, http.DefaultTransport)
}
