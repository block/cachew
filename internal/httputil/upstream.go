package httputil

import (
	"net/http"
	"sync/atomic"
)

// upstream is the transport cachewd installs for outbound requests that would
// otherwise use http.DefaultTransport. It is kept here instead of being
// assigned to http.DefaultTransport because third-party code, such as OPA's
// http.send, asserts that the global is a *http.Transport and panics on any
// wrapper.
var upstream atomic.Pointer[http.RoundTripper] //nolint:gochecknoglobals

// SetUpstreamTransport installs the shared outbound transport returned by
// [UpstreamTransport], leaving http.DefaultTransport unchanged. A nil
// transport restores the default of following http.DefaultTransport.
func SetUpstreamTransport(rt http.RoundTripper) {
	if rt == nil {
		upstream.Store(nil)
		return
	}
	upstream.Store(&rt)
}

// UpstreamTransport returns the shared outbound transport. Until one is
// installed, requests go to whatever http.DefaultTransport is at request time,
// so tests can still swap the global to intercept upstream traffic.
func UpstreamTransport() http.RoundTripper {
	return upstreamTransport{}
}

// UpstreamClient returns a client that sends through [UpstreamTransport].
func UpstreamClient() *http.Client {
	return &http.Client{Transport: UpstreamTransport()}
}

type upstreamTransport struct{}

func (upstreamTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt := upstream.Load(); rt != nil {
		return (*rt).RoundTrip(req) //nolint:wrapcheck
	}
	return http.DefaultTransport.RoundTrip(req) //nolint:wrapcheck
}
