package httputil_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

func TestReuseLifetime(t *testing.T) {
	const heuristic = 5 * time.Second
	tests := []struct {
		name         string
		method       string
		reqHeader    http.Header
		status       int
		header       http.Header
		wantLifetime time.Duration
		wantOK       bool
	}{
		{name: "HeuristicNotFound", status: 404, wantLifetime: heuristic, wantOK: true},
		{name: "HeuristicGone", status: 410, wantLifetime: heuristic, wantOK: true},
		{name: "ServerErrorWithoutFreshness", status: 500},
		{name: "ServerErrorWithMaxAge", status: 503, header: http.Header{"Cache-Control": {"max-age=30"}}, wantLifetime: 30 * time.Second, wantOK: true},
		{name: "SharedMaxAgeWins", status: 500, header: http.Header{"Cache-Control": {"max-age=30, s-maxage=10"}}, wantLifetime: 10 * time.Second, wantOK: true},
		{name: "PublicWithoutFreshness", status: 502, header: http.Header{"Cache-Control": {"public"}}, wantLifetime: heuristic, wantOK: true},
		{name: "NoStore", status: 404, header: http.Header{"Cache-Control": {"no-store"}}},
		{name: "NoCache", status: 404, header: http.Header{"Cache-Control": {"max-age=30, no-cache"}}},
		{name: "Private", status: 404, header: http.Header{"Cache-Control": {"private, max-age=30"}}},
		{name: "DirectivesAreCaseInsensitive", status: 404, header: http.Header{"Cache-Control": {"No-Store"}}},
		{name: "HeadIsNotShared", method: http.MethodHead, status: 404},
		{name: "AuthorizedWithoutPermission", reqHeader: http.Header{"Authorization": {"Bearer x"}}, status: 404, header: http.Header{"Cache-Control": {"max-age=30"}}},
		{name: "AuthorizedPublic", reqHeader: http.Header{"Authorization": {"Bearer x"}}, status: 404, header: http.Header{"Cache-Control": {"public, max-age=30"}}, wantLifetime: 30 * time.Second, wantOK: true},
		{name: "AgeReducesLifetime", status: 500, header: http.Header{"Cache-Control": {"max-age=30"}, "Age": {"20"}}, wantLifetime: 10 * time.Second, wantOK: true},
		{name: "AgeExhaustsLifetime", status: 500, header: http.Header{"Cache-Control": {"max-age=30"}, "Age": {"30"}}},
		{name: "InvalidMaxAge", status: 404, header: http.Header{"Cache-Control": {"max-age=soon"}}},
		{
			name:         "ExpiresRelativeToDate",
			status:       500,
			header:       http.Header{"Date": {"Mon, 09 Nov 2026 10:00:00 GMT"}, "Expires": {"Mon, 09 Nov 2026 10:00:20 GMT"}},
			wantLifetime: 20 * time.Second,
			wantOK:       true,
		},
		{name: "InvalidExpires", status: 404, header: http.Header{"Expires": {"0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := tt.method
			if method == "" {
				method = http.MethodGet
			}
			req := &http.Request{Method: method, Header: tt.reqHeader}
			if req.Header == nil {
				req.Header = http.Header{}
			}
			header := tt.header
			if header == nil {
				header = http.Header{}
			}
			lifetime, ok := httputil.ReuseLifetime(req, tt.status, header, heuristic)
			assert.Equal(t, tt.wantOK, ok)
			if ok {
				assert.Equal(t, tt.wantLifetime, lifetime)
			}
		})
	}
}

func TestRequestsNoCache(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		want   bool
	}{
		{name: "NoDirectives"},
		{name: "CacheControlNoCache", header: http.Header{"Cache-Control": {"max-age=0, No-Cache"}}, want: true},
		{name: "CacheControlOtherDirective", header: http.Header{"Cache-Control": {"max-age=60"}}},
		{name: "PragmaNoCache", header: http.Header{"Pragma": {"no-cache"}}, want: true},
		{name: "CacheControlOverridesPragma", header: http.Header{"Cache-Control": {"max-age=60"}, "Pragma": {"no-cache"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := tt.header
			if header == nil {
				header = http.Header{}
			}
			assert.Equal(t, tt.want, httputil.RequestsNoCache(&http.Request{Header: header}))
		})
	}
}

func TestVaryMatches(t *testing.T) {
	tests := []struct {
		name   string
		vary   []string
		stored http.Header
		req    http.Header
		want   bool
	}{
		{name: "NoVary", req: http.Header{"Accept": {"a"}}, want: true},
		{name: "SameValues", vary: []string{"Accept"}, stored: http.Header{"Accept": {"a"}}, req: http.Header{"Accept": {"a"}}, want: true},
		{name: "DifferentValues", vary: []string{"Accept"}, stored: http.Header{"Accept": {"a"}}, req: http.Header{"Accept": {"b"}}},
		{name: "MissingFromRequest", vary: []string{"accept"}, stored: http.Header{"Accept": {"a"}}, req: http.Header{}},
		{name: "OnlyListedFields", vary: []string{"Accept, X-Other"}, stored: http.Header{"Accept": {"a"}, "X-Unlisted": {"1"}}, req: http.Header{"Accept": {"a"}}, want: true},
		{name: "Star", vary: []string{"*"}, stored: http.Header{}, req: http.Header{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{"Vary": tt.vary}
			assert.Equal(t, tt.want, httputil.VaryMatches(header, tt.stored, tt.req))
		})
	}
}
