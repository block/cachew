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
