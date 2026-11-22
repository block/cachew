package logging //nolint:testpackage

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
)

const signedQuery = "X-Amz-Credential=synthetic-credential&X-Amz-Signature=synthetic-signature"

func TestRedactText(t *testing.T) {
	tests := []struct {
		name, text, want string
	}{
		{name: "NoURL", text: "is the cache warm?", want: "is the cache warm?"},
		{name: "QuestionMarkInWord", text: "ignored (allowFilter unset?)", want: "ignored (allowFilter unset?)"},
		{name: "EmptyQuery", text: "GET /a? failed", want: "GET /a? failed"},
		{name: "PathOnly", text: "fetch /bucket/object?" + signedQuery + ": EOF", want: "fetch /bucket/object?REDACTED EOF"},
		{name: "OtherScheme", text: "s3://bucket/key?versionId=synthetic", want: "s3://bucket/key?REDACTED"},
		{name: "QuotedRelative", text: `parse "key%zz?token=synthetic": bad`, want: `parse "key%zz?REDACTED": bad`},
		{name: "QuotedQueryWithSpace", text: `parse "/a?name=x y&token=synthetic": bad`, want: `parse "/a?REDACTED": bad`},
		{name: "QuotedPathWithSpace", text: `parse "/a b?token=synthetic": bad`, want: `parse "/a b?REDACTED": bad`},
		{name: "UnclosedQuote", text: `parse "/a?name=x y&token=synthetic`, want: `parse "/a?REDACTED`},
		{
			name: "QuoteInUnquotedQuery",
			text: `http://one.invalid/a?name="x"&sig=synthetic then Get "https://two.invalid/b?sig=synthetic": EOF`,
			want: `http://one.invalid/a?REDACTED then Get "https://two.invalid/b?REDACTED": EOF`,
		},
		{
			name: "QuoteInUnquotedPath",
			text: "redirect: http://one.invalid/a\"x\"?token=synthetic\n" + `Get "https://two.invalid/b?sig=synthetic": EOF`,
			want: "redirect: http://one.invalid/a\"x\"?REDACTED\n" + `Get "https://two.invalid/b?REDACTED": EOF`,
		},
		{name: "NoQuery", text: "GET https://example.invalid/a failed", want: "GET https://example.invalid/a failed"},
		{name: "Query", text: "https://example.invalid/a?" + signedQuery, want: "https://example.invalid/a?REDACTED"},
		{name: "UpperCaseScheme", text: "HTTP://example.invalid?" + signedQuery, want: "HTTP://example.invalid?REDACTED"},
		{name: "QuestionMarkInFragment", text: "https://example.invalid/a#b?c", want: "https://example.invalid/a#b?REDACTED"},
		{
			name: "SeveralURLs",
			text: "https://one.invalid/a?" + signedQuery + " then http://two.invalid/ and https://three.invalid/c?" + signedQuery,
			want: "https://one.invalid/a?REDACTED then http://two.invalid/ and https://three.invalid/c?REDACTED",
		},
		{
			name: "NestedClientErrors",
			text: errors.Wrap(&url.Error{Op: "Get", URL: "https://one.invalid/a?" + signedQuery,
				Err: &url.Error{Op: "Get", URL: "https://two.invalid/b?" + signedQuery, Err: http.ErrHandlerTimeout}}, "failed to fetch").Error(),
			want: `failed to fetch: Get "https://one.invalid/a?REDACTED": Get "https://two.invalid/b?REDACTED": ` + http.ErrHandlerTimeout.Error(),
		},
		{
			name: "EscapedQuoteInQuery",
			text: (&url.Error{Op: "Get", URL: `https://example.invalid/a?sig="synthetic-signature"&next=1`, Err: http.ErrHandlerTimeout}).Error(),
			want: `Get "https://example.invalid/a?REDACTED": ` + http.ErrHandlerTimeout.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RedactText(tt.text))
		})
	}
}

func TestRedactTextClientErrors(t *testing.T) {
	for _, tt := range []struct {
		name, location string
	}{
		{name: "InvalidAbsoluteLocation", location: "/invalid%zz?token=synthetic-token"},
		{name: "InvalidRelativeLocation", location: "invalid%zz?token=synthetic-token"},
		{name: "ApostrophesInURL", location: "/O'Reilly/object?a=synthetic-a'b&c=synthetic-c"},
		{name: "SpacesInInvalidLocation", location: "/invalid%zz x?name=x y&token=synthetic-token"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/" {
					w.Header()["Location"] = []string{tt.location}
					w.WriteHeader(http.StatusFound)
					return
				}
				conn, _, err := w.(http.Hijacker).Hijack()
				assert.NoError(t, err)
				assert.NoError(t, conn.Close())
			}))
			defer origin.Close()

			_, err := origin.Client().Get(origin.URL + "/?" + signedQuery)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "synthetic")
			assert.NotContains(t, RedactText(err.Error()), "synthetic")
		})
	}
}

func TestRedactTextIsLinear(t *testing.T) {
	for _, text := range []string{
		strings.Repeat("?", 1<<20),
		strings.Repeat("/?", 1<<19),
		strings.Repeat(`"?`, 1<<19),
		"/" + strings.Repeat("a?", 1<<19),
	} {
		start := time.Now()
		RedactText(text)
		assert.True(t, time.Since(start) < 5*time.Second, "RedactText took %s on %.10q", time.Since(start), text)
	}
}

func TestRedactURI(t *testing.T) {
	assert.Equal(t, "/bucket/object", RedactURI("/bucket/object"))
	assert.Equal(t, "/bucket/object?REDACTED", RedactURI("/bucket/object?"+signedQuery))
	assert.Equal(t, "https://example.invalid/object?REDACTED", RedactURI("https://example.invalid/object?"+signedQuery))
}

type urlValuer struct{ u *url.URL }

func (v urlValuer) LogValue() slog.Value { return slog.AnyValue(v.u) }

func TestRedactingHandler(t *testing.T) {
	signed, err := url.Parse("https://example.invalid/object?" + signedQuery)
	assert.NoError(t, err)
	clientErr := errors.Wrap(&url.Error{Op: "Get", URL: signed.String(), Err: http.ErrHandlerTimeout}, "failed to fetch")
	const redacted = "https://example.invalid/object?REDACTED"

	for _, tt := range []struct {
		name  string
		inner func(io.Writer) slog.Handler
		trace bool
	}{
		{name: "Text", inner: func(w io.Writer) slog.Handler { return slog.NewTextHandler(w, nil) }, trace: true},
		{name: "JSON", inner: func(w io.Writer) slog.Handler { return &messageHandler{inner: slog.NewJSONHandler(w, nil)} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(&redactingHandler{inner: tt.inner(&logs)}).With("bound", signed).WithGroup("request")
			logger.Error("Fetch of "+signed.String()+" failed", "error", clientErr, "pointer", signed, "value", *signed,
				"text", signed.String(), "valuer", urlValuer{signed},
				slog.Group("nested", "error", clientErr, "status", http.StatusBadGateway), "nil", (*url.Error)(nil))

			out := logs.String()
			assert.NotContains(t, out, "synthetic")
			assert.True(t, strings.Count(out, redacted) >= 8, out)
			assert.Contains(t, out, "502")
			assert.Contains(t, out, http.ErrHandlerTimeout.Error())
			assert.Contains(t, out, "<nil>")
			assert.Equal(t, tt.trace, strings.Contains(out, "redact_test.go:"), out)
		})
	}
}

func TestConfigureRedactsURLQueries(t *testing.T) {
	for _, json := range []bool{false, true} {
		logger, _ := Configure(context.Background(), Config{JSON: json, Level: slog.LevelError})
		_, ok := logger.Handler().(*redactingHandler)
		assert.True(t, ok, "json=%v", json)
	}
}
