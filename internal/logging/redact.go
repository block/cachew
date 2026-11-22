package logging

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"
)

const redactedQuery = "REDACTED"

// RedactURI replaces the query of a URL or request target, such as [net/http.Request.RequestURI].
//
// Queries can carry credentials, such as the signature of a presigned URL.
func RedactURI(uri string) string {
	path, _, found := strings.Cut(uri, "?")
	if !found {
		return uri
	}
	return path + "?" + redactedQuery
}

// RedactText replaces the query of every URL in text.
//
// A URL is a double-quoted string or a word with a "/" before its "?", where quotes after the "/" are part of the word.
// A quoted query runs to the closing quote, since net/http quotes URLs with %q, and an unquoted one to whitespace.
func RedactText(text string) string {
	if !strings.Contains(text, "?") {
		return text
	}
	var out strings.Builder
	written := 0
	quoted, slash := false, false
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case c == '"' && (quoted || !slash):
			quoted, slash = !quoted, false
		case c == '\\' && quoted:
			i++
		case isSpace(c) && !quoted:
			slash = false
		case c == '/':
			slash = true
		case c == '?' && (quoted || slash):
			end := queryEnd(text, i+1, quoted)
			if end > i+1 {
				out.WriteString(text[written : i+1])
				out.WriteString(redactedQuery)
				written = end
			}
			i = end - 1
		}
	}
	out.WriteString(text[written:])
	return out.String()
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// queryEnd returns the index of the closing quote or, for an unquoted query, the whitespace that ends it.
//
// Quotes inside an unquoted query are part of it, because [net/url.URL.String] doesn't escape them in queries.
func queryEnd(text string, start int, quoted bool) int { //nolint:privatedoc // explains why
	end := start
	for end < len(text) {
		c := text[end]
		if quoted && c == '\\' {
			end += 2
			continue
		}
		if (quoted && c == '"') || (!quoted && isSpace(c)) {
			break
		}
		end++
	}
	return min(end, len(text))
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	if u.RawQuery != "" {
		redacted := *u
		redacted.RawQuery = redactedQuery
		u = &redacted
	}
	return u.Redacted()
}

type redactingHandler struct {
	inner slog.Handler
}

func (h *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *redactingHandler) Handle(ctx context.Context, r slog.Record) error {
	redacted := slog.NewRecord(r.Time, r.Level, RedactText(r.Message), r.PC)
	r.Attrs(func(a slog.Attr) bool {
		redacted.AddAttrs(redactAttr(a))
		return true
	})
	return errors.Wrap(h.inner.Handle(ctx, redacted), "handle log record")
}

func (h *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	redacted := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		redacted[i] = redactAttr(a)
	}
	return &redactingHandler{inner: h.inner.WithAttrs(redacted)}
}

func (h *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{inner: h.inner.WithGroup(name)}
}

func redactAttr(a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindString {
		a.Value = slog.StringValue(RedactText(a.Value.String()))
		return a
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		redacted := make([]slog.Attr, len(group))
		for i, member := range group {
			redacted[i] = redactAttr(member)
		}
		a.Value = slog.GroupValue(redacted...)
		return a
	}
	if a.Value.Kind() != slog.KindAny {
		return a
	}
	switch v := a.Value.Any().(type) {
	case *url.URL:
		a.Value = slog.StringValue(redactURL(v))
	case url.URL:
		a.Value = slog.StringValue(redactURL(&v))
	case error:
		if text, ok := errorText(v); ok {
			a.Value = slog.AnyValue(redactedError{err: v, text: RedactText(text)})
		}
	}
	return a
}

// redactedError formats like the error it redacts, so a handler printing "%+v" still gets the error's trace.
type redactedError struct {
	err  error
	text string
}

func (e redactedError) Error() string { return e.text }

// Format implements [fmt.Formatter].
func (e redactedError) Format(s fmt.State, verb rune) {
	switch {
	case verb == 'v' && s.Flag('+'):
		fmt.Fprint(s, RedactText(fmt.Sprintf("%+v", e.err)))
	case verb == 'q':
		fmt.Fprintf(s, "%q", e.text)
	default:
		fmt.Fprint(s, e.text)
	}
}

// errorText returns err.Error(), or false if it panics.
//
// Error panics on typed nil pointers, which the inner handler then formats as "<nil>" like it did before.
func errorText(err error) (text string, ok bool) { //nolint:privatedoc // explains why
	defer func() {
		if recover() != nil {
			text, ok = "", false
		}
	}()
	return err.Error(), true
}
