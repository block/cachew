package logging

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/alecthomas/errors"
)

// messageHandler wraps a slog.Handler and appends record attributes to the
// message text for easier debugging (e.g. "My message (err=..., id=...)").
type messageHandler struct {
	inner slog.Handler
}

func (h *messageHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *messageHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.NumAttrs() > 0 {
		var b strings.Builder
		first := true
		r.Attrs(func(a slog.Attr) bool {
			if first {
				first = false
			} else {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s=%s", a.Key, formatValue(a.Value))
			return true
		})
		r.Message = r.Message + " (" + b.String() + ")"
	}
	return errors.Wrap(h.inner.Handle(ctx, r), "handle log record")
}

func needsQuoting(s string) bool {
	for _, c := range s {
		if c <= ' ' || c == '"' || c == ',' || c == '=' || c == '(' || c == ')' {
			return true
		}
	}
	return false
}

func formatValue(v slog.Value) string {
	v = v.Resolve()
	if v.Kind() == slog.KindString {
		s := v.String()
		if s == "" || needsQuoting(s) {
			return fmt.Sprintf("%q", s)
		}
		return s
	}
	return v.String()
}

func (h *messageHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &messageHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *messageHandler) WithGroup(name string) slog.Handler {
	return &messageHandler{inner: h.inner.WithGroup(name)}
}
