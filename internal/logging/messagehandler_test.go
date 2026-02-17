package logging //nolint:testpackage

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestMessageHandler(t *testing.T) {
	type logEntry struct {
		Level   string `json:"level"`
		Msg     string `json:"msg"`
		Err     string `json:"err,omitempty"`
		ID      int    `json:"id,omitempty"`
		Request string `json:"request,omitempty"`
	}

	tests := []struct {
		name    string
		msg     string
		attrs   []slog.Attr
		wantMsg string
	}{
		{
			name:    "NoAttrs",
			msg:     "simple message",
			wantMsg: "simple message",
		},
		{
			name:    "SingleAttr",
			msg:     "failed",
			attrs:   []slog.Attr{slog.String("err", "timeout")},
			wantMsg: "failed (err=timeout)",
		},
		{
			name: "MultipleAttrs",
			msg:  "request handled",
			attrs: []slog.Attr{
				slog.String("request", "/foo"),
				slog.Int("id", 42),
			},
			wantMsg: "request handled (request=/foo, id=42)",
		},
		{
			name:    "QuotedStringWithSpaces",
			msg:     "failed",
			attrs:   []slog.Attr{slog.String("err", "connection refused, try again")},
			wantMsg: `failed (err="connection refused, try again")`,
		},
		{
			name:    "EmptyString",
			msg:     "failed",
			attrs:   []slog.Attr{slog.String("reason", "")},
			wantMsg: `failed (reason="")`,
		},
		{
			name:    "SimpleWordUnquoted",
			msg:     "done",
			attrs:   []slog.Attr{slog.String("status", "ok")},
			wantMsg: "done (status=ok)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
				ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == slog.TimeKey {
						return slog.Attr{}
					}
					return a
				},
			})
			handler := &messageHandler{inner: inner}
			logger := slog.New(handler)

			args := make([]any, 0, len(tt.attrs)*2)
			for _, a := range tt.attrs {
				args = append(args, a.Key, a.Value)
			}
			logger.Info(tt.msg, args...)

			var entry logEntry
			assert.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
			assert.Equal(t, tt.wantMsg, entry.Msg)
		})
	}
}

func TestMessageHandlerWithContextAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})
	handler := &messageHandler{inner: inner}
	logger := slog.New(handler).With("client", "10.0.0.1")
	logger.InfoContext(context.Background(), "connected", "id", 7)

	var entry map[string]any
	assert.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
	// Only record-level attrs appear in the message suffix; context attrs do not.
	assert.Equal(t, "connected (id=7)", entry["msg"])
	assert.Equal(t, "10.0.0.1", entry["client"])
	assert.Equal(t, float64(7), entry["id"].(float64))
}
