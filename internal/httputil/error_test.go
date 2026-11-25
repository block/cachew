package httputil_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

func TestErrorResponseRedactsURLQueries(t *testing.T) {
	var logs bytes.Buffer
	ctx := logging.ContextWithLogger(t.Context(), slog.New(slog.NewJSONHandler(&logs, nil)))
	r := httptest.NewRequest(http.MethodGet, "/object", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	httputil.ErrorResponse(w, r, http.StatusBadGateway, `failed to fetch: Get "https://origin.invalid/object?sig=synthetic": `+
		`failed to parse Location header "/next%zz?token=synthetic": parse "/next%zz?token=synthetic": invalid URL escape "%zz"`)

	assert.Equal(t, http.StatusBadGateway, w.Code)
	assert.Equal(t, `failed to fetch: Get "https://origin.invalid/object?REDACTED": `+
		`failed to parse Location header "/next%zz?REDACTED": parse "/next%zz?REDACTED": invalid URL escape "%zz"`+"\n", w.Body.String())
	assert.Contains(t, logs.String(), "https://origin.invalid/object?REDACTED")
	assert.NotContains(t, logs.String(), "synthetic")
}
