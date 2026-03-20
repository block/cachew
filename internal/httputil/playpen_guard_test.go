package httputil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

func TestPlaypenGuardMiddleware(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	guard := httputil.PlaypenGuardMiddleware(ok)

	t.Run("allows normal requests", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()
		guard.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("blocks requests with cachew playpen baggage", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Baggage", "cachew-playpen=jrobotham")
		rr := httptest.NewRecorder()
		guard.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	})

	t.Run("allows requests with unrelated baggage", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Baggage", "blox-playpen=jrobotham")
		rr := httptest.NewRecorder()
		guard.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
	})
}
