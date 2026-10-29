package httputil_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/httputil"
)

func TestValidatePath(t *testing.T) {
	for _, path := range []string{"", "/", "a/b/c", "/a/b.c/..d/e..", "a//b"} {
		assert.NoError(t, httputil.ValidatePath(path), path)
	}
	for _, path := range []string{"..", ".", "a/../b", "/a/./b", "a/..", "../a", `a\..\b`, "a\x00b"} {
		assert.Error(t, httputil.ValidatePath(path), path)
	}
}

func TestValidatePathSegment(t *testing.T) {
	assert.NoError(t, httputil.ValidatePathSegment("github.com"))
	for _, segment := range []string{"", ".", "..", "a/b", `a\b`} {
		assert.Error(t, httputil.ValidatePathSegment(segment), segment)
	}
}

func TestRejectTraversal(t *testing.T) {
	handler := httputil.RejectTraversal(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	tests := map[string]int{
		"/github.com/org/repo/releases/download/v1/file":          http.StatusOK,
		"/github.com/org/repo/releases/download/..%2F..%2Fx/file": http.StatusBadRequest,
		"/github.com/org/%2E%2E/releases/download/v1/file":        http.StatusBadRequest,
		"/hermit/a.com/a%5C..%5Cb":                                http.StatusBadRequest,
	}
	for target, expected := range tests {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
		assert.Equal(t, expected, w.Code, target)
	}
}
