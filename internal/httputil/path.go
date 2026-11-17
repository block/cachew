package httputil

import (
	"net/http"
	"strings"

	"github.com/alecthomas/errors"
)

// ValidatePath returns an error if path contains "." or ".." segments, backslashes, or NUL bytes.
//
// ServeMux only cleans the escaped path, so encoded sequences such as "..%2F" reach r.URL.Path and
// r.PathValue. Strategies splicing these into upstream URLs or filesystem paths must reject them.
func ValidatePath(path string) error {
	if strings.ContainsAny(path, "\\\x00") {
		return errors.Errorf("invalid path %q", path)
	}
	for segment := range strings.SplitSeq(path, "/") {
		if segment == "." || segment == ".." {
			return errors.Errorf("invalid path %q: contains dot segment", path)
		}
	}
	return nil
}

// ValidatePathSegment returns an error unless s is one non-empty segment safe to splice into a URL or path.
func ValidatePathSegment(s string) error {
	if s == "" || strings.Contains(s, "/") {
		return errors.Errorf("invalid path segment %q", s)
	}
	return ValidatePath(s)
}

// RejectTraversal wraps next, answering 400 Bad Request when the decoded URL path fails [ValidatePath].
func RejectTraversal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := ValidatePath(r.URL.Path); err != nil {
			http.Error(w, "invalid request path", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
