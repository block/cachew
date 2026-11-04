package httputil

import (
	"net/http"
	"strings"

	"github.com/alecthomas/errors"
)

// ValidatePath returns an error if the slash-separated path contains "." or
// ".." segments, backslashes, or NUL bytes.
//
// Go's ServeMux only cleans the escaped request path, so percent-encoded
// sequences such as "..%2F" or "%2E%2E" survive routing and appear verbatim
// in r.URL.Path and r.PathValue. Strategies that splice these values into
// upstream URLs or filesystem paths must reject them to prevent traversal.
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

// ValidatePathSegment returns an error if s is not a single, non-empty path
// segment that is safe to splice into a URL or filesystem path.
func ValidatePathSegment(s string) error {
	if s == "" || strings.Contains(s, "/") {
		return errors.Errorf("invalid path segment %q", s)
	}
	return ValidatePath(s)
}

// RejectTraversal wraps next, responding with 400 Bad Request to any request
// whose decoded URL path fails [ValidatePath].
func RejectTraversal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := ValidatePath(r.URL.Path); err != nil {
			http.Error(w, "invalid request path", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
