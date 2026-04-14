package httputil

import (
	"net/http"

	"github.com/block/cachew/internal/logging"
)

// LoggingMiddleware adds method/URI to the logger and optionally extracts
// request headers into log attributes based on the provided headerAttrs map.
func LoggingMiddleware(headerAttrs map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := logging.FromContext(r.Context()).With("method", r.Method, "uri", r.RequestURI)
		for header, attr := range headerAttrs {
			if v := r.Header.Get(header); v != "" {
				logger = logger.With(attr, v)
			}
		}
		r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
		next.ServeHTTP(w, r)
	})
}
