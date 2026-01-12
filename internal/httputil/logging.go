package httputil

import (
	"fmt"
	"net/http"

	"github.com/block/sfptc/internal/logging"
)

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Propagate attributes tot the handlers.
		logger := logging.FromContext(r.Context()).With("request", fmt.Sprintf("%s %s", r.Method, r.RequestURI))
		r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
		logger.Debug("Request received")
		next.ServeHTTP(w, r)
	})
}
