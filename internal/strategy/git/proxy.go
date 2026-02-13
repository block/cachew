package git

import (
	"fmt"
	"net/http"

	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), fmt.Sprintf("Forwarding to upstream: %s %s%s", r.Method, host, pathValue),
		"method", r.Method,
		"host", host,
		"path", pathValue)

	s.proxy.ServeHTTP(w, r)
}
