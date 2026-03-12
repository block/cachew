package git

import (
	"net/http"

	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), "Forwarding to upstream", "method", r.Method, "host", host, "path", pathValue)

	s.proxy.ServeHTTP(w, r)
}
