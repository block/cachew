package git

import (
	"log/slog"
	"net/http"

	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), "Forwarding to upstream",
		slog.String("method", r.Method),
		slog.String("host", host),
		slog.String("path", pathValue))

	s.proxy.ServeHTTP(w, r)
}
