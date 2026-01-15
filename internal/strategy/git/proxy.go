package git

import (
	"io"
	"log/slog"
	"net/http"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// forwardToUpstream forwards a request to the upstream Git server.
func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	upstreamURL := "https://" + host + "/" + pathValue
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	logger.DebugContext(ctx, "Forwarding to upstream",
		slog.String("method", r.Method),
		slog.String("upstream_url", upstreamURL))

	upstreamReq, err := http.NewRequestWithContext(ctx, r.Method, upstreamURL, r.Body)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to create upstream request")
		return
	}

	// Copy relevant headers
	for _, header := range []string{"Content-Type", "Content-Length", "Content-Encoding", "Accept", "Accept-Encoding", "Git-Protocol"} {
		if v := r.Header.Get(header); v != "" {
			upstreamReq.Header.Set(header, v)
		}
	}

	resp, err := s.httpClient.Do(upstreamReq)
	if err != nil {
		logger.ErrorContext(ctx, "Upstream request failed", slog.String("error", err.Error()))
		httputil.ErrorResponse(w, r, http.StatusBadGateway, "upstream request failed")
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)

	if _, err := io.Copy(w, resp.Body); err != nil {
		logger.ErrorContext(ctx, "Failed to stream upstream response", slog.String("error", err.Error()))
	}
}
