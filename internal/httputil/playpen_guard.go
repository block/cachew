package httputil

import (
	"net/http"
	"strings"
)

// PlaypenGuardMiddleware rejects requests that target a playpen instance
// (via the Baggage header) when this instance is not a playpen.
// This prevents requests from silently falling through to staging.
func PlaypenGuardMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if baggage := r.Header.Get("Baggage"); hasCachewPlaypenKey(baggage) {
			http.Error(w, "no matching cachew playpen found — start one with: sq playpen sync", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func hasCachewPlaypenKey(baggage string) bool {
	if baggage == "" {
		return false
	}
	for entry := range strings.SplitSeq(baggage, ",") {
		if strings.HasPrefix(strings.TrimSpace(entry), "cachew-playpen=") {
			return true
		}
	}
	return false
}
