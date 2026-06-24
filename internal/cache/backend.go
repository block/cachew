package cache

import (
	"net/http"
	"strings"
)

// ServedByHeader is an internal response header set by the Tiered cache on a
// successful Open to report which backing tier produced the object (e.g.
// "disk" or "s3"). It lets serve handlers attribute latency to a specific
// tier. It is not forwarded to clients by the strategy handlers.
const ServedByHeader = "X-Cachew-Served-By"

// BackendKind returns a coarse, low-cardinality identifier for a cache
// implementation, derived from the text before the first ":" in its String()
// form (e.g. "disk", "s3", "memory", "remote", "tiered"). It is suitable as a
// metric label value.
func BackendKind(c Cache) string {
	if c == nil {
		return "unknown"
	}
	kind, _, _ := strings.Cut(c.String(), ":")
	return kind
}

// BackendFromHeaders returns the serving tier reported via ServedByHeader, or
// "" when absent (e.g. a single-tier cache that does not annotate the tier).
func BackendFromHeaders(h http.Header) string {
	if h == nil {
		return ""
	}
	return h.Get(ServedByHeader)
}
