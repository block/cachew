package httputil

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

// heuristicallyCacheable lists the status codes RFC 9110 §15.1 defines as
// heuristically cacheable.
var heuristicallyCacheable = map[int]bool{ //nolint:gochecknoglobals
	http.StatusOK:                   true,
	http.StatusNonAuthoritativeInfo: true,
	http.StatusNoContent:            true,
	http.StatusPartialContent:       true,
	http.StatusMultipleChoices:      true,
	http.StatusMovedPermanently:     true,
	http.StatusPermanentRedirect:    true,
	http.StatusNotFound:             true,
	http.StatusMethodNotAllowed:     true,
	http.StatusGone:                 true,
	http.StatusRequestURITooLong:    true,
	http.StatusNotImplemented:       true,
}

// ReuseLifetime applies RFC 9111 shared-cache rules to a response to req. heuristic
// is the lifetime of a heuristically cacheable response without explicit freshness.
func ReuseLifetime(req *http.Request, status int, header http.Header, heuristic time.Duration) (lifetime time.Duration, ok bool) {
	if req.Method != http.MethodGet || status < http.StatusOK {
		return 0, false
	}
	directives := cacheControl(header)
	if directives.has("no-store") || directives.has("no-cache") || directives.has("private") {
		return 0, false
	}
	if req.Header.Get("Authorization") != "" &&
		!directives.has("public") && !directives.has("s-maxage") && !directives.has("must-revalidate") {
		return 0, false
	}
	switch {
	case directives.has("s-maxage"):
		lifetime, ok = directives.seconds("s-maxage")
	case directives.has("max-age"):
		lifetime, ok = directives.seconds("max-age")
	case header.Get("Expires") != "":
		lifetime, ok = expiresLifetime(header)
	case heuristicallyCacheable[status] || directives.has("public"):
		lifetime, ok = heuristic, true
	}
	if !ok {
		return 0, false
	}
	if age, err := strconv.ParseInt(header.Get("Age"), 10, 64); err == nil {
		lifetime -= time.Duration(age) * time.Second
	}
	return lifetime, lifetime > 0
}

type cacheDirectives map[string]string

func cacheControl(header http.Header) cacheDirectives {
	directives := cacheDirectives{}
	for _, value := range header.Values("Cache-Control") {
		for directive := range strings.SplitSeq(value, ",") {
			name, arg, _ := strings.Cut(strings.TrimSpace(directive), "=")
			if name != "" {
				directives[strings.ToLower(name)] = strings.Trim(arg, `"`)
			}
		}
	}
	return directives
}

func (d cacheDirectives) has(name string) bool {
	_, ok := d[name]
	return ok
}

func (d cacheDirectives) seconds(name string) (time.Duration, bool) {
	seconds, err := strconv.ParseInt(d[name], 10, 64)
	if err != nil {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

// expiresLifetime measures Expires against Date so clock skew does not matter. An
// unparseable Expires means already stale (RFC 9111 §5.3).
func expiresLifetime(header http.Header) (time.Duration, bool) {
	expires, err := http.ParseTime(header.Get("Expires"))
	if err != nil {
		return 0, false
	}
	date, err := http.ParseTime(header.Get("Date"))
	if err != nil {
		date = time.Now()
	}
	return expires.Sub(date), true
}
