package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
	"github.com/block/cachew/internal/strategy/handler"
)

// RegisterHTTPProxy registers a caching HTTP proxy strategy for absolute-form requests like GET http://host/path.
//
// Used by e.g. sdkmanager with --proxy_host/--proxy_port. The URI is upgraded to HTTPS, and the response fetched
// and cached. Only GET requests are intercepted; other methods are passed through.
func RegisterHTTPProxy(r *Registry, spools spool.Provider) {
	Register(r, "proxy", "Caching HTTP proxy for absolute-form proxy requests.", func(ctx context.Context, _ ProxyConfig, c cache.Cache, mux Mux) (*HTTPProxy, error) {
		registry, err := spools()
		if err != nil {
			return nil, errors.Wrap(err, "create spool registry")
		}
		return NewHTTPProxy(ctx, c, mux, registry)
	})
}

// ProxyConfig holds configuration for the HTTP proxy strategy, which currently has no options.
type ProxyConfig struct{}

// HTTPProxy is a caching HTTP proxy strategy for absolute-form requests (GET http://host/path HTTP/1.1).
//
// It is an Interceptor rather than a mux route so detection happens before ServeMux matching, stopping
// more-specific routes such as /api/v1/ or /admin/ capturing proxied upstream paths that match them.
type HTTPProxy struct {
	logger  *slog.Logger
	handler http.Handler
}

var (
	_ Strategy    = (*HTTPProxy)(nil)
	_ Interceptor = (*HTTPProxy)(nil)
)

// NewHTTPProxy creates an HTTPProxy; it registers no routes because it intercepts requests instead.
func NewHTTPProxy(ctx context.Context, c cache.Cache, _ Mux, spools *spool.Registry) (*HTTPProxy, error) {
	logger := logging.FromContext(ctx)
	client := httputil.UpstreamClient()
	p := &HTTPProxy{logger: logger}

	p.handler = handler.New(client, c, spools).Metrics("proxy", "other").
		CacheKey(func(r *http.Request) string {
			target := p.parseProxyURI(r)
			if target == nil {
				return ""
			}
			return target.String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			target := p.parseProxyURI(r)
			if target == nil {
				return r, nil
			}
			return http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), nil)
		}).
		OnError(func(err error, w http.ResponseWriter, r *http.Request) {
			target := p.parseProxyURI(r)
			if target == nil {
				http.NotFound(w, r)
				return
			}
			p.logger.ErrorContext(r.Context(), "Proxy request failed",
				slog.String("url", target.String()),
				slog.String("error", err.Error()))
			http.Error(w, "proxy error: "+err.Error(), http.StatusBadGateway)
		})

	logger.InfoContext(ctx, "HTTP proxy strategy initialized")
	return p, nil
}

func (p *HTTPProxy) String() string { return "proxy" }

// Intercept handles absolute-form GET proxy requests before the ServeMux and delegates all others to next.
//
// This stops a proxied path like /api/v1/... being routed to cachew's own API handler.
func (p *HTTPProxy) Intercept(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Non-GET requests (HEAD, POST, …) are not cached and are passed through.
		if r.Method == http.MethodGet && strings.HasPrefix(r.RequestURI, "http://") {
			p.handler.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (p *HTTPProxy) parseProxyURI(r *http.Request) *url.URL {
	if !strings.HasPrefix(r.RequestURI, "http://") {
		return nil
	}
	target, err := url.Parse(r.RequestURI)
	if err != nil || target.Host == "" {
		return nil
	}
	// Upgrade to HTTPS for the upstream fetch.
	target.Scheme = "https"
	return target
}
