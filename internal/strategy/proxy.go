package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

// RegisterHTTPProxy registers a caching HTTP proxy strategy. It intercepts
// absolute-form proxy requests (e.g. from sdkmanager with --proxy_host /
// --proxy_port) where the client sends:
//
//	GET http://dl.google.com/some/path HTTP/1.1
//
// The request URI is upgraded to HTTPS and the response is fetched and cached.
// Only GET requests are intercepted; other methods are passed through.
func RegisterHTTPProxy(r *Registry) {
	Register(r, "proxy", "Caching HTTP proxy for absolute-form proxy requests.", func(ctx context.Context, _ ProxyConfig, c cache.Cache, mux Mux) (*HTTPProxy, error) {
		return NewHTTPProxy(ctx, c, mux)
	})
}

// ProxyConfig holds configuration for the HTTP proxy strategy.
// Currently no options are required.
type ProxyConfig struct{}

// HTTPProxy is a caching HTTP proxy strategy that handles standard HTTP proxy
// requests in absolute form (GET http://host/path HTTP/1.1).
//
// It implements the Interceptor interface rather than registering on the mux
// directly, so that absolute-form request detection happens before ServeMux
// route matching. This prevents overlap with more-specific routes such as
// /api/v1/ or /admin/ when the proxied upstream path happens to match them.
type HTTPProxy struct {
	logger  *slog.Logger
	handler http.Handler
}

var (
	_ Strategy    = (*HTTPProxy)(nil)
	_ Interceptor = (*HTTPProxy)(nil)
)

func NewHTTPProxy(ctx context.Context, c cache.Cache, _ Mux) (*HTTPProxy, error) {
	logger := logging.FromContext(ctx)
	client := &http.Client{}
	p := &HTTPProxy{logger: logger}

	p.handler = handler.New(client, c).
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

// Intercept returns an http.Handler that intercepts absolute-form GET proxy
// requests before they reach the ServeMux, delegating all other requests to
// next. This ensures that a proxied path like /api/v1/... is not accidentally
// routed to cachew's own API handler.
func (p *HTTPProxy) Intercept(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only intercept absolute-form GET requests. Non-GET requests
		// (HEAD, POST, …) are not cached and are passed through.
		if r.Method == http.MethodGet && strings.HasPrefix(r.RequestURI, "http://") {
			p.handler.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// parseProxyURI returns the HTTPS upstream URL for an absolute-form proxy
// request, or nil if the request is not a proxy request.
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
