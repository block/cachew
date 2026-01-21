package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func init() {
	Register("gomod", "Caches Go module proxy requests.", NewGoMod)
}

// GoModConfig represents the configuration for the Go module proxy strategy.
//
// In HCL it looks like:
//
//	gomod {
//	  proxy = "https://proxy.golang.org"
//	}
type GoModConfig struct {
	Proxy        string        `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
	MutableTTL   time.Duration `hcl:"mutable-ttl,optional" help:"TTL for mutable Go module proxy endpoints (list, latest). Defaults to 5m." default:"5m"`
	ImmutableTTL time.Duration `hcl:"immutable-ttl,optional" help:"TTL for immutable Go module proxy endpoints (versioned info, mod, zip). Defaults to 168h (7 days)." default:"168h"`
}

// The GoMod strategy implements a caching proxy for the Go module proxy protocol.
//
// It supports all standard GOPROXY endpoints:
// - /$module/@v/list - Lists available versions
// - /$module/@v/$version.info - Version metadata JSON
// - /$module/@v/$version.mod - go.mod file
// - /$module/@v/$version.zip - Module source code
// - /$module/@latest - Latest version info
//
// The strategy uses differential caching: short TTL (5 minutes) for mutable
// endpoints (list, latest) and long TTL (7 days) for immutable versioned content.
type GoMod struct {
	config GoModConfig
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	proxy  *url.URL
}

var _ Strategy = (*GoMod)(nil)

// NewGoMod creates a new Go module proxy strategy.
func NewGoMod(ctx context.Context, config GoModConfig, scheduler jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*GoMod, error) {
	parsedURL, err := url.Parse(config.Proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	g := &GoMod{
		config: config,
		cache:  cache,
		client: http.DefaultClient,
		logger: logging.FromContext(ctx),
		proxy:  parsedURL,
	}

	g.logger.InfoContext(ctx, "Initialized Go module proxy strategy",
		slog.String("proxy", g.proxy.String()))

	// Create handler with caching configuration
	h := handler.New(g.client, g.cache).
		CacheKey(func(r *http.Request) string {
			return g.buildUpstreamURL(r).String()
		}).
		Transform(g.transformRequest).
		TTL(g.calculateTTL)

	// Register a namespaced handler for Go module proxy patterns
	mux.Handle("GET /gomod/{path...}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		// Check if this is a valid Go module proxy endpoint
		if g.isGoModulePath(path) {
			h.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))

	return g, nil
}

// isGoModulePath checks if the path matches a valid Go module proxy endpoint pattern.
func (g *GoMod) isGoModulePath(path string) bool {
	// Strip the /gomod prefix before checking the pattern
	path = strings.TrimPrefix(path, "/gomod")

	// Valid patterns:
	// - /@v/list
	// - /@v/{version}.info
	// - /@v/{version}.mod
	// - /@v/{version}.zip
	// - /@latest
	return strings.HasSuffix(path, "/@v/list") ||
		strings.HasSuffix(path, "/@latest") ||
		(strings.Contains(path, "/@v/") &&
			(strings.HasSuffix(path, ".info") ||
				strings.HasSuffix(path, ".mod") ||
				strings.HasSuffix(path, ".zip")))
}

func (g *GoMod) String() string {
	return "gomod:" + g.proxy.Host
}

// buildUpstreamURL constructs the full upstream URL from the incoming request.
func (g *GoMod) buildUpstreamURL(r *http.Request) *url.URL {
	// The full path includes the module path and the endpoint
	// e.g., /gomod/github.com/user/repo/@v/v1.0.0.info
	// We need to strip the /gomod prefix before forwarding to the upstream proxy
	path := r.URL.Path
	path = strings.TrimPrefix(path, "/gomod")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	targetURL := *g.proxy
	targetURL.Path = g.proxy.Path + path
	targetURL.RawQuery = r.URL.RawQuery

	return &targetURL
}

// transformRequest creates the upstream request to the Go module proxy.
func (g *GoMod) transformRequest(r *http.Request) (*http.Request, error) {
	targetURL := g.buildUpstreamURL(r)

	g.logger.DebugContext(r.Context(), "Transforming Go module request",
		slog.String("original_path", r.URL.Path),
		slog.String("upstream_url", targetURL.String()))

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, httputil.Errorf(http.StatusInternalServerError, "create upstream request: %w", err)
	}

	return req, nil
}

// calculateTTL returns the appropriate cache TTL based on the endpoint type.
//
// Mutable endpoints (list, latest) get short TTL (5 minutes).
// Immutable versioned content (info, mod, zip) gets long TTL (7 days).
func (g *GoMod) calculateTTL(r *http.Request) time.Duration {
	path := r.URL.Path

	// Short TTL for mutable endpoints
	if strings.HasSuffix(path, "/@v/list") || strings.HasSuffix(path, "/@latest") {
		return g.config.MutableTTL
	}

	// Long TTL for immutable versioned content (.info, .mod, .zip)
	return g.config.ImmutableTTL
}
