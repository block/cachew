package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
	"github.com/block/cachew/internal/strategy/handler"
)

// RegisterArtifactory registers the artifactory strategy, which caches artifacts from an Artifactory server.
func RegisterArtifactory(r *Registry, spools spool.Provider) {
	Register(r, "artifactory", "Caches artifacts from an Artifactory server.", func(ctx context.Context, config ArtifactoryConfig, c cache.Cache, mux Mux) (*Artifactory, error) {
		registry, err := spools()
		if err != nil {
			return nil, errors.Wrap(err, "create spool registry")
		}
		return NewArtifactory(ctx, config, c, mux, registry)
	})
}

// ArtifactoryConfig configures the Artifactory strategy, e.g. `artifactory "https://x.jfrog.io" { hosts = [...] }`.
//
// With hosts configured, both host-based routing (clients connect to maven.example.com) and path-based routing
// (clients connect to /example.jfrog.io) are supported, sharing the same cache.
type ArtifactoryConfig struct {
	Target string   `hcl:"target,label" help:"The target Artifactory URL to proxy requests to."`
	Hosts  []string `hcl:"hosts,optional" help:"List of hostnames to accept for host-based routing. If empty, uses path-based routing only."`
}

// The Artifactory [Strategy] forwards GET requests to an Artifactory instance, caching the response payloads.
//
// It passes through auth headers, disables download redirects, and supports host- and path-based routing at once.
type Artifactory struct {
	target       *url.URL
	cache        cache.Cache
	client       *http.Client
	logger       *slog.Logger
	prefix       string   // For path-based routing
	allowedHosts []string // For host-based routing
}

var _ Strategy = (*Artifactory)(nil)

// NewArtifactory creates an Artifactory strategy and registers its path-based and host-based routes on mux.
func NewArtifactory(ctx context.Context, config ArtifactoryConfig, cache cache.Cache, mux Mux, spools *spool.Registry) (*Artifactory, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, errors.Errorf("invalid target URL: %w", err)
	}

	a := &Artifactory{
		target: u,
		cache:  cache,
		client: httputil.UpstreamClient(),
		logger: logging.FromContext(ctx),
	}

	hdlr := handler.New(a.client, cache, spools).Metrics("artifactory", u.Hostname()).
		CacheKey(func(r *http.Request) string {
			return a.buildTargetURL(r).String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			return a.transformRequest(r)
		})

	// Register path-based route (for backward compatibility)
	a.registerPathBased(ctx, u, hdlr, mux)

	// Register host-based routes if configured
	if len(config.Hosts) > 0 {
		a.registerHostBased(ctx, config.Hosts, hdlr, mux)
	}

	return a, nil
}

func (a *Artifactory) registerPathBased(ctx context.Context, target *url.URL, hdlr http.Handler, mux Mux) {
	a.prefix = "/" + target.Host + target.EscapedPath()

	pattern := "GET " + a.prefix + "/"
	mux.Handle(pattern, hdlr)
	a.logger.InfoContext(ctx, "Registered Artifactory path-based route", "prefix", a.prefix, "target", target)
}

func (a *Artifactory) registerHostBased(ctx context.Context, hosts []string, hdlr http.Handler, mux Mux) {
	// Store allowed hosts for routing detection in buildTargetURL
	a.allowedHosts = hosts

	for _, host := range hosts {
		pattern := "GET " + host + "/"
		mux.Handle(pattern, hdlr)
		a.logger.InfoContext(ctx, "Registered Artifactory host-based route", "pattern", pattern, "target", a.target)
	}
}

func (a *Artifactory) String() string { return "artifactory:" + a.target.Host + a.target.Path }

func (a *Artifactory) transformRequest(r *http.Request) (*http.Request, error) {
	if err := httputil.ValidatePath(r.URL.Path); err != nil {
		return nil, httputil.Errorf(http.StatusBadRequest, "%w", err)
	}
	targetURL := a.buildTargetURL(r)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, errors.Errorf("failed to create request: %w", err)
	}

	// Pass through authentication headers
	a.copyAuthHeaders(r, req)

	// Disable Artifactory download redirects so the proxy caches the artifact itself.
	req.Header.Set("X-Jfrog-Download-Redirect-To", "None")

	return req, nil
}

// buildTargetURL constructs the target URL from the incoming request.
func (a *Artifactory) buildTargetURL(r *http.Request) *url.URL {
	var path string

	isHostBased := a.isHostBasedRequest(r)

	if isHostBased {
		// Host-based: proxy the request path as-is, e.g. /libs-release/foo.jar.
		path = r.URL.Path
		if path == "" {
			path = "/"
		}
	} else {
		// Path-based: strip the prefix, e.g. /example.jfrog.io/libs-release/foo.jar -> /libs-release/foo.jar.
		path = r.URL.Path
		if len(path) >= len(a.prefix) {
			path = path[len(a.prefix):]
		}
		if path == "" {
			path = "/"
		}
	}

	a.logger.DebugContext(r.Context(), "buildTargetURL", "host_based", isHostBased, "request_host", r.Host, "request_path", r.URL.Path,
		"stripped_path", path)

	targetURL := *a.target
	targetURL.Path = a.target.Path + path
	targetURL.RawQuery = r.URL.RawQuery

	a.logger.DebugContext(r.Context(), "buildTargetURL result", "url", targetURL)

	return &targetURL
}

func (a *Artifactory) isHostBasedRequest(r *http.Request) bool {
	if len(a.allowedHosts) == 0 {
		return false // No hosts configured, must be path-based
	}

	// Strip port from request host for comparison
	requestHost := r.Host
	if colonIdx := strings.Index(requestHost, ":"); colonIdx != -1 {
		requestHost = requestHost[:colonIdx]
	}

	// Check if request host matches any configured host
	return slices.Contains(a.allowedHosts, requestHost)
}

func (a *Artifactory) copyAuthHeaders(src, dst *http.Request) {
	authHeaders := []string{
		"Authorization",
		"X-JFrog-Art-Api",
		"Cookie",
	}

	for _, header := range authHeaders {
		if value := src.Header.Get(header); value != "" {
			dst.Header.Set(header, value)
		}
	}
}
