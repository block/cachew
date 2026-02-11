package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

const artifactoryStrategyName = "artifactory"

func RegisterArtifactory(r *Registry) {
	Register(r, artifactoryStrategyName, "Caches artifacts from an Artifactory server.", NewArtifactory)
}

// ArtifactoryConfig represents the configuration for the Artifactory strategy.
//
// In HCL it looks something like this:
//
//	artifactory "https://example.jfrog.io" {
//	  hosts = ["maven.example.com", "npm.example.com"]
//	}
//
// When hosts are configured, the strategy supports both host-based routing
// (clients connect to maven.example.com) and path-based routing
// (clients connect to /example.jfrog.io). Both modes share the same cache.
type ArtifactoryConfig struct {
	Target string   `hcl:"target,label" help:"The target Artifactory URL to proxy requests to."`
	Hosts  []string `hcl:"hosts,optional" help:"List of hostnames to accept for host-based routing. If empty, uses path-based routing only."`
}

// The Artifactory [Strategy] forwards all GET requests to the specified Artifactory instance,
// caching the response payloads.
//
// Key features:
// - Sets X-JFrog-Download-Redirect-To header to prevent redirects
// - Passes through authentication headers
// - Supports both host-based and path-based routing simultaneously.
type Artifactory struct {
	target       *url.URL
	cache        cache.Cache
	client       *http.Client
	logger       *slog.Logger
	prefix       string   // For path-based routing
	allowedHosts []string // For host-based routing
}

var _ Strategy = (*Artifactory)(nil)

func NewArtifactory(ctx context.Context, config ArtifactoryConfig, cache cache.Cache, mux Mux) (*Artifactory, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}

	a := &Artifactory{
		target: u,
		cache:  cache,
		client: &http.Client{},
		logger: logging.FromContext(ctx),
	}

	hdlr := handler.New(a.client, cache).
		StrategyName(artifactoryStrategyName).
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

// registerPathBased registers the path-based routing pattern.
func (a *Artifactory) registerPathBased(ctx context.Context, target *url.URL, hdlr http.Handler, mux Mux) {
	a.prefix = "/" + target.Host + target.EscapedPath()

	pattern := "GET " + a.prefix + "/"
	mux.Handle(pattern, hdlr)
	a.logger.InfoContext(ctx, "Registered Artifactory path-based route",
		slog.String("prefix", a.prefix),
		slog.String("target", target.String()))
}

// registerHostBased registers host-based routing patterns for the configured hosts.
func (a *Artifactory) registerHostBased(ctx context.Context, hosts []string, hdlr http.Handler, mux Mux) {
	// Store allowed hosts for routing detection in buildTargetURL
	a.allowedHosts = hosts

	for _, host := range hosts {
		pattern := "GET " + host + "/"
		mux.Handle(pattern, hdlr)
		a.logger.InfoContext(ctx, "Registered Artifactory host-based route",
			slog.String("pattern", pattern),
			slog.String("target", a.target.String()))
	}
}

func (a *Artifactory) String() string { return "artifactory:" + a.target.Host + a.target.Path }

// transformRequest transforms the incoming request before sending to upstream Artifactory.
func (a *Artifactory) transformRequest(r *http.Request) (*http.Request, error) {
	targetURL := a.buildTargetURL(r)

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Pass through authentication headers
	a.copyAuthHeaders(r, req)

	// Set X-JFrog-Download-Redirect-To to None to prevent Artifactory from redirecting
	// This ensures the proxy can cache the actual artifact content
	req.Header.Set("X-Jfrog-Download-Redirect-To", "None")

	return req, nil
}

// buildTargetURL constructs the target URL from the incoming request.
func (a *Artifactory) buildTargetURL(r *http.Request) *url.URL {
	var path string

	// Dynamically detect routing mode based on request
	// If request Host matches one of our configured hosts, use host-based routing
	// Otherwise, use path-based routing
	isHostBased := a.isHostBasedRequest(r)

	if isHostBased {
		// Host-based: use full request path as-is
		// Request: GET http://maven.example.jfrog.io/libs-release/foo.jar
		// Proxy to: GET https://global.example.jfrog.io/libs-release/foo.jar
		path = r.URL.Path
		if path == "" {
			path = "/"
		}
	} else {
		// Path-based: strip prefix from request path
		// Request: GET http://cachew.local/global.example.jfrog.io/libs-release/foo.jar
		// Strip "/global.example.jfrog.io" -> "/libs-release/foo.jar"
		// Proxy to: GET https://global.example.jfrog.io/libs-release/foo.jar
		path = r.URL.Path
		if len(path) >= len(a.prefix) {
			path = path[len(a.prefix):]
		}
		if path == "" {
			path = "/"
		}
	}

	a.logger.Debug("buildTargetURL",
		"host_based", isHostBased,
		"request_host", r.Host,
		"request_path", r.URL.Path,
		"stripped_path", path)

	targetURL := *a.target
	targetURL.Path = a.target.Path + path
	targetURL.RawQuery = r.URL.RawQuery

	a.logger.Debug("buildTargetURL result",
		"url", targetURL.String())

	return &targetURL
}

// isHostBasedRequest checks if the incoming request is using host-based routing.
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

// copyAuthHeaders copies authentication-related headers from the source to destination request.
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
