package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func init() {
	Register("artifactory", NewArtifactory)
}

// ArtifactoryConfig represents the configuration for the Artifactory strategy.
//
// In HCL it looks something like this:
//
//	artifactory "https://global.block-artifacts.com" {
//	}
//
// The strategy will be mounted under "/global.block-artifacts.com".
type ArtifactoryConfig struct {
	Target string `hcl:"target,label" help:"The target Artifactory URL to proxy requests to."`
}

// The Artifactory [Strategy] forwards all GET requests to the specified Artifactory instance,
// caching the response payloads.
//
// Key features:
// - Cache key uses full target URL (consistent with other strategies)
// - Sets X-JFrog-Download-Redirect-To header to prevent redirects
// - 7-day default TTL for cached artifacts
// - Passes through authentication headers
type Artifactory struct {
	target *url.URL
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	prefix string
}

var _ Strategy = (*Artifactory)(nil)

func NewArtifactory(ctx context.Context, config ArtifactoryConfig, cache cache.Cache, mux Mux) (*Artifactory, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}

	prefix := "/" + u.Host + u.EscapedPath()
	a := &Artifactory{
		target: u,
		cache:  cache,
		client: &http.Client{},
		logger: logging.FromContext(ctx),
		prefix: prefix,
	}

	hdlr := handler.New(a.client, cache).
		CacheKey(func(r *http.Request) string {
			return a.buildTargetURL(r).String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			return a.transformRequest(r)
		}).
		TTL(func(r *http.Request) time.Duration {
			return 7 * 24 * time.Hour // 7 days
		})

	mux.Handle("GET "+prefix+"/", hdlr)
	a.logger.InfoContext(ctx, "Registered Artifactory strategy",
		slog.String("prefix", prefix),
		slog.String("target", config.Target))

	return a, nil
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
	req.Header.Set("X-JFrog-Download-Redirect-To", "None")

	return req, nil
}

// buildTargetURL constructs the target URL from the incoming request.
func (a *Artifactory) buildTargetURL(r *http.Request) *url.URL {
	// Strip the prefix from the request path
	path := r.URL.Path
	if len(path) >= len(a.prefix) {
		path = path[len(a.prefix):]
	}
	if path == "" {
		path = "/"
	}

	a.logger.Debug("buildTargetURL",
		"target", a.target.String(),
		"target.Scheme", a.target.Scheme,
		"target.Host", a.target.Host,
		"target.Path", a.target.Path,
		"stripped_path", path)

	targetURL := *a.target
	targetURL.Path = a.target.Path + path
	targetURL.RawQuery = r.URL.RawQuery

	a.logger.Debug("buildTargetURL result",
		"url", targetURL.String(),
		"scheme", targetURL.Scheme,
		"host", targetURL.Host,
		"path", targetURL.Path)

	return &targetURL
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
