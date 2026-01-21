package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func init() {
	Register("host", NewHost)
}

// HostConfig represents the configuration for the Host strategy.
//
// In HCL it looks something like this:
//
//	host {
//		target = "https://github.com/"
//	}
//
// In this example, the strategy will be mounted under "/github.com".
type HostConfig struct {
	Target string `hcl:"target,label" help:"The target URL to proxy requests to."`
}

// The Host [Strategy] forwards all GET requests to the specified host, caching the response payloads.
type Host struct {
	target *url.URL
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	prefix string
}

var _ Strategy = (*Host)(nil)

func NewHost(ctx context.Context, config HostConfig, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*Host, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, fmt.Errorf("invalid target URL: %w", err)
	}
	prefix := "/" + u.Host + u.EscapedPath()
	h := &Host{
		target: u,
		cache:  cache,
		client: &http.Client{},
		logger: logging.FromContext(ctx),
		prefix: prefix,
	}

	hdlr := handler.New(h.client, cache).
		CacheKey(func(r *http.Request) string {
			return h.buildTargetURL(r).String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			targetURL := h.buildTargetURL(r)
			return http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
		})

	mux.Handle("GET "+prefix+"/", hdlr)
	return h, nil
}

func (d *Host) String() string { return "host:" + d.target.Host + d.target.Path }

// buildTargetURL constructs the target URL from the incoming request.
func (d *Host) buildTargetURL(r *http.Request) *url.URL {
	// Strip the prefix from the request path
	path := r.URL.Path
	if len(path) >= len(d.prefix) {
		path = path[len(d.prefix):]
	}
	if path == "" {
		path = "/"
	}

	targetURL, err := url.Parse(d.target.String())
	if err != nil {
		d.logger.Error("Failed to parse target URL", "error", err.Error(), "target", d.target.String())
		return &url.URL{}
	}
	targetURL.Path = path
	targetURL.RawQuery = r.URL.RawQuery
	return targetURL
}
