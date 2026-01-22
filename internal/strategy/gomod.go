package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/goproxy/goproxy"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func init() {
	Register("gomod", "Caches Go module proxy requests.", NewGoMod)
}

type GoModConfig struct {
	Proxy        string        `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
	MutableTTL   time.Duration `hcl:"mutable-ttl,optional" help:"TTL for mutable Go module proxy endpoints (list, latest). Defaults to 5m." default:"5m"`
	ImmutableTTL time.Duration `hcl:"immutable-ttl,optional" help:"TTL for immutable Go module proxy endpoints (versioned info, mod, zip). Defaults to 168h (7 days)." default:"168h"`
}

type GoMod struct {
	config  GoModConfig
	cache   cache.Cache
	logger  *slog.Logger
	proxy   *url.URL
	goproxy *goproxy.Goproxy
}

var _ Strategy = (*GoMod)(nil)

// NewGoMod creates a new Go module proxy strategy.
func NewGoMod(ctx context.Context, config GoModConfig, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*GoMod, error) {
	parsedURL, err := url.Parse(config.Proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	g := &GoMod{
		config: config,
		cache:  cache,
		logger: logging.FromContext(ctx),
		proxy:  parsedURL,
	}

	// Create the goproxy instance with our custom cacher adapter
	g.goproxy = &goproxy.Goproxy{
		Logger: g.logger,
		Fetcher: &goproxy.GoFetcher{
			// Configure to use the specified upstream proxy
			Env: []string{
				"GOPROXY=" + config.Proxy,
				"GOSUMDB=off", // Disable checksum database validation in fetcher, to prevent unneccessary double validation
			},
		},
		Cacher: &goproxyCacher{
			cache:        cache,
			mutableTTL:   config.MutableTTL,
			immutableTTL: config.ImmutableTTL,
		},
		ProxiedSumDBs: []string{
			"sum.golang.org https://sum.golang.org",
		},
	}

	g.logger.InfoContext(ctx, "Initialized Go module proxy strategy",
		slog.String("proxy", g.proxy.String()),
		slog.Duration("mutable_ttl", config.MutableTTL),
		slog.Duration("immutable_ttl", config.ImmutableTTL))

	// Register a namespaced handler for Go module proxy patterns
	// Strip the /gomod prefix and delegate to goproxy
	mux.Handle("GET /gomod/{path...}", http.StripPrefix("/gomod", g.goproxy))

	return g, nil
}

func (g *GoMod) String() string {
	return "gomod:" + g.proxy.Host
}
