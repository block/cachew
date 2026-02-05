package gomod

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/goproxy/goproxy"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func Register(r *strategy.Registry) {
	strategy.Register(r, "gomod", "Caches Go module proxy requests.", New)
}

type Config struct {
	Proxy string `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
}

type Strategy struct {
	config  Config
	cache   cache.Cache
	logger  *slog.Logger
	proxy   *url.URL
	goproxy *goproxy.Goproxy
}

var _ strategy.Strategy = (*Strategy)(nil)

func New(ctx context.Context, config Config, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
	parsedURL, err := url.Parse(config.Proxy)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}

	s := &Strategy{
		config: config,
		cache:  cache,
		logger: logging.FromContext(ctx),
		proxy:  parsedURL,
	}

	s.goproxy = &goproxy.Goproxy{
		Logger: s.logger,
		Fetcher: &goproxy.GoFetcher{
			Env: []string{
				"GOPROXY=" + config.Proxy,
				"GOSUMDB=off", // Disable checksum database validation in fetcher, to prevent unneccessary double validation
			},
		},
		Cacher: &goproxyCacher{
			cache: cache,
		},
		ProxiedSumDBs: []string{
			"sum.golang.org https://sum.golang.org",
		},
	}

	s.logger.InfoContext(ctx, "Initialized Go module proxy strategy",
		slog.String("proxy", s.proxy.String()))

	mux.Handle("GET /gomod/{path...}", http.StripPrefix("/gomod", s.goproxy))

	return s, nil
}

func (s *Strategy) String() string {
	return "gomod:" + s.proxy.Host
}
