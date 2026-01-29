package gomod

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/alecthomas/errors"
	"github.com/goproxy/goproxy"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

func init() {
	strategy.Register("gomod", "Caches Go module proxy requests.", New)
}

type Config struct {
	Proxy            string        `hcl:"proxy,optional" help:"Upstream Go module proxy URL (defaults to proxy.golang.org)" default:"https://proxy.golang.org"`
	PrivatePaths     []string      `hcl:"private-paths,optional" help:"Module path patterns for private repositories"`
	MirrorRoot       string        `hcl:"mirror-root,optional" help:"Directory to store git clones for private repos." default:""`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream for private repos." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks for private repos." default:"10s"`
	CloneDepth       int           `hcl:"clone-depth,optional" help:"Depth for shallow clones of private repos. 0 means full clone." default:"0"`
}

type Strategy struct {
	config       Config
	cache        cache.Cache
	logger       *slog.Logger
	proxy        *url.URL
	goproxy      *goproxy.Goproxy
	cloneManager *gitclone.Manager // Manager for cloning private repositories
}

var _ strategy.Strategy = (*Strategy)(nil)

func New(ctx context.Context, config Config, _ jobscheduler.Scheduler, cache cache.Cache, mux strategy.Mux) (*Strategy, error) {
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

	publicFetcher := &goproxy.GoFetcher{
		Env: []string{
			"GOPROXY=" + config.Proxy,
			"GOSUMDB=off", // Disable checksum database validation in fetcher, to prevent unneccessary double validation
		},
	}

	var fetcher goproxy.Fetcher = publicFetcher

	if len(config.PrivatePaths) > 0 {
		// Set default mirror root if not specified
		mirrorRoot := config.MirrorRoot
		if mirrorRoot == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return nil, errors.Wrap(err, "get user home directory")
			}
			mirrorRoot = filepath.Join(homeDir, ".cache", "cachew", "gomod-git-mirrors")
		}

		// Create gitclone manager for private repositories
		cloneManager, err := gitclone.NewManager(ctx, gitclone.Config{
			RootDir:          mirrorRoot,
			FetchInterval:    config.FetchInterval,
			RefCheckInterval: config.RefCheckInterval,
			CloneDepth:       config.CloneDepth,
			GitConfig:        gitclone.DefaultGitTuningConfig(),
		})
		if err != nil {
			return nil, errors.Wrap(err, "create clone manager for private repos")
		}
		s.cloneManager = cloneManager

		// Discover existing clones
		if err := cloneManager.DiscoverExisting(ctx); err != nil {
			s.logger.WarnContext(ctx, "Failed to discover existing clones for private repos",
				slog.String("error", err.Error()))
		}

		privateFetcher := newPrivateFetcher(s, cloneManager)
		fetcher = newCompositeFetcher(publicFetcher, privateFetcher, config.PrivatePaths)

		s.logger.InfoContext(ctx, "Configured private module support",
			slog.Any("private_paths", config.PrivatePaths),
			slog.String("mirror_root", mirrorRoot))
	}

	s.goproxy = &goproxy.Goproxy{
		Logger:  s.logger,
		Fetcher: fetcher,
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
