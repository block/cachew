package strategy

import (
	"context"
	"io"
	"time"

	"github.com/goproxy/goproxy"
)

type compositeFetcher struct {
	publicFetcher  goproxy.Fetcher
	privateFetcher goproxy.Fetcher
	matcher        *ModulePathMatcher
}

func newCompositeFetcher(
	publicFetcher goproxy.Fetcher,
	privateFetcher goproxy.Fetcher,
	patterns []string,
) *compositeFetcher {
	return &compositeFetcher{
		publicFetcher:  publicFetcher,
		privateFetcher: privateFetcher,
		matcher:        NewModulePathMatcher(patterns),
	}
}

func (c *compositeFetcher) Query(ctx context.Context, path, query string) (version string, t time.Time, err error) {
	if c.matcher.IsPrivate(path) {
		return c.privateFetcher.Query(ctx, path, query)
	}
	return c.publicFetcher.Query(ctx, path, query)
}

func (c *compositeFetcher) List(ctx context.Context, path string) (versions []string, err error) {
	if c.matcher.IsPrivate(path) {
		return c.privateFetcher.List(ctx, path)
	}
	return c.publicFetcher.List(ctx, path)
}

func (c *compositeFetcher) Download(ctx context.Context, path, version string) (info, mod, zip io.ReadSeekCloser, err error) {
	if c.matcher.IsPrivate(path) {
		return c.privateFetcher.Download(ctx, path, version)
	}
	return c.publicFetcher.Download(ctx, path, version)
}
