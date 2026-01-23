package strategy

import (
	"context"
	"io"
	"time"

	"github.com/alecthomas/errors"
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
		v, tm, err := c.privateFetcher.Query(ctx, path, query)
		return v, tm, errors.Wrap(err, "private fetcher query")
	}
	v, tm, err := c.publicFetcher.Query(ctx, path, query)
	return v, tm, errors.Wrap(err, "public fetcher query")
}

func (c *compositeFetcher) List(ctx context.Context, path string) (versions []string, err error) {
	if c.matcher.IsPrivate(path) {
		v, err := c.privateFetcher.List(ctx, path)
		return v, errors.Wrap(err, "private fetcher list")
	}
	v, err := c.publicFetcher.List(ctx, path)
	return v, errors.Wrap(err, "public fetcher list")
}

func (c *compositeFetcher) Download(ctx context.Context, path, version string) (info, mod, zip io.ReadSeekCloser, err error) {
	if c.matcher.IsPrivate(path) {
		i, m, z, err := c.privateFetcher.Download(ctx, path, version)
		return i, m, z, errors.Wrap(err, "private fetcher download")
	}
	i, m, z, err := c.publicFetcher.Download(ctx, path, version)
	return i, m, z, errors.Wrap(err, "public fetcher download")
}
