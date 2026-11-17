package gomod

import (
	"context"
	"io"
	"path"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"github.com/goproxy/goproxy"
)

// CompositeFetcher routes module requests to either public or private fetchers based on module path patterns.
type CompositeFetcher struct {
	publicFetcher  goproxy.Fetcher
	privateFetcher goproxy.Fetcher
	patterns       []string
}

// NewCompositeFetcher returns a CompositeFetcher that uses privateFetcher for module paths matching patterns.
func NewCompositeFetcher(
	publicFetcher goproxy.Fetcher,
	privateFetcher goproxy.Fetcher,
	patterns []string,
) *CompositeFetcher {
	return &CompositeFetcher{
		publicFetcher:  publicFetcher,
		privateFetcher: privateFetcher,
		patterns:       patterns,
	}
}

// IsPrivate reports whether modulePath matches a pattern as a glob, exactly, or as a path prefix.
func (c *CompositeFetcher) IsPrivate(modulePath string) bool {
	for _, pattern := range c.patterns {
		matched, err := path.Match(pattern, modulePath)
		if err == nil && matched {
			return true
		}

		if strings.HasPrefix(modulePath, pattern+"/") || modulePath == pattern {
			return true
		}
	}

	return false
}

// Query implements goproxy.Fetcher.
func (c *CompositeFetcher) Query(ctx context.Context, path, query string) (version string, t time.Time, err error) {
	if c.IsPrivate(path) {
		v, tm, err := c.privateFetcher.Query(ctx, path, query)
		return v, tm, errors.Wrap(err, "private fetcher query")
	}
	v, tm, err := c.publicFetcher.Query(ctx, path, query)
	return v, tm, errors.Wrap(err, "public fetcher query")
}

// List implements goproxy.Fetcher.
func (c *CompositeFetcher) List(ctx context.Context, path string) (versions []string, err error) {
	if c.IsPrivate(path) {
		v, err := c.privateFetcher.List(ctx, path)
		return v, errors.Wrap(err, "private fetcher list")
	}
	v, err := c.publicFetcher.List(ctx, path)
	return v, errors.Wrap(err, "public fetcher list")
}

// Download implements goproxy.Fetcher.
func (c *CompositeFetcher) Download(ctx context.Context, path, version string) (info, mod, zip io.ReadSeekCloser, err error) {
	if c.IsPrivate(path) {
		i, m, z, err := c.privateFetcher.Download(ctx, path, version)
		return i, m, z, errors.Wrap(err, "private fetcher download")
	}
	i, m, z, err := c.publicFetcher.Download(ctx, path, version)
	return i, m, z, errors.Wrap(err, "public fetcher download")
}
