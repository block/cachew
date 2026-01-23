package strategy

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/block/cachew/internal/cache"
)

type goproxyCacher struct {
	cache cache.Cache
}

func (g *goproxyCacher) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	key := cache.NewKey(name)

	rc, _, err := g.cache.Open(ctx, key)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	return rc, nil
}

func (g *goproxyCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	if strings.HasSuffix(name, "/@v/list") || strings.HasSuffix(name, "/@latest") {
		return nil
	}

	key := cache.NewKey(name)

	wc, err := g.cache.Create(ctx, key, nil, 0)
	if err != nil {
		return fmt.Errorf("create cache entry: %w", err)
	}
	defer wc.Close()

	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	if _, err := io.Copy(wc, content); err != nil {
		return fmt.Errorf("write to cache: %w", err)
	}

	if err := wc.Close(); err != nil {
		return fmt.Errorf("close cache entry: %w", err)
	}

	return nil
}
