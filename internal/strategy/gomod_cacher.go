package strategy

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/textproto"
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
	key := cache.NewKey(name)

	// Determine Content-Type from the file extension
	contentType := g.getContentType(name)

	headers := make(textproto.MIMEHeader)
	headers.Set("Content-Type", contentType)

	wc, err := g.cache.Create(ctx, key, headers, 0)
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

func (g *goproxyCacher) getContentType(name string) string {
	switch {
	case strings.HasSuffix(name, ".info"):
		return "application/json"
	case strings.HasSuffix(name, ".mod"):
		return "text/plain; charset=utf-8"
	case strings.HasSuffix(name, ".zip"):
		return "application/zip"
	case strings.HasSuffix(name, "/@v/list"):
		return "text/plain; charset=utf-8"
	case strings.HasSuffix(name, "/@latest"):
		return "application/json"
	default:
		return "application/octet-stream"
	}
}
