package strategy

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"net/textproto"
	"strings"

	"github.com/block/cachew/internal/cache"
)

// goproxyCacher adapts cachew's cache.Cache interface to work with goproxy's Cacher interface.
// It handles the translation between goproxy's file-based caching model and cachew's
// HTTP-response-based caching model.
type goproxyCacher struct {
	cache cache.Cache
}

// Get retrieves cached content by name from cachew's cache.
// It returns fs.ErrNotExist if the content is not found, which goproxy uses
// as a signal to fetch from upstream.
func (g *goproxyCacher) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	// Hash the name to create a cache key that matches cachew's format
	key := cache.Key(sha256.Sum256([]byte(name)))

	// Try to open the cached content
	rc, _, err := g.cache.Open(ctx, key)
	if err != nil {
		// If the cache backend returns an error, treat it as not found
		// This ensures goproxy will fetch from upstream
		return nil, fs.ErrNotExist
	}

	return rc, nil
}

// Put stores content in cachew's cache.
func (g *goproxyCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	// Hash the name to create a cache key
	key := cache.Key(sha256.Sum256([]byte(name)))

	// Determine Content-Type from the file extension
	contentType := g.getContentType(name)

	// Create headers for the cached response
	headers := make(textproto.MIMEHeader)
	headers.Set("Content-Type", contentType)

	// Create the cache entry with zero TTL (cache handles TTL via its own config)
	wc, err := g.cache.Create(ctx, key, headers, 0)
	if err != nil {
		return fmt.Errorf("create cache entry: %w", err)
	}
	defer wc.Close()

	// Reset the seeker to the beginning
	if _, err := content.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	// Copy the content to the cache
	if _, err := io.Copy(wc, content); err != nil {
		return fmt.Errorf("write to cache: %w", err)
	}

	// Close the writer to commit the cache entry
	if err := wc.Close(); err != nil {
		return fmt.Errorf("close cache entry: %w", err)
	}

	return nil
}

// getContentType returns the appropriate Content-Type header based on the file extension.
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
