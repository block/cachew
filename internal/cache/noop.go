package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"
)

// noOpCache is a cache implementation that doesn't cache anything.
// It always returns cache misses and discards writes.
// Useful for pass-through handlers that shouldn't cache.
type noOpCache struct{}

// NoOpCache returns a cache that doesn't cache anything.
// All Open() calls return os.ErrNotExist (cache miss).
// All Create() calls return a writer that discards data.
func NoOpCache() Cache {
	return &noOpCache{}
}

func (n *noOpCache) String() string { return "noop" }

func (n *noOpCache) Stat(_ context.Context, _ string, _ Key) (http.Header, error) {
	return nil, os.ErrNotExist
}

func (n *noOpCache) Open(_ context.Context, _ string, _ Key) (io.ReadCloser, http.Header, error) {
	return nil, nil, os.ErrNotExist
}

func (n *noOpCache) Create(_ context.Context, _ string, _ Key, _ http.Header, _ time.Duration) (io.WriteCloser, error) {
	// Return a discard writer that does nothing
	return &noOpWriter{}, nil
}

func (n *noOpCache) Delete(_ context.Context, _ string, _ Key) error {
	return nil
}

func (n *noOpCache) Stats(_ context.Context) (Stats, error) {
	return Stats{}, ErrStatsUnavailable
}

func (n *noOpCache) Close() error {
	return nil
}

// noOpWriter is a writer that discards all data.
type noOpWriter struct{}

func (n *noOpWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (n *noOpWriter) Close() error {
	return nil
}

var _ Cache = (*noOpCache)(nil)
var _ io.WriteCloser = (*noOpWriter)(nil)
