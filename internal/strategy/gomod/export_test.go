package gomod

import (
	"context"
	"io"

	"github.com/block/cachew/internal/cache"
)

// TestableGoproxyCacher exposes goproxyCacher.Put for external tests.
type TestableGoproxyCacher struct {
	inner goproxyCacher
}

// NewTestableGoproxyCacher creates a testable goproxy cacher for the given cache.
func NewTestableGoproxyCacher(c cache.Cache) *TestableGoproxyCacher {
	return &TestableGoproxyCacher{inner: goproxyCacher{cache: c}}
}

// Put delegates to goproxyCacher.Put.
func (t *TestableGoproxyCacher) Put(ctx context.Context, name string, content io.ReadSeeker) error {
	return t.inner.Put(ctx, name, content)
}
