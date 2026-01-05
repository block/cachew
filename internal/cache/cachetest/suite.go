package cachetest

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/sfptc/internal/cache"
)

// Suite runs a comprehensive test suite against a cache.Cache implementation.
// All cache implementations should pass this test suite to ensure consistent semantics.
func Suite(t *testing.T, newCache func(t *testing.T) cache.Cache) {
	t.Run("CreateAndOpen", func(t *testing.T) {
		testCreateAndOpen(t, newCache(t))
	})

	t.Run("NotFound", func(t *testing.T) {
		testNotFound(t, newCache(t))
	})

	t.Run("Expiration", func(t *testing.T) {
		testExpiration(t, newCache(t))
	})

	t.Run("DefaultTTL", func(t *testing.T) {
		testDefaultTTL(t, newCache(t))
	})

	t.Run("Delete", func(t *testing.T) {
		testDelete(t, newCache(t))
	})

	t.Run("MultipleWrites", func(t *testing.T) {
		testMultipleWrites(t, newCache(t))
	})

	t.Run("NotAvailableUntilClosed", func(t *testing.T) {
		testNotAvailableUntilClosed(t, newCache(t))
	})
}

func testCreateAndOpen(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("hello world"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func testNotFound(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("nonexistent")

	_, err := c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testExpiration(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, 10*time.Millisecond)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	time.Sleep(20 * time.Millisecond)

	_, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testDefaultTTL(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, 0)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())
}

func testDelete(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	err = c.Delete(ctx, key)
	assert.NoError(t, err)

	_, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testMultipleWrites(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("hello "))
	assert.NoError(t, err)

	_, err = writer.Write([]byte("world"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(data))
}

func testNotAvailableUntilClosed(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	_, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	err = writer.Close()
	assert.NoError(t, err)

	_, err = c.Open(ctx, key)
	assert.NoError(t, err)
}
