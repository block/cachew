package cachetest

import (
	"context"
	"io"
	"net/textproto"
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

	t.Run("Headers", func(t *testing.T) {
		testHeaders(t, newCache(t))
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		testContextCancellation(t, newCache(t))
	})
}

func testCreateAndOpen(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("hello world"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
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

	_, _, err := c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testExpiration(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, nil, 10*time.Millisecond)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	time.Sleep(20 * time.Millisecond)

	_, _, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testDefaultTTL(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())
}

func testDelete(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	err = c.Delete(ctx, key)
	assert.NoError(t, err)

	_, _, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

func testMultipleWrites(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key")

	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("hello "))
	assert.NoError(t, err)

	_, err = writer.Write([]byte("world"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
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

	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	_, _, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	err = writer.Close()
	assert.NoError(t, err)

	_, _, err = c.Open(ctx, key)
	assert.NoError(t, err)
}

func testHeaders(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-key-with-headers")

	// Create headers to store
	headers := textproto.MIMEHeader{
		"Content-Type":   []string{"application/json"},
		"Cache-Control":  []string{"max-age=3600"},
		"X-Custom-Field": []string{"custom-value"},
	}

	writer, err := c.Create(ctx, key, headers, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data with headers"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	// Open and verify headers are returned
	reader, returnedHeaders, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()

	// Verify the data
	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, "test data with headers", string(data))

	// Verify headers
	assert.Equal(t, headers, returnedHeaders)
}

func testContextCancellation(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	// Create a cancellable context
	cancelledCtx, cancel := context.WithCancel(ctx)

	// Create an object with the cancellable context
	key := cache.NewKey("test-cancelled")
	writer, err := c.Create(cancelledCtx, key, textproto.MIMEHeader{}, time.Hour)
	assert.NoError(t, err)

	// Write some data
	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	// Cancel the context before closing
	cancel()

	// Close should fail due to cancelled context
	err = writer.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancel")

	// Object should not be in cache
	_, _, err = c.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}
