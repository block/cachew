package cachetest

import (
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
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

	t.Run("LastModified", func(t *testing.T) {
		testLastModified(t, newCache(t))
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

	writer, err := c.Create(ctx, key, nil, time.Millisecond*250)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	time.Sleep(500 * time.Millisecond)

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
	headers := http.Header{
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

	// Verify headers that were passed in are present
	assert.Equal(t, "application/json", returnedHeaders.Get("Content-Type"))
	assert.Equal(t, "max-age=3600", returnedHeaders.Get("Cache-Control"))
	assert.Equal(t, "custom-value", returnedHeaders.Get("X-Custom-Field"))

	// Verify Last-Modified header was added
	assert.NotZero(t, returnedHeaders.Get("Last-Modified"))
}

func testContextCancellation(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	// Create a cancellable context
	cancelledCtx, cancel := context.WithCancel(ctx)

	// Create an object with the cancellable context
	key := cache.NewKey("test-cancelled")
	writer, err := c.Create(cancelledCtx, key, http.Header{}, time.Hour)
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

func testLastModified(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-last-modified")

	// Create an object without specifying Last-Modified
	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	// Open and verify Last-Modified header is present
	reader, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()

	lastModified := headers.Get("Last-Modified")
	assert.NotZero(t, lastModified, "Last-Modified header should be set")

	// Verify it can be parsed as an HTTP date
	parsedTime, err := http.ParseTime(lastModified)
	assert.NoError(t, err)
	assert.True(t, parsedTime.Before(time.Now().Add(time.Second)), "Last-Modified should be in the past")

	// Test with explicit Last-Modified header
	key2 := cache.NewKey("test-last-modified-explicit")
	explicitTime := time.Date(2023, 1, 15, 12, 30, 0, 0, time.UTC)
	explicitHeaders := http.Header{
		"Last-Modified": []string{explicitTime.Format(http.TimeFormat)},
	}

	writer2, err := c.Create(ctx, key2, explicitHeaders, time.Hour)
	assert.NoError(t, err)

	_, err = writer2.Write([]byte("test data 2"))
	assert.NoError(t, err)

	err = writer2.Close()
	assert.NoError(t, err)

	// Verify explicit Last-Modified is preserved
	reader2, headers2, err := c.Open(ctx, key2)
	assert.NoError(t, err)
	defer reader2.Close()

	assert.Equal(t, explicitTime.Format(http.TimeFormat), headers2.Get("Last-Modified"))
}
