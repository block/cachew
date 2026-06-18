package cachetest

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

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

	t.Run("ContentLength", func(t *testing.T) {
		testContentLength(t, newCache(t))
	})

	t.Run("ETag", func(t *testing.T) {
		testETag(t, newCache(t))
	})

	t.Run("ETagConsistency", func(t *testing.T) {
		testETagConsistency(t, newCache(t))
	})

	t.Run("OpenIfNoneMatch", func(t *testing.T) {
		testOpenIfNoneMatch(t, newCache(t))
	})

	t.Run("OpenIfMatch", func(t *testing.T) {
		testOpenIfMatch(t, newCache(t))
	})

	t.Run("NamespaceIsolation", func(t *testing.T) {
		testNamespaceIsolation(t, newCache(t))
	})

	t.Run("ListNamespaces", func(t *testing.T) {
		testListNamespaces(t, newCache(t))
	})

	t.Run("NamespaceDelete", func(t *testing.T) {
		testNamespaceDelete(t, newCache(t))
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

	writer, err := c.Create(ctx, key, nil, 2*time.Second)
	assert.NoError(t, err)

	_, err = writer.Write([]byte("test data"))
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	reader, _, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	time.Sleep(4 * time.Second)

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

func testContentLength(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	content := []byte("hello content length")
	key := cache.NewKey("test-content-length")

	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	reader, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()
	assert.Equal(t, strconv.Itoa(len(content)), headers.Get("Content-Length"))

	statHeaders, err := c.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(len(content)), statHeaders.Get("Content-Length"))
}

func testETag(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-etag")

	writer, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write([]byte("hello world"))
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	reader, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()

	etag := headers.Get("ETag")
	assert.NotZero(t, etag, "ETag header should be set")
	assert.True(t, strings.HasPrefix(etag, `"sha256:`), "ETag should be a sha256-based strong ETag, got: %s", etag)
	assert.True(t, strings.HasSuffix(etag, `"`), "ETag should end with a quote")

	statHeaders, err := c.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, etag, statHeaders.Get("ETag"))
}

func testETagConsistency(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	content := []byte("identical content for etag consistency")

	key1 := cache.NewKey("etag-consistency-1")
	w1, err := c.Create(ctx, key1, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w1.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w1.Close())

	key2 := cache.NewKey("etag-consistency-2")
	w2, err := c.Create(ctx, key2, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w2.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w2.Close())

	_, h1, err := c.Open(ctx, key1)
	assert.NoError(t, err)
	_, h2, err := c.Open(ctx, key2)
	assert.NoError(t, err)

	assert.Equal(t, h1.Get("ETag"), h2.Get("ETag"))
}

func testOpenIfNoneMatch(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-if-none-match")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("some content"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Get the ETag
	_, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	etag := headers.Get("ETag")
	assert.NotZero(t, etag)

	// If-None-Match with matching ETag → ErrNotModified
	_, retHeaders, err := c.Open(ctx, key, cache.WithIfNoneMatch(etag))
	assert.IsError(t, err, cache.ErrNotModified)
	assert.Equal(t, etag, retHeaders.Get("ETag"))

	// If-None-Match with non-matching ETag → success
	reader, _, err := c.Open(ctx, key, cache.WithIfNoneMatch(`"wrong"`))
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	// If-None-Match on non-existent key → ErrNotExist (not ErrNotModified)
	_, _, err = c.Open(ctx, cache.NewKey("nonexistent"), cache.WithIfNoneMatch(etag))
	assert.IsError(t, err, os.ErrNotExist)
}

func testOpenIfMatch(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	key := cache.NewKey("test-if-match")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("some content"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Get the ETag
	_, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	etag := headers.Get("ETag")
	assert.NotZero(t, etag)

	// If-Match with matching ETag → success
	reader, _, err := c.Open(ctx, key, cache.WithIfMatch(etag))
	assert.NoError(t, err)
	assert.NoError(t, reader.Close())

	// If-Match with non-matching ETag → ErrPreconditionFailed
	_, retHeaders, err := c.Open(ctx, key, cache.WithIfMatch(`"wrong"`))
	assert.IsError(t, err, cache.ErrPreconditionFailed)
	assert.Equal(t, etag, retHeaders.Get("ETag"))

	// If-Match on non-existent key → ErrNotExist (not ErrPreconditionFailed)
	_, _, err = c.Open(ctx, cache.NewKey("nonexistent"), cache.WithIfMatch(etag))
	assert.IsError(t, err, os.ErrNotExist)
}

func testNamespaceIsolation(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	// Create namespace views
	gitCache := c.Namespace("git")
	gomodCache := c.Namespace("gomod")

	// Create entries in different namespaces with same key
	key := cache.NewKey("same-key")

	// Write to git namespace
	w, err := gitCache.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("git data"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Write to gomod namespace
	w, err = gomodCache.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("gomod data"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Verify isolation - each namespace returns its own data
	r, _, err := gitCache.Open(ctx, key)
	assert.NoError(t, err)
	gitData, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, "git data", string(gitData))
	assert.NoError(t, r.Close())

	r, _, err = gomodCache.Open(ctx, key)
	assert.NoError(t, err)
	gomodData, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, "gomod data", string(gomodData))
	assert.NoError(t, r.Close())
}

func testListNamespaces(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	// Initially no namespaces
	namespaces, err := c.ListNamespaces(ctx)
	if errors.Is(err, cache.ErrStatsUnavailable) {
		t.Skip("Cache does not support ListNamespaces")
	}
	assert.NoError(t, err)
	assert.Equal(t, 0, len(namespaces))

	// Create entries in different namespaces
	gitCache := c.Namespace("git")
	gomodCache := c.Namespace("gomod")
	hermitCache := c.Namespace("hermit")

	for i, cacheNS := range []cache.Cache{gitCache, gomodCache, hermitCache} {
		w, err := cacheNS.Create(ctx, cache.NewKey(string(rune('a'+i))), nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write([]byte("data"))
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
	}

	// Verify all namespaces are listed
	namespaces, err = c.ListNamespaces(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(namespaces))

	nsMap := make(map[string]bool)
	for _, ns := range namespaces {
		nsMap[ns] = true
	}
	assert.True(t, nsMap["git"])
	assert.True(t, nsMap["gomod"])
	assert.True(t, nsMap["hermit"])
}

func testNamespaceDelete(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	gitCache := c.Namespace("git")
	gomodCache := c.Namespace("gomod")

	key := cache.NewKey("test-key")

	// Create entry in git namespace
	w, err := gitCache.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("git data"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Create entry in gomod namespace
	w, err = gomodCache.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("gomod data"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Delete from git namespace
	err = gitCache.Delete(ctx, key)
	assert.NoError(t, err)

	// Verify git entry is gone
	_, _, err = gitCache.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	// Verify gomod entry still exists
	r, _, err := gomodCache.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}
