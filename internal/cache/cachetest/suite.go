package cachetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"strconv"
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

	t.Run("NamespaceIsolation", func(t *testing.T) {
		testNamespaceIsolation(t, newCache(t))
	})

	t.Run("ListNamespaces", func(t *testing.T) {
		testListNamespaces(t, newCache(t))
	})

	t.Run("NamespaceDelete", func(t *testing.T) {
		testNamespaceDelete(t, newCache(t))
	})

	t.Run("ETag", func(t *testing.T) {
		testETag(t, newCache(t))
	})

	t.Run("Conditional", func(t *testing.T) {
		testConditional(t, newCache(t))
	})

	t.Run("Range", func(t *testing.T) {
		testRange(t, newCache(t))
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

func testETag(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	content := []byte("hello etag world")
	key := cache.NewKey("test-etag")

	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	sum := sha256.Sum256(content)
	expectedETag := `"` + hex.EncodeToString(sum[:]) + `"`

	// Verify ETag from Open
	reader, openHeaders, err := c.Open(ctx, key)
	assert.NoError(t, err)
	defer reader.Close()
	assert.Equal(t, expectedETag, openHeaders.Get("ETag"))

	// Verify ETag from Stat is consistent
	statHeaders, err := c.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, expectedETag, statHeaders.Get("ETag"))
}

// testConditional verifies that Open and Stat honour If-Match / If-None-Match
// preconditions against the stored ETag, returning the unified sentinel errors.
func testConditional(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	content := []byte("conditional content")
	key := cache.NewKey("test-conditional")

	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	sum := sha256.Sum256(content)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`

	t.Run("IfNoneMatchHitReturnsNotModified", func(t *testing.T) {
		_, headers, err := c.Open(ctx, key, cache.IfNoneMatch(etag))
		assert.IsError(t, err, cache.ErrNotModified)
		assert.Equal(t, etag, headers.Get("ETag")) // headers surfaced for the 304

		headers, err = c.Stat(ctx, key, cache.IfNoneMatch(etag))
		assert.IsError(t, err, cache.ErrNotModified)
		assert.Equal(t, etag, headers.Get("ETag"))
	})

	t.Run("IfNoneMatchMissServesBody", func(t *testing.T) {
		reader, _, err := c.Open(ctx, key, cache.IfNoneMatch(`"other"`))
		assert.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, content, data)
	})

	t.Run("IfMatchHitServesBody", func(t *testing.T) {
		reader, _, err := c.Open(ctx, key, cache.IfMatch(etag))
		assert.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, content, data)
	})

	t.Run("IfMatchMissReturnsPreconditionFailed", func(t *testing.T) {
		_, _, err := c.Open(ctx, key, cache.IfMatch(`"other"`))
		assert.IsError(t, err, cache.ErrPreconditionFailed)

		_, err = c.Stat(ctx, key, cache.IfMatch(`"other"`))
		assert.IsError(t, err, cache.ErrPreconditionFailed)
	})
}

// testRange verifies that Open honours a single byte range, sets Content-Range
// and a range-sized Content-Length, returns ErrRangeNotSatisfiable for an
// out-of-bounds range, and that Stat ignores Range.
func testRange(t *testing.T, c cache.Cache) {
	defer c.Close()
	ctx := t.Context()

	content := []byte("0123456789")
	key := cache.NewKey("test-range")

	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	t.Run("PartialContent", func(t *testing.T) {
		reader, headers, err := c.Open(ctx, key, cache.Range("bytes=2-5"))
		assert.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, []byte("2345"), data)
		assert.Equal(t, "bytes 2-5/10", headers.Get("Content-Range"))
		assert.Equal(t, "4", headers.Get("Content-Length"))
	})

	t.Run("Suffix", func(t *testing.T) {
		reader, headers, err := c.Open(ctx, key, cache.Range("bytes=-3"))
		assert.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, []byte("789"), data)
		assert.Equal(t, "bytes 7-9/10", headers.Get("Content-Range"))
	})

	t.Run("NotSatisfiable", func(t *testing.T) {
		_, headers, err := c.Open(ctx, key, cache.Range("bytes=20-30"))
		assert.IsError(t, err, cache.ErrRangeNotSatisfiable)
		assert.Equal(t, "bytes */10", headers.Get("Content-Range"))
	})

	t.Run("IfRangeMismatchServesFull", func(t *testing.T) {
		reader, headers, err := c.Open(ctx, key, cache.Range("bytes=2-5"), cache.IfRange(`"stale"`))
		assert.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, content, data)
		assert.Equal(t, "", headers.Get("Content-Range"))
	})

	t.Run("StatIgnoresRange", func(t *testing.T) {
		headers, err := c.Stat(ctx, key, cache.Range("bytes=2-5"))
		assert.NoError(t, err)
		assert.Equal(t, "", headers.Get("Content-Range"))
		assert.Equal(t, "10", headers.Get("Content-Length"))
	})
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
