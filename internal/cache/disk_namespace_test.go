package cache_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestDiskNamespaceIsolation(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	// Create base cache
	baseCache, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:   dir,
		MaxTTL: time.Hour,
	})
	assert.NoError(t, err)
	defer baseCache.Close()

	// Create namespace views
	gitCache := baseCache.Namespace("git")
	gomodCache := baseCache.Namespace("gomod")

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
	gitData := make([]byte, 8)
	n, _ := r.Read(gitData)
	assert.Equal(t, "git data", string(gitData[:n]))
	assert.NoError(t, r.Close())

	r, _, err = gomodCache.Open(ctx, key)
	assert.NoError(t, err)
	gomodData := make([]byte, 10)
	n, _ = r.Read(gomodData)
	assert.Equal(t, "gomod data", string(gomodData[:n]))
	assert.NoError(t, r.Close())
}

func TestDiskListNamespaces(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	baseCache, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:   dir,
		MaxTTL: time.Hour,
	})
	assert.NoError(t, err)
	defer baseCache.Close()

	// Initially no namespaces
	namespaces, err := baseCache.ListNamespaces(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(namespaces))

	// Create entries in different namespaces
	gitCache := baseCache.Namespace("git")
	gomodCache := baseCache.Namespace("gomod")
	hermitCache := baseCache.Namespace("hermit")

	for i, c := range []cache.Cache{gitCache, gomodCache, hermitCache} {
		w, err := c.Create(ctx, cache.NewKey(string(rune(i))), nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write([]byte("data"))
		assert.NoError(t, err)
		assert.NoError(t, w.Close())
	}

	// Verify all namespaces are listed
	namespaces, err = baseCache.ListNamespaces(ctx)
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

func TestDiskNamespaceDelete(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	baseCache, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:   dir,
		MaxTTL: time.Hour,
	})
	assert.NoError(t, err)
	defer baseCache.Close()

	gitCache := baseCache.Namespace("git")
	gomodCache := baseCache.Namespace("gomod")

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
	assert.Error(t, err)

	// Verify gomod entry still exists
	r, _, err := gomodCache.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
}
