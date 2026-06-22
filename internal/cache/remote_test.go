package cache_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

// countingHandler counts GET and HEAD requests to object endpoints.
type countingHandler struct {
	next        http.Handler
	mu          sync.Mutex
	gets, heads int
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.Path, "/object/") {
		h.mu.Lock()
		switch r.Method {
		case http.MethodGet:
			h.gets++
		case http.MethodHead:
			h.heads++
		}
		h.mu.Unlock()
	}
	h.next.ServeHTTP(w, r)
}

func TestRemoteCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		ctx := t.Context()
		_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
		memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{
			MaxTTL: 3 * time.Second,
		})
		assert.NoError(t, err)
		t.Cleanup(func() { memCache.Close() })

		mux := http.NewServeMux()
		_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
		assert.NoError(t, err)
		ts := httptest.NewServer(mux)
		t.Cleanup(ts.Close)

		client := cache.NewRemote(ts.URL, nil)
		return client.Namespace("test")
	})
}

// TestRemoteSeekIssuesSingleGet verifies the seekable-Open contract: a HEAD
// obtains the size, seeks are free (no I/O), and the first Read issues exactly
// one ranged GET from the seeked offset.
func TestRemoteSeekIssuesSingleGet(t *testing.T) {
	ctx := t.Context()
	_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Minute})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)
	counter := &countingHandler{next: mux}
	ts := httptest.NewServer(counter)
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, nil).Namespace("test")
	t.Cleanup(func() { cl.Close() })

	content := []byte("0123456789abcdefghij") // 20 bytes
	key := cache.NewKey("ranged")
	w, err := cl.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	// Open issues a HEAD for the size; no body fetched yet.
	rc, _, err := cl.Open(ctx, key)
	assert.NoError(t, err)
	t.Cleanup(func() { rc.Close() })
	assert.Equal(t, 1, counter.heads)
	assert.Equal(t, 0, counter.gets)

	// Seeks are free — still no GET.
	off, err := rc.Seek(10, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), off)
	assert.Equal(t, 0, counter.gets)

	// The first Read issues a single ranged GET from the seeked offset.
	got, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.Equal(t, content[10:], got)
	assert.Equal(t, 1, counter.gets)
}

func TestRemoteCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	ctx := t.Context()
	_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{
		LimitMB: 50,
		MaxTTL:  10 * time.Minute,
	})
	assert.NoError(t, err)
	defer memCache.Close()

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := cache.NewRemote(ts.URL, nil).Namespace("test")
	defer client.Close()

	cachetest.Soak(t, client, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      4,
		TTL:              5 * time.Minute,
	})
}
