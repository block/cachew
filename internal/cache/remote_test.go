package cache_test

import (
	"bytes"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/strategy"
)

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

func TestRemoteInvalidateSkipsRemoteAuthoritativeTier(t *testing.T) {
	ctx := t.Context()
	_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})

	lower, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { lower.Close() })

	authoritative, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { authoritative.Close() })

	tiered := cache.MaybeNewTiered(ctx, []cache.Cache{lower, authoritative}, metadatadb.New(ctx, metadatadb.NewMemoryBackend()))
	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, tiered, mux)
	assert.NoError(t, err)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	remote := cache.NewRemote(ts.URL, nil).Namespace("test")
	t.Cleanup(func() { remote.Close() })

	key := cache.NewKey("remote-invalidate")
	content := []byte("authoritative content")
	w, err := remote.Create(ctx, key, nil, time.Minute)
	assert.NoError(t, err)
	_, err = w.Write(content)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	assert.NoError(t, remote.Invalidate(ctx, key))

	_, _, err = lower.Namespace("test").Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	r, _, err := authoritative.Namespace("test").Open(ctx, key)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, content, data)
}

// countingHandler wraps an http.Handler, counting HEAD requests and ranged
// GETs, and how many ranged GETs lack an If-Match revision pin.
type countingHandler struct {
	http.Handler
	heads        atomic.Int32
	rangedGets   atomic.Int32
	unpinnedGets atomic.Int32
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodHead:
		h.heads.Add(1)
	case r.Method == http.MethodGet && r.Header.Get("Range") != "":
		h.rangedGets.Add(1)
		if r.Header.Get("If-Match") == "" {
			h.unpinnedGets.Add(1)
		}
	}
	h.Handler.ServeHTTP(w, r)
}

func newCountingRemote(t *testing.T) (cache.Cache, *countingHandler) {
	t.Helper()
	ctx := t.Context()
	_, ctx = logging.Configure(ctx, logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{LimitMB: 64, MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { memCache.Close() })

	mux := http.NewServeMux()
	_, err = strategy.NewAPIV1(ctx, struct{}{}, memCache, mux)
	assert.NoError(t, err)
	counting := &countingHandler{Handler: mux}
	ts := httptest.NewServer(counting)
	t.Cleanup(ts.Close)

	remote := cache.NewRemote(ts.URL, nil).Namespace("test")
	t.Cleanup(func() { remote.Close() })
	return remote, counting
}

func TestRemoteLargeRangedReadFansOut(t *testing.T) {
	ctx := t.Context()
	remote, counting := newCountingRemote(t)

	data := make([]byte, 12<<20)
	_, err := rand.New(rand.NewSource(42)).Read(data)
	assert.NoError(t, err)

	key := cache.NewKey("remote-fan-out")
	w, err := remote.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, headers, err := remote.Open(ctx, key, cache.Range(1<<20, 11<<20))
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	assert.True(t, bytes.Equal(data[1<<20:11<<20], got), "reassembled range differs from original")
	assert.Equal(t, "bytes 1048576-11534335/12582912", headers.Get("Content-Range"))
	assert.Equal(t, "10485760", headers.Get("Content-Length"))
	assert.True(t, counting.rangedGets.Load() >= 3,
		"expected sub-range fan-out, got %d ranged GETs", counting.rangedGets.Load())
	assert.Equal(t, int32(0), counting.unpinnedGets.Load(), "every sub-range request must carry If-Match")
}

func TestRemoteSmallRangedReadStaysSingleStream(t *testing.T) {
	ctx := t.Context()
	remote, counting := newCountingRemote(t)

	data := bytes.Repeat([]byte("abcdefgh"), 1<<20/8)
	key := cache.NewKey("remote-small-range")
	w, err := remote.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, _, err := remote.Open(ctx, key, cache.Range(16, 4096))
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	assert.True(t, bytes.Equal(data[16:4096], got), "range body differs from original")
	assert.Equal(t, int32(0), counting.heads.Load(), "small ranges must not pay a Stat")
	assert.Equal(t, int32(1), counting.rangedGets.Load(), "small ranges must use a single request")
}

func TestRemoteLargeRangedReadIfRangeMissServesFullBody(t *testing.T) {
	ctx := t.Context()
	remote, counting := newCountingRemote(t)

	data := make([]byte, 9<<20)
	_, err := rand.New(rand.NewSource(7)).Read(data)
	assert.NoError(t, err)

	key := cache.NewKey("remote-if-range-miss")
	w, err := remote.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, _, err := remote.Open(ctx, key, cache.Range(0, 9<<20), cache.IfRange(`"other-revision"`))
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	assert.True(t, bytes.Equal(data, got), "If-Range miss must serve the full representation")
	assert.Equal(t, int32(1), counting.rangedGets.Load(), "an unpinnable range must degrade to a single stream")
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
