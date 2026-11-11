package cache_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestDiskCache(t *testing.T) {
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		dir := t.TempDir()
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
		c, err := cache.NewDisk(ctx, cache.DiskConfig{
			Root:   dir,
			MaxTTL: 3 * time.Second,
		})
		assert.NoError(t, err)
		return c
	})
}

func TestDiskCacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{
		Root:          dir,
		LimitMB:       50,
		MaxTTL:        10 * time.Minute,
		EvictInterval: time.Second,
	})
	assert.NoError(t, err)
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         time.Minute,
		NumObjects:       500,
		MaxObjectSize:    512 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      8,
		TTL:              5 * time.Minute,
	})
}

func TestDiskOpenRevisionAtomic(t *testing.T) {
	dir := t.TempDir()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{Root: dir, MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("revision-atomic")

	write := func(rev int) {
		body := bytes.Repeat([]byte{byte(rev)}, 4096+rev)
		sum := sha256.Sum256(body)
		raw := hex.EncodeToString(sum[:])
		w, err := c.Create(ctx, key, http.Header{}, time.Hour, cache.WithETag(raw))
		if err != nil {
			t.Errorf("create: %v", err)
			return
		}
		if _, err := w.Write(body); err != nil {
			t.Errorf("write: %v", errors.Join(err, w.Abort(err)))
			return
		}
		if err := w.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}

	write(0)

	const writers, readsPerReader, readers = 4, 500, 4
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range writers {
		wg.Go(func() {
			for rev := 1; ; rev++ {
				select {
				case <-stop:
					return
				default:
				}
				write(rev % 251)
			}
		})
	}

	for range readers {
		wg.Go(func() {
			for range readsPerReader {
				r, headers, err := c.Open(ctx, key)
				if err != nil {
					t.Errorf("open: %v", err)
					continue
				}
				body, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil {
					t.Errorf("read: %v", err)
					continue
				}
				sum := sha256.Sum256(body)
				want, err := cache.FormatETag(hex.EncodeToString(sum[:]))
				if err != nil {
					t.Errorf("format etag: %v", err)
					continue
				}
				if got := headers.Get(cache.ETagKey); got != want {
					t.Errorf("etag/body mismatch: header %s does not match body hash %s (spliced revision)", got, want)
				}
			}
		})
	}

	go func() {
		time.Sleep(time.Second)
		close(stop)
	}()
	wg.Wait()
}

// fileSinkWriter records what io.Copy hands to ReadFrom, as net/http does when
// it passes a body to the connection's sendfile path.
type fileSinkWriter struct {
	bytes.Buffer
	src io.Reader
}

func (w *fileSinkWriter) ReadFrom(r io.Reader) (int64, error) {
	w.src = r
	return w.Buffer.ReadFrom(r)
}

// TestDiskRangeReachesSendfile verifies a ranged disk read reaches ReadFrom as
// an *io.LimitedReader over the file, the only shape net's sendfile accepts.
func TestDiskRangeReachesSendfile(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("range-sendfile")
	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("0123456789abcdef"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, _, err := c.Open(ctx, key, cache.Range(4, 10))
	assert.NoError(t, err)

	var sink fileSinkWriter
	_, err = io.Copy(&sink, r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "456789", sink.String())
	limited, ok := sink.src.(*io.LimitedReader)
	assert.True(t, ok, "ReadFrom got %T", sink.src)
	_, ok = limited.R.(syscall.Conn)
	assert.True(t, ok, "LimitedReader wraps %T", limited.R)
}
