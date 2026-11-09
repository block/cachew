package cache_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestOpenPending(t *testing.T) {
	newDisk := func(ctx context.Context, t *testing.T) cache.Cache {
		t.Helper()
		c, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), MaxTTL: time.Hour})
		assert.NoError(t, err)
		return c
	}
	newMemory := func(ctx context.Context, t *testing.T) cache.Cache {
		t.Helper()
		c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
		assert.NoError(t, err)
		return c
	}
	tests := []struct {
		name            string
		newCache        func(ctx context.Context, t *testing.T) cache.Cache
		wantUnsupported bool
	}{
		{name: "Disk", newCache: newDisk},
		{name: "TieredMemoryOverDisk", newCache: func(ctx context.Context, t *testing.T) cache.Cache {
			return newTiered(ctx, newMemory(ctx, t), newDisk(ctx, t))
		}},
		{name: "TieredMemoryOnly", newCache: func(ctx context.Context, t *testing.T) cache.Cache {
			return newTiered(ctx, newMemory(ctx, t), newMemory(ctx, t))
		}, wantUnsupported: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
			c := tt.newCache(ctx, t)
			defer c.Close()
			w, err := c.Create(ctx, cache.NewKey("pending"), http.Header{}, time.Hour)
			assert.NoError(t, err)
			pending, ok := w.(cache.PendingReader)
			assert.True(t, ok)

			_, err = w.Write([]byte("abc"))
			assert.NoError(t, err)
			r, err := pending.OpenPending()
			if tt.wantUnsupported {
				assert.IsError(t, err, cache.ErrPendingReadUnsupported)
				_ = w.Abort(err)
				return
			}
			assert.NoError(t, err)
			defer r.Close()
			_, err = w.Write([]byte("def"))
			assert.NoError(t, err)

			got, err := io.ReadAll(io.NewSectionReader(r, 0, 6))
			assert.NoError(t, err)
			assert.Equal(t, "abcdef", string(got))

			assert.NoError(t, w.Close())
			got, err = io.ReadAll(io.NewSectionReader(r, 0, 6))
			assert.NoError(t, err)
			assert.Equal(t, "abcdef", string(got))
			_, err = pending.OpenPending()
			assert.Error(t, err)
		})
	}
}
