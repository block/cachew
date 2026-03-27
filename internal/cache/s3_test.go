package cache_test

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/minitest"
)

// TestS3Cache tests the S3 cache implementation using MinIO in Docker.
func TestS3Cache(t *testing.T) {
	minitest.Start(t)

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

		minitest.CleanBucket(t)

		c, err := cache.NewS3(ctx, cache.S3Config{
			Endpoint:         minitest.Addr,
			Bucket:           minitest.Bucket,
			Region:           "",
			UseSSL:           false,
			MaxTTL:           100 * time.Millisecond,
			UploadPartSizeMB: 16,
		})
		assert.NoError(t, err)
		return c
	})
}

func TestS3CacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	minitest.Start(t)

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

	minitest.CleanBucket(t)

	c, err := cache.NewS3(ctx, cache.S3Config{
		Endpoint:         minitest.Addr,
		Bucket:           minitest.Bucket,
		Region:           "",
		UseSSL:           false,
		MaxTTL:           10 * time.Minute,
		UploadPartSizeMB: 16,
	})
	assert.NoError(t, err)
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         30 * time.Second,
		NumObjects:       100,
		MaxObjectSize:    64 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      4,
		TTL:              5 * time.Minute,
	})
}
