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
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

// TestS3Cache tests the S3 cache implementation using MinIO in Docker.
func TestS3Cache(t *testing.T) {
	bucket := s3clienttest.Start(t)

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		s3clienttest.CleanBucket(t, bucket)
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

		clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
			Endpoint:      s3clienttest.Addr,
			Region:        "",
			UseSSL:        false,
			SkipSSLVerify: false,
		})

		c, err := cache.NewS3(ctx, cache.S3Config{
			Bucket:           bucket,
			MaxTTL:           500 * time.Millisecond,
			UploadPartSizeMB: 16,
		}, clientProvider)
		assert.NoError(t, err)
		return c
	})
}

func TestS3CacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	bucket := s3clienttest.Start(t)

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint:      s3clienttest.Addr,
		Region:        "",
		UseSSL:        false,
		SkipSSLVerify: false,
	})

	c, err := cache.NewS3(ctx, cache.S3Config{
		Bucket:           bucket,
		MaxTTL:           10 * time.Minute,
		UploadPartSizeMB: 16,
	}, clientProvider)
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
