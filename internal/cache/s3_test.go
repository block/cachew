package cache_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func newS3Cache(t *testing.T, bucket string) cache.Cache {
	t.Helper()
	s3clienttest.CleanBucket(t, bucket)
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint: s3clienttest.Addr,
		UseSSL:   false,
	})

	c, err := cache.NewS3(ctx, cache.S3Config{
		Bucket:           bucket,
		MaxTTL:           500 * time.Millisecond,
		UploadPartSizeMB: 16,
	}, clientProvider)
	assert.NoError(t, err)
	return c
}

// TestS3Cache tests the S3 cache implementation using MinIO in Docker.
func TestS3Cache(t *testing.T) {
	bucket := s3clienttest.Start(t)
	cachetest.Suite(t, func(t *testing.T) cache.Cache { return newS3Cache(t, bucket) })
}

// TestS3ContextCancellationAbortsUpload verifies that cancelling the context before
// closing the writer aborts the S3 upload and does not leave any object behind.
// This is the mechanism snapshot.CreatePaths uses to prevent partial/corrupt uploads.
func TestS3ContextCancellationAbortsUpload(t *testing.T) {
	bucket := s3clienttest.Start(t)
	c := newS3Cache(t, bucket)
	defer c.Close()

	key := cache.NewKey("aborted-upload")

	ctx, cancel := context.WithCancelCause(t.Context())

	w, err := c.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)

	// Write some data so this isn't just a 0-byte edge case.
	_, err = w.Write([]byte("partial data that should not be persisted"))
	assert.NoError(t, err)

	// Cancel the context before closing, simulating an archive failure.
	cancel(errors.New("archive failed"))
	err = w.Close()
	assert.Error(t, err)

	// The object must not be retrievable.
	_, _, err = c.Open(t.Context(), key)
	assert.IsError(t, err, os.ErrNotExist)
}

func TestS3CacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	bucket := s3clienttest.Start(t)
	s3clienttest.CleanBucket(t, bucket)
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint: s3clienttest.Addr,
		UseSSL:   false,
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
