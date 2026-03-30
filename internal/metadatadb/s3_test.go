package metadatadb_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func TestS3Backend(t *testing.T) {
	bucket := s3clienttest.Start(t)

	metadatadbtest.Suite(t, func(t *testing.T, n int) []metadatadb.Backend {
		t.Helper()
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		backends := make([]metadatadb.Backend, n)
		for i := range backends {
			b, err := metadatadb.NewS3Backend(ctx, s3client.ClientProvider(func() (*minio.Client, error) { return s3clienttest.Client(t), nil }), metadatadb.S3BackendConfig{
				Bucket:       bucket,
				LockTTL:      5 * time.Second,
				SyncInterval: time.Hour,
			})
			assert.NoError(t, err)
			backends[i] = b
		}
		return backends
	})
}

func TestS3BackendSoak(t *testing.T) {
	bucket := s3clienttest.Start(t)
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

	b, err := metadatadb.NewS3Backend(ctx, s3client.ClientProvider(func() (*minio.Client, error) { return s3clienttest.Client(t), nil }), metadatadb.S3BackendConfig{
		Bucket:       bucket,
		LockTTL:      5 * time.Second,
		SyncInterval: time.Hour,
	})
	assert.NoError(t, err)

	metadatadbtest.Soak(t, b, metadatadbtest.SoakConfig{
		Duration:    5 * time.Second,
		Concurrency: 4,
		NumKeys:     10,
	})
}
