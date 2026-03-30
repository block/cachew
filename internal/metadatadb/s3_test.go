package metadatadb_test

import (
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func TestS3Backend(t *testing.T) {
	bucket := s3clienttest.Start(t)

	metadatadbtest.Suite(t, func(t *testing.T, n int) []metadatadb.Backend {
		t.Helper()
		backends := make([]metadatadb.Backend, n)
		for i := range backends {
			b, err := metadatadb.NewS3Backend(t.Context(), metadatadb.S3BackendConfig{
				Client:       s3clienttest.Client(t),
				Bucket:       bucket,
				Prefix:       "_meta-" + t.Name(),
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

	b, err := metadatadb.NewS3Backend(t.Context(), metadatadb.S3BackendConfig{
		Client:       s3clienttest.Client(t),
		Bucket:       bucket,
		Prefix:       "_meta-soak",
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
