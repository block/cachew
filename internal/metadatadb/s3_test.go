package metadatadb_test

import (
	"testing"
	"time"

	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func TestS3Backend(t *testing.T) {
	bucket := s3clienttest.Start(t)

	metadatadbtest.Suite(t, func(t *testing.T) metadatadb.Backend {
		t.Helper()
		return metadatadb.NewS3Backend(metadatadb.S3BackendConfig{
			Client:  s3clienttest.Client(t),
			Bucket:  bucket,
			Prefix:  "_meta-" + t.Name(),
			LockTTL: 5 * time.Second,
		})
	})
}

func TestS3BackendSoak(t *testing.T) {
	bucket := s3clienttest.Start(t)

	metadatadbtest.Soak(t, metadatadb.NewS3Backend(metadatadb.S3BackendConfig{
		Client:  s3clienttest.Client(t),
		Bucket:  bucket,
		Prefix:  "_meta-soak",
		LockTTL: 5 * time.Second,
	}), metadatadbtest.SoakConfig{
		Duration:    5 * time.Second,
		Concurrency: 4,
		NumKeys:     10,
	})
}
