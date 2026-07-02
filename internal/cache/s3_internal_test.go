package cache

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func newS3(t *testing.T) *S3 {
	t.Helper()
	bucket := s3clienttest.Start(t)
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

	clientProvider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint: s3clienttest.Addr,
		UseSSL:   false,
	})

	s, err := NewS3(ctx, S3Config{
		Bucket:           bucket,
		MaxTTL:           time.Hour,
		UploadPartSizeMB: 16,
	}, clientProvider)
	assert.NoError(t, err)
	return s
}

// TestS3TagMismatchIsMiss verifies that when the companion metadata object
// describes a different data object than the one stored (the outcome of
// interleaved concurrent writes to the same key), reads report a cache miss
// rather than serving the data with mismatched metadata.
func TestS3TagMismatchIsMiss(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("tag-mismatch")

	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("hello world"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, _, err := s.Open(ctx, key)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "hello world", string(data))

	// Simulate a stale companion left behind by an interleaved writer: the data
	// object keeps its tag while the metadata is overwritten with a different
	// one.
	assert.NoError(t, s.writeMeta(ctx, s.namespace, key, s3Meta{
		Tag:       "stale-tag",
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	_, err = s.Stat(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
	_, _, err = s.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

// TestS3LegacyUntaggedObjectIsReadable verifies backwards compatibility: an
// object written before tagging was introduced carries no tag on either the
// data object or its companion metadata, and must still read as a hit.
func TestS3LegacyUntaggedObjectIsReadable(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("legacy-untagged")
	data := []byte("legacy payload")

	// Write the data object with no tag user-metadata, mirroring an object
	// produced by the pre-tagging implementation.
	_, err := s.client.PutObject(ctx, s.config.Bucket, s.keyToPath(s.namespace, key),
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	assert.NoError(t, err)

	// Companion metadata with no tag field, as older writers produced.
	assert.NoError(t, s.writeMeta(ctx, s.namespace, key, s3Meta{ExpiresAt: time.Now().Add(time.Hour)}))

	r, _, err := s.Open(ctx, key)
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, string(data), string(got))
}
