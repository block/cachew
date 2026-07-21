package cache

import (
	"bytes"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
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

// TestS3StaleCompanionFallsBackToObjectMetadata verifies that a companion
// describing a different data object than the one stored reads as a miss
// while the data object is young (a commit may still be in flight), and past
// the grace window serves the data object with its own embedded metadata.
func TestS3StaleCompanionFallsBackToObjectMetadata(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("tag-mismatch")

	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("hello world"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, headers, err := s.Open(ctx, key)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "hello world", string(data))
	wantETag := headers.Get(ETagKey)
	assert.NotEqual(t, "", wantETag)

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

	s.companionGrace = 0

	headers, err = s.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, wantETag, headers.Get(ETagKey))

	r, headers, err = s.Open(ctx, key)
	assert.NoError(t, err)
	data, err = io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "hello world", string(data))
	assert.Equal(t, wantETag, headers.Get(ETagKey))

	assert.NoError(t, s.client.RemoveObject(ctx, s.config.Bucket, s.metaPath(s.namespace, key), minio.RemoveObjectOptions{}))

	r, headers, err = s.Open(ctx, key)
	assert.NoError(t, err)
	data, err = io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "hello world", string(data))
	assert.Equal(t, wantETag, headers.Get(ETagKey))
}

// TestS3StaleCompanionWithoutETagStaysMiss verifies that a data object whose
// embedded headers carry no ETag (an intermediate format stored the ETag only
// in the companion) is never served via the stale-companion fallback, since
// it would be a hit without a validator.
func TestS3StaleCompanionWithoutETagStaysMiss(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("no-embedded-etag")
	data := []byte("payload")

	// Write a tagged data object whose embedded headers lack an ETag,
	// mirroring the format that stored the ETag only in the companion.
	_, err := s.client.PutObject(ctx, s.config.Bucket, s.keyToPath(s.namespace, key),
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			UserMetadata: map[string]string{
				s3TagMetadataKey: "data-tag",
				"Headers":        `{"Content-Type":["application/octet-stream"]}`,
			},
		})
	assert.NoError(t, err)
	assert.NoError(t, s.writeMeta(ctx, s.namespace, key, s3Meta{
		Tag:       "stale-tag",
		ExpiresAt: time.Now().Add(time.Hour),
	}))

	s.companionGrace = 0

	_, err = s.Stat(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
	_, _, err = s.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)

	// Same with the companion missing entirely.
	assert.NoError(t, s.client.RemoveObject(ctx, s.config.Bucket, s.metaPath(s.namespace, key), minio.RemoveObjectOptions{}))
	_, _, err = s.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

// TestS3AbortedWriteNeverBecomesReadable verifies that a write aborted after
// its data object upload already succeeded deletes the data object, so the
// uncommitted write cannot age into the stale-companion fallback.
func TestS3AbortedWriteNeverBecomesReadable(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("aborted-write")

	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("uncommitted"))
	assert.NoError(t, err)

	// Complete the upload before aborting: close the pipe so PutObject sees
	// EOF and finishes, then wait for the data object to land.
	sw := w.(*s3Writer)
	assert.NoError(t, sw.pipe.Close())
	objectName := s.keyToPath(s.namespace, key)
	for start := time.Now(); ; {
		if _, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{}); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("data object never landed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.NoError(t, w.Abort(errors.New("caller aborted")))

	s.companionGrace = 0

	_, _, err = s.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
	_, err = s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{})
	assert.Error(t, err)
}

// TestS3AbortSparesConcurrentWrite verifies that an aborted
// writer's cleanup does not delete a concurrent writer's committed data
// object for the same key: the delete is conditional on the stored object
// still carrying the aborting writer's tag.
func TestS3AbortSparesConcurrentWrite(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("aborted-write-interleaved")

	// Writer A uploads its data object but does not commit yet.
	wa, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = wa.Write([]byte("writer A"))
	assert.NoError(t, err)
	swa := wa.(*s3Writer)
	assert.NoError(t, swa.pipe.Close())
	objectName := s.keyToPath(s.namespace, key)
	for start := time.Now(); ; {
		if _, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{}); err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatal("data object never landed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Writer B replaces the object and commits before A's cleanup runs.
	wb, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = wb.Write([]byte("writer B"))
	assert.NoError(t, err)
	assert.NoError(t, wb.Close())

	// A's abort must not delete B's committed object.
	assert.NoError(t, wa.Abort(errors.New("caller aborted")))

	r, _, err := s.Open(ctx, key)
	assert.NoError(t, err)
	data, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, "writer B", string(data))
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
