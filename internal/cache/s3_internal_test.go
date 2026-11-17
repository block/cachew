package cache

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
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

// TestS3StaleCompanionFallsBackToObjectMetadata verifies reads when the companion mismatches the data object.
//
// A young data object reads as a miss, since a commit may be in flight. Past the grace window it is served with
// its own embedded metadata.
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

	// Simulate a stale companion from an interleaved writer: the data object keeps its tag, the metadata gets another.
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

// TestS3StaleCompanionWithoutETagStaysMiss verifies a data object without an embedded ETag is never served.
//
// An intermediate format stored the ETag only in the companion, and a fallback hit would lack a validator.
func TestS3StaleCompanionWithoutETagStaysMiss(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("no-embedded-etag")
	data := []byte("payload")

	// Write a tagged data object without an embedded ETag, mirroring the format that kept it only in the companion.
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

// TestS3AbortedWriteNeverBecomesReadable verifies an abort after the data object upload deletes the data object.
//
// Otherwise the uncommitted write could age into the stale-companion fallback.
func TestS3AbortedWriteNeverBecomesReadable(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("aborted-write")

	w, err := s.Create(ctx, key, nil, time.Hour)
	assert.NoError(t, err)
	_, err = w.Write([]byte("uncommitted"))
	assert.NoError(t, err)

	// Complete the upload before aborting: closing the pipe lets PutObject see EOF and land the data object.
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

// TestS3AbortSparesConcurrentWrite verifies an aborted writer's cleanup spares a concurrent writer's object.
//
// The delete is conditional on the stored object still carrying the aborting writer's tag.
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

// TestS3LegacyUntaggedObjectIsReadable verifies an object written before tagging still reads as a hit.
//
// Such objects carry no tag on either the data object or its companion metadata.
func TestS3LegacyUntaggedObjectIsReadable(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("legacy-untagged")
	data := []byte("legacy payload")

	// Write the data object without tag user metadata, as the pre-tagging implementation did.
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

func TestS3ExpiryRefreshPreservesUpdatedHeaders(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	ctx := t.Context()
	key := NewKey("refresh-preserves-headers")

	w, err := s.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = w.Write([]byte("hello world"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	assert.NoError(t, s.UpdateMetadata(ctx, key, http.Header{"X-Checked": {"yes"}}, time.Minute))

	r, _, err := s.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	deadline := time.Now().Add(10 * time.Second)
	for {
		meta, err := s.readMeta(ctx, s.namespace, key)
		assert.NoError(t, err)
		if time.Until(meta.ExpiresAt) > s.config.MaxTTL/2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expiry was not refreshed")
		}
		time.Sleep(50 * time.Millisecond)
	}

	headers, err := s.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "yes", headers.Get("X-Checked"))
}

func TestS3RefreshedExpiry(t *testing.T) {
	s := &S3{config: S3Config{MaxTTL: time.Hour}}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		slidingExpiry bool
		expiresAt     time.Time
		want          time.Time
		wantRefresh   bool
	}{
		{name: "SlidingDue", slidingExpiry: true, expiresAt: now.Add(10 * time.Minute), want: now.Add(time.Hour), wantRefresh: true},
		{name: "SlidingNotDue", slidingExpiry: true, expiresAt: now.Add(45 * time.Minute)},
		{name: "ExplicitTTLNeverExtended", expiresAt: now.Add(10 * time.Minute)},
		{name: "NoExpiry", slidingExpiry: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, refresh := s.refreshedExpiry(tt.slidingExpiry, tt.expiresAt, now)
			assert.Equal(t, tt.wantRefresh, refresh)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestS3CreateRecordsSlidingExpiry(t *testing.T) {
	s := newS3(t)
	defer s.Close()

	tests := []struct {
		name string
		ttl  time.Duration
		want bool
	}{
		{name: "DefaultTTL", ttl: 0, want: true},
		{name: "ExplicitTTL", ttl: 10 * time.Minute, want: false},
		{name: "ExplicitTTLAboveMax", ttl: 2 * time.Hour, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			key := NewKey(tt.name)
			w, err := s.Create(ctx, key, nil, tt.ttl)
			assert.NoError(t, err)
			_, err = w.Write([]byte("payload"))
			assert.NoError(t, err)
			assert.NoError(t, w.Close())

			objInfo, _, _, err := s.statAndHeaders(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, hasSlidingExpiry(objInfo))
		})
	}
}
