package accesslog_test

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/accesslog"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func newWriter(t *testing.T, bucket string, maxBuffered int, compression string) *accesslog.Writer {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})
	provider := s3client.NewClientProvider(ctx, s3client.Config{
		Endpoint: s3clienttest.Addr,
		UseSSL:   false,
	})
	config := accesslog.Config{
		Bucket:            bucket,
		Prefix:            "access-logs",
		FlushInterval:     time.Hour,
		MaxBufferedEvents: maxBuffered,
		Compression:       compression,
	}
	assert.NoError(t, config.Validate())
	return accesslog.NewWriter(ctx, config, provider)
}

func TestWriterExportsJSONLBatch(t *testing.T) {
	bucket := s3clienttest.Start(t)
	w := newWriter(t, bucket, 100, accesslog.CompressionGzip)

	now := time.Now().UTC().Truncate(time.Millisecond)
	w.Record(accesslog.Event{Timestamp: now, Method: http.MethodGet, Path: "/git/a", Status: 200, BytesSent: 42})
	w.Record(accesslog.Event{Timestamp: now, Method: http.MethodPost, Path: "/api/v1/object/ns/key", Status: 403})
	assert.NoError(t, w.Close(t.Context()))

	events := readExportedEvents(t, bucket)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, "/git/a", events[0].Path)
	assert.Equal(t, int64(42), events[0].BytesSent)
	assert.Equal(t, now, events[0].Timestamp)
	assert.Equal(t, 403, events[1].Status)
}

func TestWriterExportsUncompressedJSONL(t *testing.T) {
	bucket := s3clienttest.Start(t)
	w := newWriter(t, bucket, 100, accesslog.CompressionNone)

	now := time.Now().UTC().Truncate(time.Millisecond)
	w.Record(accesslog.Event{Timestamp: now, Method: http.MethodGet, Path: "/git/a", Status: 200})
	assert.NoError(t, w.Close(t.Context()))

	events := readExportedEvents(t, bucket)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, "/git/a", events[0].Path)
	assert.Equal(t, now, events[0].Timestamp)
}

func TestConfigValidate(t *testing.T) {
	assert.Error(t, accesslog.Config{Bucket: "b", FlushInterval: time.Minute, Compression: "zstd"}.Validate())
	assert.Error(t, accesslog.Config{Bucket: "b"}.Validate())
	assert.Error(t, accesslog.Config{Bucket: "b", FlushInterval: -time.Second}.Validate())
	assert.NoError(t, accesslog.Config{Bucket: "b", FlushInterval: time.Minute}.Validate())
	assert.NoError(t, accesslog.Config{Bucket: "b", FlushInterval: time.Minute, Compression: accesslog.CompressionNone}.Validate())
}

func TestWriterDropsWhenBufferFull(t *testing.T) {
	bucket := s3clienttest.Start(t)
	w := newWriter(t, bucket, 2, accesslog.CompressionGzip)

	for range 5 {
		w.Record(accesslog.Event{Timestamp: time.Now(), Method: http.MethodGet, Path: "/x", Status: 200})
	}
	assert.NoError(t, w.Close(t.Context()))

	events := readExportedEvents(t, bucket)
	assert.Equal(t, 2, len(events))
}

// readExportedEvents decodes every exported JSONL object in the bucket in key order.
func readExportedEvents(t *testing.T, bucket string) []accesslog.Event {
	t.Helper()
	client := s3clienttest.Client(t)
	var events []accesslog.Event
	for object := range client.ListObjects(t.Context(), bucket, minio.ListObjectsOptions{Recursive: true}) {
		assert.NoError(t, object.Err)
		assert.True(t, strings.HasPrefix(object.Key, "access-logs/"))

		obj, err := client.GetObject(t.Context(), bucket, object.Key, minio.GetObjectOptions{})
		assert.NoError(t, err)
		var body io.Reader = obj
		var gz *gzip.Reader
		if strings.HasSuffix(object.Key, ".jsonl.gz") {
			var err error
			gz, err = gzip.NewReader(obj)
			assert.NoError(t, err)
			body = gz
		} else {
			assert.True(t, strings.HasSuffix(object.Key, ".jsonl"))
		}
		dec := json.NewDecoder(body)
		for dec.More() {
			var event accesslog.Event
			assert.NoError(t, dec.Decode(&event))
			events = append(events, event)
		}
		if gz != nil {
			assert.NoError(t, gz.Close())
		}
		assert.NoError(t, obj.Close())
	}
	return events
}
