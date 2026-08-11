package accesslog

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
)

// Writer buffers access log events in memory and periodically flushes them to
// S3 as JSONL objects, gzip-compressed unless configured otherwise. Recording
// never blocks the request path: when the buffer is full new events are
// dropped and counted, and a failed flush requeues its batch for the next
// attempt.
type Writer struct {
	logger         *slog.Logger
	config         Config
	clientProvider s3client.ClientProvider
	hostname       string

	mu      sync.Mutex
	events  []Event
	dropped int64

	stop    chan struct{}
	stopped chan struct{}
}

// NewWriter creates a Writer and starts its background flush loop. The config
// must have been validated with Config.Validate.
func NewWriter(ctx context.Context, config Config, clientProvider s3client.ClientProvider) *Writer {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	w := &Writer{
		logger:         logging.FromContext(ctx),
		config:         config,
		clientProvider: clientProvider,
		hostname:       hostname,
		stop:           make(chan struct{}),
		stopped:        make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// Record buffers an event for the next flush, dropping it if the buffer is full.
func (w *Writer) Record(event Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.events) >= w.config.MaxBufferedEvents {
		w.dropped++
		return
	}
	w.events = append(w.events, event)
}

// Close stops the background loop, performs the final flush, and returns its
// error. It must be called after the last Record, once the HTTP server has
// fully shut down.
func (w *Writer) Close(ctx context.Context) error {
	close(w.stop)
	select {
	case <-w.stopped:
	case <-ctx.Done():
		return errors.Errorf("access log writer close: %w", ctx.Err())
	}
	return w.flushBatch(ctx)
}

// run flushes periodically until Close is called. Cancellation of ctx must
// not stop the loop or fail uploads: the root context is cancelled on SIGTERM
// while the server is still handling (and recording) requests during graceful
// shutdown, and Close performs the final flush only after that drain.
func (w *Writer) run(ctx context.Context) {
	defer close(w.stopped)
	ctx = context.WithoutCancel(ctx)
	ticker := time.NewTicker(w.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush(ctx)
		case <-w.stop:
			return
		}
	}
}

func (w *Writer) flush(ctx context.Context) {
	if err := w.flushBatch(ctx); err != nil {
		w.logger.ErrorContext(ctx, "Failed to export access log batch", "error", err)
	}
}

// flushBatch drains the buffer and uploads it as one object, requeueing the
// batch on failure so a later flush can retry it.
func (w *Writer) flushBatch(ctx context.Context) error {
	w.mu.Lock()
	events := w.events
	dropped := w.dropped
	w.events = nil
	w.dropped = 0
	w.mu.Unlock()

	if dropped > 0 {
		w.logger.WarnContext(ctx, "Dropped access log events, buffer full", "dropped", dropped)
	}
	if len(events) == 0 {
		return nil
	}
	if err := w.upload(ctx, events); err != nil {
		w.requeue(events)
		return errors.Errorf("flush %d access log events: %w", len(events), err)
	}
	return nil
}

// requeue puts a failed batch back at the front of the buffer so ordering is
// preserved, dropping the newest events if the combined size exceeds the cap.
func (w *Writer) requeue(events []Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	combined := append(events, w.events...) //nolint:gocritic
	if len(combined) > w.config.MaxBufferedEvents {
		w.dropped += int64(len(combined) - w.config.MaxBufferedEvents)
		combined = combined[:w.config.MaxBufferedEvents]
	}
	w.events = combined
}

func (w *Writer) upload(ctx context.Context, events []Event) error {
	client, err := w.clientProvider()
	if err != nil {
		return errors.Errorf("s3 client: %w", err)
	}

	buf, suffix, contentType, err := encodeBatch(events, w.config.Compression)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	key := path.Join(w.config.Prefix, now.Format("2006/01/02"),
		fmt.Sprintf("%s-%d%s", w.hostname, now.UnixNano(), suffix))
	_, err = client.PutObject(ctx, w.config.Bucket, key, buf, int64(buf.Len()),
		minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return errors.Errorf("upload access log batch: %w", err)
	}
	w.logger.DebugContext(ctx, "Exported access log batch", "bucket", w.config.Bucket, "key", key, "events", len(events))
	return nil
}

func encodeBatch(events []Event, compression string) (body *bytes.Buffer, suffix, contentType string, err error) {
	var buf bytes.Buffer
	if compression == CompressionNone {
		enc := json.NewEncoder(&buf)
		for _, event := range events {
			if err := enc.Encode(event); err != nil {
				return nil, "", "", errors.Errorf("encode access log event: %w", err)
			}
		}
		return &buf, ".jsonl", "application/x-ndjson", nil
	}
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, event := range events {
		if err := enc.Encode(event); err != nil {
			return nil, "", "", errors.Errorf("encode access log event: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return nil, "", "", errors.Errorf("compress access log batch: %w", err)
	}
	return &buf, ".jsonl.gz", "application/gzip", nil
}
