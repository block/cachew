package cache

import (
	"context"
	"sync"

	"github.com/block/cachew/internal/metrics"
)

// BackendName is a bounded cache type label, excluding paths, buckets and namespaces.
func BackendName(c Cache) string {
	switch c := c.(type) {
	case *Disk:
		return "disk"
	case *S3:
		return "s3"
	case *Memory:
		return "memory"
	case *Remote:
		return "remote"
	case *noOpCache:
		return "disabled"
	case authoritativeCache:
		return BackendName(c.Cache)
	default:
		return "other"
	}
}

const (
	writeSuccess     = "success"
	writeCommitError = "commit_error"
	writeCanceled    = "canceled"
)

func observeWriter(ctx context.Context, c Cache, tier int, w Writer, err error) Writer {
	if !metrics.HasArtifactRequest(ctx) {
		return w
	}
	backend := BackendName(c)
	if err != nil {
		metrics.RecordCacheWrite(ctx, backend, tier, "create_error")
		return w
	}
	return &observedWriter{Writer: w, ctx: ctx, backend: backend, tier: tier}
}

type observedWriter struct {
	Writer
	ctx     context.Context
	backend string
	tier    int
	result  string
	once    sync.Once
}

func (w *observedWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err != nil && w.result == "" {
		w.result = "write_error"
	}
	return n, err //nolint:wrapcheck // Instrumentation preserves backend errors.
}

func (w *observedWriter) Close() error {
	err := w.Writer.Close()
	w.record(err)
	return err //nolint:wrapcheck // Instrumentation preserves backend errors.
}

func (w *observedWriter) Abort(cause error) error {
	if w.result == "" {
		w.result = "aborted"
	}
	err := w.Writer.Abort(cause)
	w.record(err)
	return err //nolint:wrapcheck // Instrumentation preserves backend errors.
}

func (w *observedWriter) record(err error) {
	w.once.Do(func() {
		result := w.result
		if result == "" {
			switch {
			case w.ctx.Err() != nil:
				result = writeCanceled
			case err != nil:
				result = writeCommitError
			default:
				result = writeSuccess
			}
		}
		metrics.RecordCacheWrite(w.ctx, w.backend, w.tier, result)
	})
}
