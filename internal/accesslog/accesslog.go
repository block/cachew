// Package accesslog exports structured HTTP access log events to S3 as
// batched JSONL objects (gzip-compressed by default), suitable for ingestion
// by log analysis and security monitoring pipelines.
package accesslog

import (
	"net/http"
	"time"

	"github.com/alecthomas/errors"
)

// Compression modes for exported objects.
const (
	CompressionGzip = "gzip"
	CompressionNone = "none"
)

// Config configures access log export to S3. Export is enabled when Bucket is
// non-empty. Connection parameters (endpoint, region, credentials) come from
// the global s3 block.
type Config struct {
	Bucket            string            `hcl:"bucket" help:"S3 bucket to export access log events to."`
	Prefix            string            `hcl:"prefix,optional" default:"access-logs" help:"Object key prefix for exported batches."`
	FlushInterval     time.Duration     `hcl:"flush-interval,optional" default:"1m" help:"How often buffered events are flushed to S3."`
	MaxBufferedEvents int               `hcl:"max-buffered-events,optional" default:"65536" help:"Maximum events held in memory; new events are dropped when the buffer is full."`
	Compression       string            `hcl:"compression,optional" default:"gzip" help:"Compression for exported objects: gzip or none."`
	Headers           map[string]string `hcl:"headers,optional" help:"Record these inbound request headers as the given event field."`
}

// Validate checks the configuration for invalid values.
func (c Config) Validate() error {
	if c.FlushInterval <= 0 {
		return errors.Errorf("invalid access log flush-interval %s: must be positive", c.FlushInterval)
	}
	switch c.Compression {
	case "", CompressionGzip, CompressionNone:
		return nil
	default:
		return errors.Errorf("invalid access log compression %q: must be %q or %q", c.Compression, CompressionGzip, CompressionNone)
	}
}

// Event is a single access log record. It is serialised as one JSON object
// per line (JSONL).
type Event struct {
	Timestamp  time.Time         `json:"timestamp"`
	Method     string            `json:"method"`
	Path       string            `json:"path"`
	Query      string            `json:"query,omitempty"`
	Status     int               `json:"status"`
	BytesSent  int64             `json:"bytes_sent"`
	DurationMS float64           `json:"duration_ms"`
	RemoteAddr string            `json:"remote_addr"`
	Host       string            `json:"host,omitempty"`
	UserAgent  string            `json:"user_agent,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// Recorder accepts access log events. Implemented by *Writer.
type Recorder interface {
	Record(event Event)
}

// Middleware records one Event per request to the given Recorder.
func Middleware(next http.Handler, recorder Recorder, config Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &responseRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(rec, r)

		event := Event{
			Timestamp:  start.UTC(),
			Method:     r.Method,
			Path:       r.URL.Path,
			Query:      r.URL.RawQuery,
			Status:     rec.statusCode(),
			BytesSent:  rec.bytes,
			DurationMS: float64(time.Since(start)) / float64(time.Millisecond),
			RemoteAddr: r.RemoteAddr,
			Host:       r.Host,
			UserAgent:  r.UserAgent(),
		}
		for header, field := range config.Headers {
			if v := r.Header.Get(header); v != "" {
				if event.Headers == nil {
					event.Headers = map[string]string{}
				}
				event.Headers[field] = v
			}
		}
		recorder.Record(event)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err //nolint:wrapcheck
}

// Flush is implemented explicitly because streaming handlers type-assert
// http.Flusher directly on the ResponseWriter they receive.
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap supports http.ResponseController.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *responseRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}
