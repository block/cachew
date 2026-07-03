package cache

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
)

// RegisterS3 registers the S3 cache backend. The clientProvider supplies the
// shared minio client constructed from the global s3 config block.
func RegisterS3(r *Registry, clientProvider s3client.ClientProvider) {
	Register(
		r,
		"s3",
		"Caches objects in S3",
		func(ctx context.Context, config S3Config) (*S3, error) {
			return NewS3(ctx, config, clientProvider)
		},
	)
}

// S3Config contains cache-specific S3 settings. Connection parameters
// (endpoint, region, SSL, credentials) are provided by the global
// s3client.Config block and shared via the s3client.ClientProvider.
type S3Config struct {
	Bucket              string        `hcl:"bucket" help:"S3 bucket name."`
	MaxTTL              time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the S3 cache (defaults to 1 hour)." default:"1h"`
	UploadConcurrency   uint          `hcl:"upload-concurrency,optional" help:"Number of concurrent workers for multi-part uploads (0 = use all CPU cores, defaults to 1)." default:"1"`
	UploadPartSizeMB    uint          `hcl:"upload-part-size-mb,optional" help:"Size of each part for multi-part uploads in megabytes (defaults to 16MB, minimum 5MB)." default:"16"`
	DownloadConcurrency uint          `hcl:"download-concurrency,optional" help:"Number of concurrent range-GET workers for downloads (defaults to 8)." default:"8"`
	DownloadPartSizeMB  uint          `hcl:"download-part-size-mb,optional" help:"Size of each parallel range-GET request in megabytes (defaults to 32MB)." default:"32"`
}

type S3 struct {
	logger    *slog.Logger
	config    S3Config
	namespace Namespace
	client    *minio.Client
}

var _ Cache = (*S3)(nil)

// s3Meta stores mutable metadata in a companion object alongside the
// data object, avoiding expensive server-side copies when updating
// headers or refreshing expiry.
//
// Tag correlates the metadata with the data object it describes. The data
// object and its companion are written in two separate, non-atomic S3
// operations, so concurrent writers to the same key can interleave and leave
// the metadata describing a different data object than the one actually
// stored. Stamping both objects with the same random tag lets readers detect
// this mismatch (see statAndHeaders) and treat it as a cache miss.
type s3Meta struct {
	Headers   http.Header `json:"headers,omitempty"`
	ExpiresAt time.Time   `json:"expires_at"`
	Tag       string      `json:"tag,omitempty"`
}

// s3TagMetadataKey is the data object's user-metadata key holding the tag that
// must match the companion s3Meta.Tag. Objects written before tagging was
// introduced carry no tag on either object, so an empty-to-empty comparison
// keeps them readable.
const s3TagMetadataKey = "Tag"

// newS3Tag returns a random hex tag used to correlate a data object with its
// companion metadata object.
func newS3Tag() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", errors.Wrap(err, "generate tag")
	}
	return hex.EncodeToString(buf[:]), nil
}

// NewS3 creates a new S3-based cache instance.
//
// The minio client is obtained from the shared clientProvider, which is
// constructed once from the global s3 configuration block. Cache-specific
// settings (bucket, TTL, upload tuning) come from the per-instance S3Config.
//
// This [Cache] implementation stores cache entries in an S3-compatible object storage service.
// Metadata (headers and expiration time) are stored as object user metadata. The implementation
// uses the lightweight minio-go SDK to reduce overhead compared to the AWS SDK.
func NewS3(ctx context.Context, config S3Config, clientProvider s3client.ClientProvider) (*S3, error) {
	// Set defaults and validate configuration
	if config.UploadConcurrency == 0 {
		// #nosec G115 -- n is guaranteed >= 1. I was unable to satisfy all linters.
		config.UploadConcurrency = uint(max(runtime.NumCPU(), 1))
	}

	if config.UploadPartSizeMB < 5 {
		return nil, errors.New("upload-part-size-mb must be at least 5MB (S3 minimum part size)")
	}

	if config.DownloadConcurrency == 0 {
		config.DownloadConcurrency = 8
	}

	if config.DownloadPartSizeMB == 0 {
		config.DownloadPartSizeMB = 32
	}

	client, err := clientProvider()
	if err != nil {
		return nil, errors.Errorf("failed to obtain shared S3 client: %w", err)
	}

	logging.FromContext(ctx).InfoContext(ctx, "Constructing S3 cache",
		"endpoint", client.EndpointURL(), "bucket", config.Bucket,
		"max-ttl", config.MaxTTL,
		"upload-concurrency", config.UploadConcurrency, "upload-part-size-mb", config.UploadPartSizeMB,
		"download-concurrency", config.DownloadConcurrency, "download-part-size-mb", config.DownloadPartSizeMB)

	// Verify bucket exists
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, errors.Errorf("failed to check if bucket exists: %w", err)
	}
	if !exists {
		return nil, errors.Errorf("bucket %s does not exist", config.Bucket)
	}

	return &S3{
		logger: logging.FromContext(ctx),
		config: config,
		client: client,
	}, nil
}

func (s *S3) String() string {
	return fmt.Sprintf("s3:%s/%s", s.client.EndpointURL().Host, s.config.Bucket)
}

func (s *S3) Close() error {
	return nil
}

func (s *S3) keyToPath(namespace Namespace, key Key) string {
	hexKey := key.String()
	prefix := ""

	if namespace != "" {
		prefix = string(namespace) + "/"
	}

	// Use first two hex digits as directory, full hex as filename
	return prefix + hexKey[:2] + "/" + hexKey
}

// statAndHeaders retrieves object metadata, checks expiry, and returns parsed headers.
// It reads immutable headers from the data object's user metadata, then overlays
// mutable metadata from a companion .meta object (ETag, refreshed expiry).
func (s *S3) statAndHeaders(ctx context.Context, key Key) (minio.ObjectInfo, http.Header, s3Meta, error) {
	objectName := s.keyToPath(s.namespace, key)

	objInfo, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == s3ErrNoSuchKey {
			return minio.ObjectInfo{}, nil, s3Meta{}, os.ErrNotExist
		}
		return minio.ObjectInfo{}, nil, s3Meta{}, errors.Errorf("failed to stat object: %w", err)
	}

	headers := make(http.Header)
	if headersJSON := objInfo.UserMetadata["Headers"]; headersJSON != "" {
		if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
			return minio.ObjectInfo{}, nil, s3Meta{}, errors.Errorf("failed to unmarshal headers: %w", err)
		}
	}

	meta, err := s.readMeta(ctx, s.namespace, key)
	if err != nil {
		return minio.ObjectInfo{}, nil, s3Meta{}, err
	}

	// Reject metadata that describes a different data object than the one
	// stored (e.g. interleaved concurrent writes to the same key, or a data
	// object whose companion has not been written yet). Treating this as a
	// miss lets the caller fall back to upstream; the next write reconciles.
	if objInfo.UserMetadata[s3TagMetadataKey] != meta.Tag {
		return minio.ObjectInfo{}, nil, s3Meta{}, os.ErrNotExist
	}

	maps.Copy(headers, meta.Headers)

	// Companion expiry takes precedence over the data object's Expires header.
	if meta.ExpiresAt.IsZero() {
		meta.ExpiresAt = objInfo.Expires
	}

	if !meta.ExpiresAt.IsZero() && time.Now().After(meta.ExpiresAt) {
		return minio.ObjectInfo{}, nil, s3Meta{}, errors.Join(os.ErrNotExist, s.Delete(ctx, key))
	}

	if headers.Get("Last-Modified") == "" && !objInfo.LastModified.IsZero() {
		headers.Set("Last-Modified", objInfo.LastModified.UTC().Format(http.TimeFormat))
	}

	return objInfo, headers, meta, nil
}

func (s *S3) Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error) {
	objInfo, headers, _, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return nil, err
	}
	headers.Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))
	if h, err := conditionalShortCircuit(headers, opts); err != nil {
		return h, err
	}
	return headers, nil
}

func (s *S3) Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	objInfo, headers, meta, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	headers.Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))

	objectName := s.keyToPath(s.namespace, key)

	// Reset expiration time to implement LRU (same as disk cache).
	// Only refresh when remaining TTL is below 50% of max to avoid
	// rewriting the companion metadata on every read.
	if !meta.ExpiresAt.IsZero() {
		now := time.Now()
		if meta.ExpiresAt.Sub(now) < s.config.MaxTTL/2 {
			newExpiresAt := ceilSecond(now.Add(s.config.MaxTTL))
			refreshHeaders := make(http.Header)
			if etag := meta.Headers.Get(ETagKey); etag != "" {
				refreshHeaders.Set(ETagKey, etag)
			}
			refreshMeta := s3Meta{Headers: refreshHeaders, ExpiresAt: newExpiresAt, Tag: meta.Tag}
			go func() {
				bgCtx := context.WithoutCancel(ctx)
				if err := s.writeMeta(bgCtx, s.namespace, key, refreshMeta); err != nil {
					s.logger.WarnContext(bgCtx, "Failed to refresh S3 expiration", "key", key, "error", err)
				}
			}()
		}
	}

	if h, err := conditionalShortCircuit(headers, opts); err != nil {
		return nil, h, err
	}

	start, length, partial, rangeErr := rangeShortCircuit(headers, objInfo.Size, opts)
	if rangeErr != nil {
		return nil, headers, rangeErr
	}
	if partial {
		reader, err := s.rangeGetReader(ctx, s.config.Bucket, objectName, start, length, objInfo.ETag)
		if err != nil {
			return nil, nil, err
		}
		return reader, headers, nil
	}

	reader, err := s.parallelGetReader(ctx, s.config.Bucket, objectName, objInfo.Size, objInfo.ETag)
	if err != nil {
		return nil, nil, err
	}

	return reader, headers, nil
}

func (s *S3) metaPath(namespace Namespace, key Key) string {
	return s.keyToPath(namespace, key) + ".meta"
}

// writeMeta writes the companion metadata object for the given key.
func (s *S3) writeMeta(ctx context.Context, namespace Namespace, key Key, meta s3Meta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal metadata")
	}
	objectName := s.metaPath(namespace, key)
	_, err = s.client.PutObject(ctx, s.config.Bucket, objectName, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return errors.Wrap(err, "put metadata object")
	}
	return nil
}

// readMeta reads the companion metadata object for the given key.
// Returns a zero s3Meta if the companion does not exist (legacy objects).
func (s *S3) readMeta(ctx context.Context, namespace Namespace, key Key) (s3Meta, error) {
	objectName := s.metaPath(namespace, key)
	obj, err := s.client.GetObject(ctx, s.config.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return s3Meta{}, errors.Wrap(err, "get metadata object")
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).Code == s3ErrNoSuchKey {
			return s3Meta{}, nil
		}
		return s3Meta{}, errors.Wrap(err, "read metadata object")
	}

	var meta s3Meta
	if err := json.Unmarshal(data, &meta); err != nil {
		return s3Meta{}, errors.Wrap(err, "unmarshal metadata")
	}
	return meta, nil
}

// ceilSecond rounds a time up to the next whole second. S3's Expires header
// uses HTTP-date format (second precision), so sub-second components would be
// silently truncated, potentially causing premature expiry.
func ceilSecond(t time.Time) time.Time {
	if t.Nanosecond() == 0 {
		return t
	}
	return t.Truncate(time.Second).Add(time.Second)
}

const s3ErrNoSuchKey = "NoSuchKey"

// s3Reader wraps minio.Object to convert S3 errors to standard errors.
type s3Reader struct {
	obj *minio.Object
}

func (r *s3Reader) Read(p []byte) (int, error) {
	n, err := r.obj.Read(p)
	if err == nil || errors.Is(err, io.EOF) {
		return n, err //nolint:wrapcheck
	}
	// Convert NoSuchKey to os.ErrNotExist for consistency
	errResponse := minio.ToErrorResponse(err)
	if errResponse.Code == s3ErrNoSuchKey {
		return n, os.ErrNotExist
	}
	return n, errors.WithStack(err)
}

func (r *s3Reader) Close() error {
	return errors.WithStack(r.obj.Close())
}

func (s *S3) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	if ttl > s.config.MaxTTL || ttl == 0 {
		ttl = s.config.MaxTTL
	}

	// Clone (to avoid concurrent access) and drop transport headers.
	clonedHeaders := httputil.FilterHeaders(headers, httputil.TransportHeaders...)
	if err := setCreateETag(clonedHeaders, opts...); err != nil {
		return nil, err
	}

	expiresAt := ceilSecond(time.Now().Add(ttl))

	tag, err := newS3Tag()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancelCause(ctx)

	pr, pw := io.Pipe()

	writer := &s3Writer{
		s3:        s,
		key:       key,
		namespace: s.namespace,
		pipe:      pw,
		expiresAt: expiresAt,
		headers:   clonedHeaders,
		tag:       tag,
		ctx:       ctx,
		cancel:    cancel,
		errCh:     make(chan error, 1),
	}

	// Start upload in background goroutine. The buffered reader decouples the
	// upstream pipe (zero-buffer io.Pipe from the archive process) from the
	// upload chunking loop. Without it, when the uploader blocks on a full
	// jobs channel or slow S3 part upload, the archive goroutine stalls
	// because nobody is consuming the pipe. The 8 MiB buffer absorbs ongoing
	// archive output during those brief stalls.
	br := bufio.NewReaderSize(pr, 8<<20)
	go writer.upload(pr, br)

	return writer, nil
}

func (s *S3) Delete(ctx context.Context, key Key) error {
	objectName := s.keyToPath(s.namespace, key)
	if err := s.client.RemoveObject(ctx, s.config.Bucket, objectName, minio.RemoveObjectOptions{}); err != nil {
		return errors.Errorf("failed to remove object: %w", err)
	}
	// Best-effort removal of companion metadata object.
	_ = s.client.RemoveObject(ctx, s.config.Bucket, s.metaPath(s.namespace, key), minio.RemoveObjectOptions{}) //nolint:errcheck
	return nil
}

func (s *S3) Invalidate(ctx context.Context, key Key) error {
	return errors.WithStack(s.Delete(ctx, key))
}

func (s *S3) Stats(_ context.Context) (Stats, error) {
	// S3 doesn't provide efficient count/size operations without listing the entire bucket,
	// which would be prohibitively slow and expensive.
	return Stats{}, ErrStatsUnavailable
}

type s3Writer struct {
	s3        *S3
	key       Key
	namespace Namespace
	pipe      *io.PipeWriter
	expiresAt time.Time
	headers   http.Header
	tag       string
	ctx       context.Context
	cancel    context.CancelCauseFunc
	errCh     chan error
	uploadErr error
	closed    bool
}

func (w *s3Writer) Write(p []byte) (int, error) {
	n, err := w.pipe.Write(p)
	if err != nil {
		// Check if upload failed - if so, return that error instead
		select {
		case uploadErr := <-w.errCh:
			if uploadErr != nil {
				w.uploadErr = uploadErr
				return n, uploadErr
			}
		default:
		}
		return n, errors.WithStack(err)
	}
	return n, nil
}

func (w *s3Writer) Abort(err error) error {
	w.cancel(err)
	return w.Close()
}

func (w *s3Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	// Close the pipe writer to signal EOF to the reader
	if err := w.pipe.Close(); err != nil {
		return errors.Wrap(err, "failed to close pipe")
	}

	// If we already captured the upload error during Write, return it
	if w.uploadErr != nil {
		return w.uploadErr
	}

	// Wait for upload to complete and get any error
	if err := <-w.errCh; err != nil {
		return err
	}

	// Skip companion metadata if the context was cancelled via Abort.
	if w.ctx.Err() == nil {
		metaHeaders := make(http.Header)
		metaHeaders.Set(ETagKey, w.headers.Get(ETagKey))
		return w.s3.writeMeta(w.ctx, w.namespace, w.key, s3Meta{Headers: metaHeaders, ExpiresAt: w.expiresAt, Tag: w.tag})
	}
	return nil
}

func (w *s3Writer) upload(pr *io.PipeReader, r io.Reader) {
	var uploadErr error
	defer func() {
		// Use CloseWithError to propagate any error to the writer side
		_ = pr.CloseWithError(uploadErr)
	}()

	objectName := w.s3.keyToPath(w.namespace, w.key)

	userMetadata := map[string]string{s3TagMetadataKey: w.tag}
	if len(w.headers) > 0 {
		headersJSON, err := json.Marshal(w.headers)
		if err != nil {
			uploadErr = errors.Errorf("failed to marshal headers: %w", err)
			w.errCh <- uploadErr
			return
		}
		userMetadata["Headers"] = string(headersJSON)
	}

	// Configure upload options. CRC64-NVME is computed as data streams through and sent as a
	// trailing header so S3 validates integrity server-side, preventing corrupt or truncated uploads.
	opts := minio.PutObjectOptions{
		UserMetadata: userMetadata,
		AutoChecksum: minio.ChecksumCRC64NVME,
		Expires:      w.expiresAt,
	}

	// Enable concurrent streaming for multi-part uploads if configured
	if w.s3.config.UploadConcurrency > 1 {
		opts.ConcurrentStreamParts = true
		opts.NumThreads = w.s3.config.UploadConcurrency
		opts.PartSize = uint64(w.s3.config.UploadPartSizeMB) * 1024 * 1024 // Convert MB to bytes
	}

	// Upload object with streaming (size -1 means unknown size, will use chunked encoding)
	_, err := w.s3.client.PutObject(
		w.ctx,
		w.s3.config.Bucket,
		objectName,
		r,
		-1,
		opts,
	)
	if err != nil {
		uploadErr = errors.Errorf("failed to put object: %w", err)
		w.errCh <- uploadErr
		return
	}

	w.errCh <- nil
}

// Namespace creates a namespaced view of the S3 cache.
func (s *S3) Namespace(namespace Namespace) Cache {
	c := *s
	c.namespace = namespace
	return &c
}

// ListNamespaces returns all unique namespaces in the S3 cache.
// Not implemented for S3 - would require listing all objects.
func (s *S3) ListNamespaces(_ context.Context) ([]string, error) {
	return nil, ErrStatsUnavailable
}
