package cache

import (
	"bufio"
	"context"
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
func (s *S3) statAndHeaders(ctx context.Context, key Key) (minio.ObjectInfo, http.Header, error) {
	objectName := s.keyToPath(s.namespace, key)

	objInfo, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == s3ErrNoSuchKey {
			return minio.ObjectInfo{}, nil, os.ErrNotExist
		}
		return minio.ObjectInfo{}, nil, errors.Errorf("failed to stat object: %w", err)
	}

	if !objInfo.Expires.IsZero() && time.Now().After(objInfo.Expires) {
		return minio.ObjectInfo{}, nil, errors.Join(os.ErrNotExist, s.Delete(ctx, key))
	}

	headers := make(http.Header)
	if headersJSON := objInfo.UserMetadata["Headers"]; headersJSON != "" {
		if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
			return minio.ObjectInfo{}, nil, errors.Errorf("failed to unmarshal headers: %w", err)
		}
	}

	if headers.Get("Last-Modified") == "" && !objInfo.LastModified.IsZero() {
		headers.Set("Last-Modified", objInfo.LastModified.UTC().Format(http.TimeFormat))
	}

	return objInfo, headers, nil
}

func (s *S3) Stat(ctx context.Context, key Key) (http.Header, error) {
	objInfo, headers, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return nil, err
	}
	headers.Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))
	return headers, nil
}

func (s *S3) Open(ctx context.Context, key Key) (io.ReadCloser, http.Header, error) {
	objInfo, headers, err := s.statAndHeaders(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	headers.Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))

	objectName := s.keyToPath(s.namespace, key)

	// Reset expiration time to implement LRU (same as disk cache).
	// Only refresh when remaining TTL is below 50% of max to avoid a
	// server-side copy on every read.
	if !objInfo.Expires.IsZero() {
		now := time.Now()
		if objInfo.Expires.Sub(now) < s.config.MaxTTL/2 {
			newExpiresAt := ceilSecond(now.Add(s.config.MaxTTL))
			go func() {
				bgCtx := context.WithoutCancel(ctx)
				if err := s.refreshExpiration(bgCtx, objectName, objInfo, newExpiresAt); err != nil {
					s.logger.WarnContext(bgCtx, "Failed to refresh S3 expiration", "object", objectName, "error", err)
				}
			}()
		}
	}

	reader, err := s.parallelGetReader(ctx, s.config.Bucket, objectName, objInfo.Size, objInfo.ETag)
	if err != nil {
		return nil, nil, err
	}

	return reader, headers, nil
}

// refreshExpiration updates the Expires header on an S3 object using
// server-side copy-to-self with metadata replacement. This avoids re-uploading
// the object data.
func (s *S3) refreshExpiration(ctx context.Context, objectName string, objInfo minio.ObjectInfo, newExpiresAt time.Time) error {
	src := minio.CopySrcOptions{
		Bucket: s.config.Bucket,
		Object: objectName,
	}
	dst := minio.CopyDestOptions{
		Bucket:          s.config.Bucket,
		Object:          objectName,
		UserMetadata:    objInfo.UserMetadata,
		ReplaceMetadata: true,
		Expires:         newExpiresAt,
	}
	if _, err := s.client.CopyObject(ctx, dst, src); err != nil {
		return errors.Wrap(err, "copy object")
	}
	return nil
}

// updateHeaders replaces the stored headers on an S3 object using server-side
// copy-to-self with metadata replacement.
func (s *S3) updateHeaders(ctx context.Context, namespace Namespace, key Key, headers http.Header, expiresAt time.Time) error {
	objectName := s.keyToPath(namespace, key)
	userMetadata := make(map[string]string)
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return errors.Errorf("failed to marshal headers: %w", err)
	}
	userMetadata["Headers"] = string(headersJSON)

	src := minio.CopySrcOptions{Bucket: s.config.Bucket, Object: objectName}
	dst := minio.CopyDestOptions{
		Bucket:          s.config.Bucket,
		Object:          objectName,
		UserMetadata:    userMetadata,
		ReplaceMetadata: true,
		Expires:         expiresAt,
	}
	if _, err := s.client.CopyObject(ctx, dst, src); err != nil {
		return errors.Wrap(err, "update headers")
	}
	return nil
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

func (s *S3) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (Writer, error) {
	if ttl > s.config.MaxTTL || ttl == 0 {
		ttl = s.config.MaxTTL
	}

	// Clone headers to avoid concurrent access issues
	clonedHeaders := make(http.Header)
	maps.Copy(clonedHeaders, headers)

	expiresAt := ceilSecond(time.Now().Add(ttl))

	ctx, cancel := context.WithCancelCause(ctx)

	pr, pw := io.Pipe()

	writer := &s3Writer{
		s3:        s,
		key:       key,
		namespace: s.namespace,
		pipe:      pw,
		expiresAt: expiresAt,
		headers:   clonedHeaders,
		etag:      newETagWriter(),
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

	err := s.client.RemoveObject(ctx, s.config.Bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return errors.Errorf("failed to remove object: %w", err)
	}

	return nil
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
	etag      *etagWriter
	ctx       context.Context
	cancel    context.CancelCauseFunc
	errCh     chan error
	uploadErr error
	closed    bool
}

func (w *s3Writer) Write(p []byte) (int, error) {
	n, err := w.pipe.Write(p)
	if n > 0 {
		w.etag.WriteBytes(p[:n])
	}
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

	// Update stored headers with the computed ETag via server-side copy.
	// Skip if the context was cancelled (via Abort).
	if w.ctx.Err() == nil {
		w.etag.SetETag(w.headers)
		return w.s3.updateHeaders(w.ctx, w.namespace, w.key, w.headers, w.expiresAt)
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

	userMetadata := make(map[string]string)
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
