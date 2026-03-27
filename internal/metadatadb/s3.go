package metadatadb

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
)

// S3Backend stores metadata state as JSON objects in S3. Locking uses a
// separate lock object with TTL-based expiry for stale lock recovery. The
// idempotence token maps to the S3 object ETag.
type S3Backend struct {
	client  *minio.Client
	logger  *slog.Logger
	bucket  string
	prefix  string
	lockTTL time.Duration
}

// S3BackendConfig configures the S3 metadata backend.
type S3BackendConfig struct {
	Client  *minio.Client
	Logger  *slog.Logger
	Bucket  string
	Prefix  string
	LockTTL time.Duration
}

func NewS3Backend(config S3BackendConfig) *S3Backend {
	if config.Prefix == "" {
		config.Prefix = "_meta"
	}
	if config.LockTTL == 0 {
		config.LockTTL = 30 * time.Second
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &S3Backend{
		client:  config.Client,
		logger:  logger,
		bucket:  config.Bucket,
		prefix:  config.Prefix,
		lockTTL: config.LockTTL,
	}
}

func (s *S3Backend) stateKey(namespace string) string { return s.prefix + "/" + namespace + ".json" }
func (s *S3Backend) lockKey(namespace string) string  { return s.prefix + "/" + namespace + ".lock" }

func (s *S3Backend) Load(ctx context.Context, namespace string) (json.RawMessage, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.stateKey(namespace), minio.GetObjectOptions{})
	if err != nil {
		return nil, "", errors.Wrap(err, "get object")
	}
	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		if isNotFound(err) {
			return nil, "", nil
		}
		return nil, "", errors.Wrap(err, "stat object")
	}

	var buf bytes.Buffer
	buf.Grow(int(info.Size))
	if _, err := buf.ReadFrom(obj); err != nil {
		return nil, "", errors.Wrap(err, "read object")
	}

	return buf.Bytes(), info.ETag, nil
}

func (s *S3Backend) Store(ctx context.Context, namespace string, data json.RawMessage, token string) error {
	opts := minio.PutObjectOptions{
		ContentType: "application/json",
	}
	if token != "" {
		opts.SetMatchETag(token)
	} else {
		opts.SetMatchETagExcept("*")
	}

	_, err := s.client.PutObject(ctx, s.bucket, s.stateKey(namespace),
		bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return ErrInvalidToken
		}
		return errors.Wrap(err, "put object")
	}
	return nil
}

func (s *S3Backend) Lock(ctx context.Context, namespace string) error {
	key := s.lockKey(namespace)
	for {
		// Try to acquire by writing a lock object with If-None-Match: *
		opts := minio.PutObjectOptions{
			UserMetadata: map[string]string{
				"Expires-At": time.Now().Add(s.lockTTL).Format(time.RFC3339),
			},
		}
		opts.SetMatchETagExcept("*")

		_, err := s.client.PutObject(ctx, s.bucket, key,
			bytes.NewReader([]byte("locked")), 6, opts)
		if err == nil {
			return nil
		}

		if !isPreconditionFailed(err) {
			return errors.Wrap(err, "acquire lock")
		}

		// Lock exists — check if it's stale and remove it.
		if err := s.tryExpireStaleLock(ctx, key); err != nil {
			s.logger.WarnContext(ctx, "stale lock check failed", "key", key, "error", err)
		} else {
			continue
		}

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *S3Backend) Unlock(ctx context.Context, namespace string) error {
	err := s.client.RemoveObject(ctx, s.bucket, s.lockKey(namespace), minio.RemoveObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "remove lock")
	}
	return nil
}

func (s *S3Backend) tryExpireStaleLock(ctx context.Context, key string) error {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "stat lock")
	}
	expiresAt, err := time.Parse(time.RFC3339, info.UserMetadata["Expires-At"])
	if err != nil {
		return errors.Wrap(err, "parse lock expiry")
	}
	if time.Now().Before(expiresAt) {
		return errors.New("lock not expired")
	}
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return errors.Wrap(err, "remove stale lock")
	}
	return nil
}

func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}

func isPreconditionFailed(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusPreconditionFailed
}
