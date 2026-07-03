package metadatadb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/s3client"
)

// RegisterS3 registers the S3 metadata backend. The clientProvider supplies the
// shared minio client constructed from the global s3 config block.
func RegisterS3(r *Registry, clientProvider s3client.ClientProvider) {
	Register(r, "s3", "Stores metadata state in S3 with synchronous writes",
		func(ctx context.Context, config S3BackendConfig) (*S3Backend, error) {
			return NewS3Backend(ctx, clientProvider, config)
		},
	)
}

// S3Backend stores metadata state as JSON objects in S3.
// Writes are applied to S3 before local state is updated.
// Locking uses a separate lock object with TTL-based expiry for stale lock
// recovery. The idempotence token maps to the S3 object ETag.
type S3Backend struct {
	client  *minio.Client
	bucket  string
	lockTTL time.Duration
	mu      sync.Mutex
	ns      map[string]*s3Namespace
}

// S3BackendConfig configures the S3 metadata backend.
type S3BackendConfig struct {
	Bucket       string        `hcl:"bucket" help:"S3 bucket name."`
	LockTTL      time.Duration `hcl:"lock-ttl,optional" help:"TTL for namespace locks." default:"30s"`
	SyncInterval time.Duration `hcl:"sync-interval,optional" help:"Deprecated; writes are synchronous."`
}

// s3MetadataPrefix is the fixed key prefix for all metadata objects in S3.
// It starts with "." to avoid collisions with cache namespaces, which are
// validated to not start with ".".
const s3MetadataPrefix = ".metadata"

func NewS3Backend(ctx context.Context, clientProvider s3client.ClientProvider, config S3BackendConfig) (*S3Backend, error) {
	if config.LockTTL == 0 {
		config.LockTTL = 30 * time.Second
	}
	client, err := clientProvider()
	if err != nil {
		return nil, errors.Wrap(err, "create S3 client")
	}
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, errors.Errorf("failed to check if bucket exists: %w", err)
	}
	if !exists {
		return nil, errors.Errorf("bucket %s does not exist", config.Bucket)
	}

	logging.FromContext(ctx).InfoContext(ctx, "Constructing S3 metadata backend",
		"bucket", config.Bucket, "prefix", s3MetadataPrefix, "lock-ttl", config.LockTTL)

	return &S3Backend{
		client:  client,
		bucket:  config.Bucket,
		lockTTL: config.LockTTL,
		ns:      make(map[string]*s3Namespace),
	}, nil
}

func (s *S3Backend) namespace(name string) *s3Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns, ok := s.ns[name]; ok {
		return ns
	}
	ns := &s3Namespace{
		backend: s,
		name:    name,
		state:   make(map[string]any),
	}
	s.ns[name] = ns
	return ns
}

func (s *S3Backend) Apply(ctx context.Context, namespace string, ops ...Op) error {
	if len(ops) == 0 {
		return nil
	}
	return s.namespace(namespace).apply(ctx, ops)
}

func (s *S3Backend) Query(_ context.Context, namespace string, q ReadOp, target any) error {
	ns := s.namespace(namespace)
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	result := queryState(ns.state, q)
	return jsonUnmarshalInto(result, target)
}

func (s *S3Backend) Flush(ctx context.Context, namespace string) error {
	return s.namespace(namespace).reload(ctx)
}

func (s *S3Backend) Close(_ context.Context) error { return nil }

// S3 object key helpers

func (s *S3Backend) stateKey(namespace string) string {
	return s3MetadataPrefix + "/" + namespace + ".json"
}
func (s *S3Backend) lockKey(namespace string) string {
	return s3MetadataPrefix + "/" + namespace + ".lock"
}

// S3 load/store/lock/unlock

func (s *S3Backend) load(ctx context.Context, namespace string) (json.RawMessage, string, error) {
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

func (s *S3Backend) store(ctx context.Context, namespace string, data json.RawMessage, token string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if token != "" {
		opts.SetMatchETag(token)
	} else {
		opts.SetMatchETagExcept("*")
	}

	_, err := s.client.PutObject(ctx, s.bucket, s.stateKey(namespace),
		bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		if isPreconditionFailed(err) {
			return errInvalidToken
		}
		return errors.Wrap(err, "put object")
	}
	return nil
}

func (s *S3Backend) lockNamespace(ctx context.Context, namespace string) error {
	key := s.lockKey(namespace)
	for {
		opts := minio.PutObjectOptions{
			Expires: time.Now().Add(s.lockTTL),
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

		if err := s.tryExpireStaleLock(ctx, key); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "stale lock check failed", "key", key, "error", err)
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

func (s *S3Backend) unlockNamespace(ctx context.Context, namespace string) error {
	return errors.Wrap(
		s.client.RemoveObject(ctx, s.bucket, s.lockKey(namespace), minio.RemoveObjectOptions{}),
		"remove lock")
}

func (s *S3Backend) tryExpireStaleLock(ctx context.Context, key string) error {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "stat lock")
	}
	if info.Expires.IsZero() || time.Now().Before(info.Expires) {
		return errors.New("lock not expired")
	}
	return errors.Wrap(
		s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}),
		"remove stale lock")
}

// Per-namespace sync machinery

type s3Namespace struct {
	backend *S3Backend
	name    string
	mu      sync.RWMutex
	state   map[string]any
	syncMu  sync.Mutex
}

const maxTokenRetries = 3

func (n *s3Namespace) reload(ctx context.Context) error {
	n.syncMu.Lock()
	defer n.syncMu.Unlock()

	remote, err := n.load(ctx)
	if err != nil {
		return err
	}

	n.mu.Lock()
	n.state = remote
	n.mu.Unlock()
	return nil
}

func (n *s3Namespace) apply(ctx context.Context, ops []Op) error {
	n.syncMu.Lock()
	defer n.syncMu.Unlock()

	if err := n.backend.lockNamespace(ctx, n.name); err != nil {
		return errors.Wrap(err, "lock namespace")
	}

	remote, err := n.loadReplayStore(ctx, ops)
	unlockErr := n.backend.unlockNamespace(ctx, n.name)
	if err != nil {
		return err
	}
	if unlockErr != nil {
		return errors.Wrap(unlockErr, "unlock namespace")
	}

	n.mu.Lock()
	n.state = remote
	n.mu.Unlock()

	return nil
}

func (n *s3Namespace) load(ctx context.Context) (map[string]any, error) {
	data, _, err := n.backend.load(ctx, n.name)
	if err != nil {
		return nil, errors.Wrap(err, "load namespace")
	}
	return unmarshalState(data)
}

func (n *s3Namespace) loadReplayStore(ctx context.Context, pending []Op) (map[string]any, error) {
	for range maxTokenRetries {
		remote, err := n.tryLoadReplayStore(ctx, pending)
		if errors.Is(err, errInvalidToken) {
			continue
		}
		return remote, err
	}
	return nil, errors.New("max token retries exceeded")
}

func (n *s3Namespace) tryLoadReplayStore(ctx context.Context, pending []Op) (map[string]any, error) {
	data, token, err := n.backend.load(ctx, n.name)
	if err != nil {
		return nil, errors.Wrap(err, "load namespace")
	}

	remote, err := unmarshalState(data)
	if err != nil {
		return nil, err
	}

	for _, o := range pending {
		applyOp(remote, o)
	}

	if len(pending) > 0 {
		merged, err := json.Marshal(remote)
		if err != nil {
			return nil, errors.Wrap(err, "marshal state")
		}
		if err := n.backend.store(ctx, n.name, merged, token); err != nil {
			return nil, errors.Wrap(err, "store namespace")
		}
	}

	return remote, nil
}

func unmarshalState(data json.RawMessage) (map[string]any, error) {
	state := make(map[string]any)
	if data == nil {
		return state, nil
	}
	return state, errors.Wrap(json.Unmarshal(data, &state), "unmarshal state")
}

func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}

func isPreconditionFailed(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusPreconditionFailed
}
