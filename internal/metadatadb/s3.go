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
	Register(r, "s3", "Stores metadata state in S3 with periodic sync",
		func(ctx context.Context, config S3BackendConfig) (*S3Backend, error) {
			return NewS3Backend(ctx, clientProvider, config)
		},
	)
}

// S3Backend stores metadata state as JSON objects in S3 with periodic sync.
// Writes are applied to local state immediately and queued for the next flush.
// Locking uses a separate lock object with TTL-based expiry for stale lock
// recovery. The idempotence token maps to the S3 object ETag.
type S3Backend struct {
	client       *minio.Client
	bucket       string
	prefix       string
	lockTTL      time.Duration
	syncInterval time.Duration
	mu           sync.Mutex
	ns           map[string]*s3Namespace
	ctx          context.Context
	cancel       context.CancelFunc
}

// S3BackendConfig configures the S3 metadata backend.
type S3BackendConfig struct {
	Bucket       string        `hcl:"bucket" help:"S3 bucket name."`
	Prefix       string        `hcl:"prefix,optional" help:"Key prefix for metadata objects." default:"_meta"`
	LockTTL      time.Duration `hcl:"lock-ttl,optional" help:"TTL for namespace locks." default:"30s"`
	SyncInterval time.Duration `hcl:"sync-interval,optional" help:"Interval between periodic syncs." default:"30s"`
}

func NewS3Backend(ctx context.Context, clientProvider s3client.ClientProvider, config S3BackendConfig) (*S3Backend, error) {
	if config.Prefix == "" {
		config.Prefix = "_meta"
	}
	if config.LockTTL == 0 {
		config.LockTTL = 30 * time.Second
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
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

	ctx, cancel := context.WithCancel(ctx)
	return &S3Backend{
		client:       client,
		bucket:       config.Bucket,
		prefix:       config.Prefix,
		lockTTL:      config.LockTTL,
		syncInterval: config.SyncInterval,
		ns:           make(map[string]*s3Namespace),
		ctx:          ctx,
		cancel:       cancel,
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
		done:    make(chan struct{}),
	}
	go ns.syncLoop()
	s.ns[name] = ns
	return ns
}

func (s *S3Backend) Apply(_ context.Context, namespace string, ops ...Op) error {
	ns := s.namespace(namespace)
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, o := range ops {
		applyOp(ns.state, o)
	}
	ns.pending = append(ns.pending, ops...)
	return nil
}

func (s *S3Backend) Query(_ context.Context, namespace string, q ReadOp, target any) error {
	ns := s.namespace(namespace)
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	result := queryState(ns.state, q)
	return jsonUnmarshalInto(result, target)
}

func (s *S3Backend) Flush(ctx context.Context, namespace string) error {
	return s.namespace(namespace).doSync(ctx)
}

func (s *S3Backend) Close(_ context.Context) error {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ns := range s.ns {
		<-ns.done
	}
	return nil
}

// S3 object key helpers

func (s *S3Backend) stateKey(namespace string) string { return s.prefix + "/" + namespace + ".json" }
func (s *S3Backend) lockKey(namespace string) string  { return s.prefix + "/" + namespace + ".lock" }

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
	expiresAt, err := time.Parse(time.RFC3339, info.UserMetadata["Expires-At"])
	if err != nil {
		return errors.Wrap(err, "parse lock expiry")
	}
	if time.Now().Before(expiresAt) {
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
	pending []Op
	syncMu  sync.Mutex
	done    chan struct{}
}

const maxTokenRetries = 3

func (n *s3Namespace) doSync(ctx context.Context) error {
	n.syncMu.Lock()
	defer n.syncMu.Unlock()

	n.mu.Lock()
	pending := n.pending
	n.pending = nil
	n.mu.Unlock()

	if len(pending) > 0 {
		if err := n.backend.lockNamespace(ctx, n.name); err != nil {
			n.restorePending(pending)
			return errors.Wrap(err, "lock namespace")
		}
		defer func() {
			if err := n.backend.unlockNamespace(ctx, n.name); err != nil {
				logging.FromContext(ctx).WarnContext(ctx, "unlock failed", "namespace", n.name, "error", err)
			}
		}()
	}

	remote, err := n.loadReplayStore(ctx, pending)
	if err != nil {
		n.restorePending(pending)
		return err
	}

	n.mu.Lock()
	n.state = remote
	for _, o := range n.pending {
		applyOp(n.state, o)
	}
	n.mu.Unlock()

	return nil
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

	remote := make(map[string]any)
	if data != nil {
		if err := json.Unmarshal(data, &remote); err != nil {
			return nil, errors.Wrap(err, "unmarshal state")
		}
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

func (n *s3Namespace) restorePending(ops []Op) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pending = append(ops, n.pending...)
}

func (n *s3Namespace) syncLoop() {
	defer close(n.done)
	ctx := n.backend.ctx
	logger := logging.FromContext(ctx).With("namespace", n.name)
	ticker := time.NewTicker(n.backend.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := n.doSync(ctx); err != nil {
				logger.WarnContext(ctx, "sync failed", "error", err)
			}
		}
	}
}

func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound
}

func isPreconditionFailed(err error) bool {
	var resp minio.ErrorResponse
	return errors.As(err, &resp) && resp.StatusCode == http.StatusPreconditionFailed
}
