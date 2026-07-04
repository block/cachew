// Package s3 implements the S3 metadata backend as an append-only journal of
// immutable op segments plus a compacted rollup snapshot, replayed in a
// canonical order all replicas agree on. See docs/metadatadb-s3.md for the
// design and its invariants.
package s3

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/s3client"
)

const (
	// metadataPrefix starts with "." to avoid collisions with cache
	// namespaces, which are validated to not start with ".".
	metadataPrefix = ".metadata"
	rollupName     = "rollup.json"

	defaultSyncInterval = 15 * time.Second
	// The effective age threshold is max(minAgeThreshold, 2 × sync-interval).
	minAgeThreshold  = 30 * time.Second
	segmentThreshold = 256
	sustainTicks     = 2
	// putTimeout must stay under the age threshold: a PUT retried after its
	// segment was folded and deleted would re-create it and replay it twice.
	putTimeout       = 15 * time.Second
	fetchConcurrency = 8
	maxJitter        = 500 * time.Millisecond
)

// Register registers the S3 metadata backend. The clientProvider supplies the
// shared minio client constructed from the global s3 config block.
func Register(r *metadatadb.Registry, clientProvider s3client.ClientProvider) {
	metadatadb.Register(r, "s3", "Stores metadata state in S3 as an op journal with rollup compaction",
		func(ctx context.Context, config Config) (*Backend, error) {
			return New(ctx, clientProvider, config)
		},
	)
}

// Config configures the S3 metadata backend.
type Config struct {
	Bucket       string        `hcl:"bucket" help:"S3 bucket name."`
	SyncInterval time.Duration `hcl:"sync-interval,optional" help:"Interval between background sync ticks." default:"15s"`
	LockTTL      time.Duration `hcl:"lock-ttl,optional" help:"Deprecated; the backend is lock-free."`
}

// Backend stores metadata as immutable op segments in S3, compacted into a
// rollup snapshot. Writes are synchronous group-committed segment PUTs;
// queries serve local state refreshed by a per-namespace background sync
// tick. It requires GET/PUT/LIST/DELETE permissions and a store with strong
// list-after-write consistency and enforced conditional writes (AWS S3,
// MinIO).
type Backend struct {
	client           *minio.Client
	bucket           string
	syncInterval     time.Duration
	ageThreshold     time.Duration
	segmentThreshold int
	jitter           func() time.Duration
	initialTick      bool

	// ctx carries the logger; cancelled on Close to stop all goroutines.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu     sync.Mutex
	ns     map[string]*namespace
	closed bool
}

var _ metadatadb.Backend = (*Backend)(nil)

// New creates an S3 metadata backend, verifying the bucket exists.
func New(ctx context.Context, clientProvider s3client.ClientProvider, config Config) (*Backend, error) {
	if config.SyncInterval == 0 {
		config.SyncInterval = defaultSyncInterval
	}
	client, err := clientProvider()
	if err != nil {
		return nil, errors.Wrap(err, "create S3 client")
	}
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "check bucket exists")
	}
	if !exists {
		return nil, errors.Errorf("bucket %s does not exist", config.Bucket)
	}

	ageThreshold := max(minAgeThreshold, 2*config.SyncInterval)
	logging.FromContext(ctx).InfoContext(ctx, "Constructing S3 metadata backend",
		"bucket", config.Bucket, "prefix", metadataPrefix, "sync-interval", config.SyncInterval,
		"age-threshold", ageThreshold)

	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &Backend{
		client:           client,
		bucket:           config.Bucket,
		syncInterval:     config.SyncInterval,
		ageThreshold:     ageThreshold,
		segmentThreshold: segmentThreshold,
		jitter:           func() time.Duration { return rand.N(maxJitter) }, //nolint:gosec // duplicate-work reduction, not security
		initialTick:      true,
		ctx:              bgCtx,
		cancel:           cancel,
		ns:               make(map[string]*namespace),
	}, nil
}

func (b *Backend) Apply(ctx context.Context, namespace string, ops ...metadatadb.Op) error {
	if len(ops) == 0 {
		return nil
	}
	return b.namespace(namespace).apply(ctx, ops)
}

func (b *Backend) Query(_ context.Context, namespace string, q metadatadb.ReadOp, target any) error {
	n := b.namespace(namespace)
	n.stateMu.RLock()
	defer n.stateMu.RUnlock()
	return errors.Wrap(metadatadb.QueryStateInto(n.state, q, target), "s3 query")
}

// Flush runs a sync tick now and returns its error. Its LIST starts after
// this call, so every write durable before the call is observed.
func (b *Backend) Flush(ctx context.Context, namespace string) error {
	return b.namespace(namespace).flush(ctx)
}

// Close stops all per-namespace goroutines. It is idempotent.
func (b *Backend) Close(_ context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	b.cancel()
	b.wg.Wait()
	return nil
}

func (b *Backend) namespace(name string) *namespace {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n, ok := b.ns[name]; ok {
		return n
	}
	n := &namespace{
		b:       b,
		name:    name,
		state:   make(map[string]any),
		cache:   make(map[string]*cacheEntry),
		applyCh: make(chan *applyReq, 64),
		flushCh: make(chan chan error),
	}
	b.ns[name] = n
	if !b.closed {
		b.wg.Add(2)
		go n.writeLoop()
		go n.runLoop()
	}
	return n
}

type namespace struct {
	b    *Backend
	name string

	applyCh chan *applyReq
	flushCh chan chan error

	// stateMu guards state and cache; the tick's replay runs unlocked
	// between its input-snapshot and swap critical sections.
	stateMu sync.RWMutex
	state   map[string]any
	cache   map[string]*cacheEntry

	// Owned exclusively by the runLoop goroutine.
	rollup     *heldRollup
	lastNewest time.Time
	stall      int
	sustain    int
}

// cacheEntry is one live segment: unlisted (zero lm) from writer insert until
// its first LIST supplies the canonical stamp.
type cacheEntry struct {
	ops    []metadatadb.Op
	lm     time.Time
	listed bool
	// insertedAt is monotonic, only ever compared to local LIST start times.
	insertedAt time.Time
}

// heldRollup keeps state raw so each rebuild unmarshals a fresh map.
type heldRollup struct {
	etag  string
	mark  mark
	state []byte
}

// rootNamespaceDir stands in for the empty root namespace: MinIO rejects the
// "//" an empty component would produce, and cache namespaces never start
// with ".", so it cannot collide.
const rootNamespaceDir = ".root"

func (n *namespace) dir() string {
	if n.name == "" {
		return rootNamespaceDir
	}
	return n.name
}

func (n *namespace) prefix() string    { return metadataPrefix + "/" + n.dir() + "/" }
func (n *namespace) segPrefix() string { return n.prefix() + "segment-" }
func (n *namespace) rollupKey() string { return n.prefix() + rollupName }
func (n *namespace) legacyKey() string { return metadataPrefix + "/" + n.name + ".json" }

func errStatus(err error, status int) bool {
	resp, ok := errors.AsType[minio.ErrorResponse](err)
	return ok && resp.StatusCode == status
}

func isNotFound(err error) bool           { return errStatus(err, 404) }
func isNotModified(err error) bool        { return errStatus(err, 304) }
func isPreconditionFailed(err error) bool { return errStatus(err, 412) }

// isConflict matches AWS's 409 for racing conditional writes; like 412 it
// means a benignly lost election.
func isConflict(err error) bool { return errStatus(err, 409) }
