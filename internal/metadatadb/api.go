// Package metadatadb provides an eventually consistent metadata store for
// coordinating state across cachew replicas. Mutations are applied to local
// state immediately and synced periodically to a shared backend. Last flush
// wins — the lock serialises all writes.
package metadatadb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// Config controls the metadata store's sync behaviour.
type Config struct {
	SyncInterval time.Duration `hcl:"sync-interval,optional" help:"How often to sync with the backend." default:"30s"`
	LockTTL      time.Duration `hcl:"lock-ttl,optional" help:"TTL for namespace locks." default:"30s"`
}

// op is an idempotent operation that applies itself to the state.
type op interface {
	apply(state map[string]any)
}

// ErrInvalidToken is returned by Backend.Store when the token does not match
// the current version, indicating a concurrent write.
var ErrInvalidToken = errors.New("invalid token")

// Backend is the pluggable storage and locking layer.
type Backend interface {
	// Load returns the current state and an opaque token identifying the
	// version. The token must be passed to Store to prevent stale writes.
	Load(ctx context.Context, namespace string) (data json.RawMessage, token string, err error)
	// Store persists the state. It must return ErrInvalidToken if the token
	// does not match the current version (i.e. another writer stored since
	// our Load). For S3, the token maps to an ETag with If-Match.
	Store(ctx context.Context, namespace string, data json.RawMessage, token string) error
	Lock(ctx context.Context, namespace string) error
	Unlock(ctx context.Context, namespace string) error
}

// Store is the top-level metadata store.
type Store struct {
	backend Backend
	config  Config
	logger  *slog.Logger
	mu      sync.Mutex
	ns      map[string]*Namespace
	ctx     context.Context
	cancel  context.CancelFunc
}

func New(ctx context.Context, config Config, backend Backend) *Store {
	logger := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	return &Store{
		backend: backend,
		config:  config,
		logger:  logger.With("component", "metadata"),
		ns:      make(map[string]*Namespace),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s *Store) Namespace(name string) *Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns, ok := s.ns[name]; ok {
		return ns
	}
	ns := &Namespace{
		store: s,
		name:  name,
		state: make(map[string]any),
		done:  make(chan struct{}),
	}
	go ns.syncLoop()
	s.ns[name] = ns
	return ns
}

func (s *Store) Close() error {
	s.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ns := range s.ns {
		<-ns.done
	}
	return nil
}

// Namespace is a scoped collection of named data structures. Mutations are
// applied locally immediately and synced periodically to the backend.
type Namespace struct {
	store   *Store
	name    string
	mu      sync.RWMutex
	state   map[string]any
	pending []op
	syncMu  sync.Mutex
	done    chan struct{}
}

// Flush forces an immediate sync with the backend.
func (n *Namespace) Flush(ctx context.Context) error { return n.doSync(ctx) }

func (n *Namespace) apply(o op) {
	n.mu.Lock()
	defer n.mu.Unlock()
	o.apply(n.state)
	n.pending = append(n.pending, o)
}

// Scalar is a single value. Last write wins.
type Scalar[V any] struct {
	ns   *Namespace
	name string
}

// NewScalar creates or retrieves a named scalar within a namespace.
func NewScalar[V any](ns *Namespace, name string) *Scalar[V] {
	return &Scalar[V]{ns: ns, name: name}
}

func (s *Scalar[V]) Set(value V) { s.ns.apply(&scalarSetOp[V]{name: s.name, value: value}) }
func (s *Scalar[V]) Delete()     { s.ns.apply(&scalarDeleteOp{name: s.name}) }

func (s *Scalar[V]) Get() (V, bool) {
	s.ns.mu.RLock()
	defer s.ns.mu.RUnlock()
	raw, ok := s.ns.state[s.name]
	if !ok {
		var zero V
		return zero, false
	}
	return jsonRoundTrip[V](raw), true
}

// Int is an integer with arithmetic operations. All ops are applied
// sequentially — the lock serialises flushes.
type Int struct {
	ns   *Namespace
	name string
}

// NewInt creates or retrieves a named integer within a namespace.
func NewInt(ns *Namespace, name string) *Int {
	return &Int{ns: ns, name: name}
}

func (i *Int) Set(value int64)   { i.ns.apply(&intSetOp{name: i.name, value: value}) }
func (i *Int) Add(delta int64)   { i.ns.apply(&intAddOp{name: i.name, delta: delta}) }
func (i *Int) Mul(factor int64)  { i.ns.apply(&intMulOp{name: i.name, factor: factor}) }
func (i *Int) Div(divisor int64) { i.ns.apply(&intDivOp{name: i.name, divisor: divisor}) }

func (i *Int) Get() int64 {
	i.ns.mu.RLock()
	defer i.ns.mu.RUnlock()
	return toInt64(i.ns.state[i.name])
}

// IntMap is a keyed collection of integer values supporting atomic increment.
// Keys are JSON-marshaled to strings internally.
type IntMap[K comparable] struct {
	ns   *Namespace
	name string
}

// NewIntMap creates or retrieves a named integer map within a namespace.
func NewIntMap[K comparable](ns *Namespace, name string) *IntMap[K] {
	return &IntMap[K]{ns: ns, name: name}
}

func (m *IntMap[K]) Set(key K, value int64) {
	m.ns.apply(&intMapSetOp[K]{name: m.name, key: key, value: value})
}
func (m *IntMap[K]) Add(key K, delta int64) {
	m.ns.apply(&intMapAddOp[K]{name: m.name, key: key, delta: delta})
}
func (m *IntMap[K]) Mul(key K, factor int64) {
	m.ns.apply(&intMapMulOp[K]{name: m.name, key: key, factor: factor})
}
func (m *IntMap[K]) Div(key K, divisor int64) {
	m.ns.apply(&intMapDivOp[K]{name: m.name, key: key, divisor: divisor})
}
func (m *IntMap[K]) Delete(key K) { m.ns.apply(&intMapDeleteOp[K]{name: m.name, key: key}) }

func (m *IntMap[K]) Get(key K) int64 {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		return 0
	}
	return toInt64(raw.(map[string]any)[marshalKey(key)])
}

func (m *IntMap[K]) Keys() []K {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		return nil
	}
	entries := raw.(map[string]any)
	result := make([]K, 0, len(entries))
	for k := range entries {
		result = append(result, unmarshalKey[K](k))
	}
	slices.SortFunc(result, func(a, b K) int {
		ka, kb := marshalKey(a), marshalKey(b)
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}
		return 0
	})
	return result
}

func (m *IntMap[K]) Entries() map[K]int64 {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		return nil
	}
	entries := raw.(map[string]any)
	result := make(map[K]int64, len(entries))
	for k, v := range entries {
		result[unmarshalKey[K](k)] = toInt64(v)
	}
	return result
}

// Set is an unordered collection of unique members. Members are stored as
// JSON-marshaled string keys internally, so any comparable JSON-serializable
// type can be used.
type Set[V comparable] struct {
	ns   *Namespace
	name string
}

// NewSet creates or retrieves a named set within a namespace.
func NewSet[V comparable](ns *Namespace, name string) *Set[V] {
	return &Set[V]{ns: ns, name: name}
}

func (s *Set[V]) Add(member V)    { s.ns.apply(&setAddOp[V]{name: s.name, member: member}) }
func (s *Set[V]) Remove(member V) { s.ns.apply(&setRemoveOp[V]{name: s.name, member: member}) }

func (s *Set[V]) Contains(member V) bool {
	s.ns.mu.RLock()
	defer s.ns.mu.RUnlock()
	raw, ok := s.ns.state[s.name]
	if !ok {
		return false
	}
	_, ok = raw.(map[string]any)[marshalKey(member)]
	return ok
}

func (s *Set[V]) Members() []V {
	s.ns.mu.RLock()
	defer s.ns.mu.RUnlock()
	raw, ok := s.ns.state[s.name]
	if !ok {
		return nil
	}
	m := raw.(map[string]any)
	result := make([]V, 0, len(m))
	for k := range m {
		result = append(result, unmarshalKey[V](k))
	}
	slices.SortFunc(result, func(a, b V) int {
		ka, kb := marshalKey(a), marshalKey(b)
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}
		return 0
	})
	return result
}

func marshalKey[V any](v V) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("metadatadb: marshal key %T: %v", v, err))
	}
	return string(data)
}

func unmarshalKey[V any](s string) V {
	var v V
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		panic(fmt.Sprintf("metadatadb: unmarshal key into %T: %v", v, err))
	}
	return v
}

// Map is a keyed collection of values. Keys are JSON-marshaled to strings
// internally, so any comparable JSON-serializable type can be used.
// Last write per key wins.
type Map[K comparable, V any] struct {
	ns   *Namespace
	name string
}

// NewMap creates or retrieves a named map within a namespace.
func NewMap[K comparable, V any](ns *Namespace, name string) *Map[K, V] {
	return &Map[K, V]{ns: ns, name: name}
}

func (m *Map[K, V]) Set(key K, value V) {
	m.ns.apply(&mapSetOp[K, V]{name: m.name, key: key, value: value})
}

func (m *Map[K, V]) Delete(key K) {
	m.ns.apply(&mapDeleteOp[K]{name: m.name, key: key})
}

func (m *Map[K, V]) Get(key K) (V, bool) {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		var zero V
		return zero, false
	}
	v, ok := raw.(map[string]any)[marshalKey(key)]
	if !ok {
		var zero V
		return zero, false
	}
	return jsonRoundTrip[V](v), true
}

func (m *Map[K, V]) Keys() []K {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		return nil
	}
	entries := raw.(map[string]any)
	result := make([]K, 0, len(entries))
	for k := range entries {
		result = append(result, unmarshalKey[K](k))
	}
	slices.SortFunc(result, func(a, b K) int {
		ka, kb := marshalKey(a), marshalKey(b)
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}
		return 0
	})
	return result
}

func (m *Map[K, V]) Entries() map[K]V {
	m.ns.mu.RLock()
	defer m.ns.mu.RUnlock()
	raw, ok := m.ns.state[m.name]
	if !ok {
		return nil
	}
	entries := raw.(map[string]any)
	result := make(map[K]V, len(entries))
	for k, v := range entries {
		result[unmarshalKey[K](k)] = jsonRoundTrip[V](v)
	}
	return result
}

// List is an append-only ordered collection.
type List[V any] struct {
	ns   *Namespace
	name string
}

// NewList creates or retrieves a named append-only list within a namespace.
func NewList[V any](ns *Namespace, name string) *List[V] {
	return &List[V]{ns: ns, name: name}
}

func (l *List[V]) Append(value V) {
	l.ns.apply(&listAppendOp[V]{name: l.name, value: value})
}

func (l *List[V]) Entries() []V {
	l.ns.mu.RLock()
	defer l.ns.mu.RUnlock()
	raw, ok := l.ns.state[l.name]
	if !ok {
		return nil
	}
	src := raw.([]any)
	result := make([]V, len(src))
	for i, v := range src {
		result[i] = jsonRoundTrip[V](v)
	}
	return result
}

func (l *List[V]) Len() int {
	l.ns.mu.RLock()
	defer l.ns.mu.RUnlock()
	raw, ok := l.ns.state[l.name]
	if !ok {
		return 0
	}
	return len(raw.([]any))
}

// jsonRoundTrip marshals v to JSON then unmarshals into V, handling the
// type mismatch between locally-stored Go values and JSON-deserialized values
// after a sync round-trip.
func jsonRoundTrip[V any](v any) V {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("metadata: marshal %T: %v", v, err))
	}
	var result V
	if err := json.Unmarshal(data, &result); err != nil {
		panic(fmt.Sprintf("metadata: unmarshal into %T: %v", result, err))
	}
	return result
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		return 0
	}
}
