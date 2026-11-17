// Package metadatadb provides a metadata store for coordinating state across cachew replicas.
package metadatadb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// Backend is the metadata store's pluggable storage: Apply writes, Query reads, and Flush refreshes remote state.
type Backend interface {
	// Apply applies one or more write operations to the given namespace.
	Apply(ctx context.Context, namespace string, ops ...Op) error
	// Query executes a read query and unmarshals the result into target, which must be a pointer to the result type.
	Query(ctx context.Context, namespace string, q ReadOp, target any) error
	// Flush forces the namespace to refresh from the backend.
	Flush(ctx context.Context, namespace string) error
	// Close shuts down the backend.
	Close(ctx context.Context) error
}

// Store is the top-level metadata store.
type Store struct {
	backend Backend
	logger  *slog.Logger
	mu      sync.Mutex
	ns      map[string]*Namespace
}

// New creates a new metadata store backed by the given Backend.
func New(ctx context.Context, backend Backend) *Store {
	logger := logging.FromContext(ctx)
	return &Store{
		backend: backend,
		logger:  logger.With("component", "metadata"),
		ns:      make(map[string]*Namespace),
	}
}

// Namespace returns the namespace with the given name, creating it if needed.
func (s *Store) Namespace(name string) *Namespace {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns, ok := s.ns[name]; ok {
		return ns
	}
	ns := &Namespace{
		backend: s.backend,
		name:    name,
	}
	s.ns[name] = ns
	return ns
}

// Close shuts down the store and its backend.
func (s *Store) Close(ctx context.Context) error {
	return errors.Wrap(s.backend.Close(ctx), "close backend")
}

// Namespace is a scoped collection of named data structures.
type Namespace struct {
	backend Backend
	name    string
}

// Flush forces an immediate refresh from the backend.
func (n *Namespace) Flush(ctx context.Context) error {
	return errors.Wrap(n.backend.Flush(ctx, n.name), "flush namespace")
}

func (n *Namespace) apply(ops ...Op) error {
	return errors.Wrap(n.backend.Apply(context.Background(), n.name, ops...), "apply metadata op")
}

func (n *Namespace) query(q ReadOp, target any) {
	_ = n.backend.Query(context.Background(), n.name, q, target) //nolint:errcheck // local backends never fail
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

// Set replaces the scalar's value.
func (s *Scalar[V]) Set(value V) error {
	return s.ns.apply(ScalarSet{Key: s.name, Value: value})
}

// Delete removes the scalar's value so Get reports it as unset.
func (s *Scalar[V]) Delete() error {
	return s.ns.apply(ScalarDelete{Key: s.name})
}

// Get returns the scalar's value and whether it is set.
func (s *Scalar[V]) Get() (V, bool) {
	var result struct {
		Value V
		OK    bool
	}
	s.ns.query(ScalarGet{Key: s.name}, &result)
	return result.Value, result.OK
}

// Int is an integer with arithmetic operations.
type Int struct {
	ns   *Namespace
	name string
}

// NewInt creates or retrieves a named integer within a namespace.
func NewInt(ns *Namespace, name string) *Int {
	return &Int{ns: ns, name: name}
}

// Set replaces the integer's value.
func (i *Int) Set(value int64) error {
	return i.ns.apply(IntSet{Key: i.name, Value: value})
}

// Add adds delta to the integer, treating an unset integer as zero.
func (i *Int) Add(delta int64) error {
	return i.ns.apply(IntAdd{Key: i.name, Delta: delta})
}

// Mul multiplies the integer by factor.
func (i *Int) Mul(factor int64) error {
	return i.ns.apply(IntMul{Key: i.name, Factor: factor})
}

// Div divides the integer by divisor using integer division; a zero divisor is a no-op.
func (i *Int) Div(divisor int64) error {
	return i.ns.apply(IntDiv{Key: i.name, Divisor: divisor})
}

// Get returns the integer's value, or zero if unset.
func (i *Int) Get() int64 {
	var v int64
	i.ns.query(IntGet{Key: i.name}, &v)
	return v
}

// IntMap is a keyed collection of integer values supporting atomic increment.
type IntMap[K comparable] struct {
	ns   *Namespace
	name string
}

// NewIntMap creates or retrieves a named integer map within a namespace.
func NewIntMap[K comparable](ns *Namespace, name string) *IntMap[K] {
	return &IntMap[K]{ns: ns, name: name}
}

// Set replaces the value for key.
func (m *IntMap[K]) Set(key K, value int64) error {
	return m.ns.apply(IntMapSet{Key: m.name, MapKey: key, Value: value})
}

// Add adds delta to the value for key, treating a missing key as zero.
func (m *IntMap[K]) Add(key K, delta int64) error {
	return m.ns.apply(IntMapAdd{Key: m.name, MapKey: key, Delta: delta})
}

// Mul multiplies the value for key by factor.
func (m *IntMap[K]) Mul(key K, factor int64) error {
	return m.ns.apply(IntMapMul{Key: m.name, MapKey: key, Factor: factor})
}

// Div divides the value for key by divisor using integer division; a zero divisor is a no-op.
func (m *IntMap[K]) Div(key K, divisor int64) error {
	return m.ns.apply(IntMapDiv{Key: m.name, MapKey: key, Divisor: divisor})
}

// Delete removes key from the map.
func (m *IntMap[K]) Delete(key K) error {
	return m.ns.apply(IntMapDelete{Key: m.name, MapKey: key})
}

// Get returns the value for key, or zero if missing.
func (m *IntMap[K]) Get(key K) int64 {
	var v int64
	m.ns.query(IntMapGet{Key: m.name, MapKey: key}, &v)
	return v
}

// Keys returns the map's keys sorted by their JSON encoding.
func (m *IntMap[K]) Keys() []K {
	var result []K
	m.ns.query(IntMapKeys{Key: m.name}, &result)
	slices.SortFunc(result, compareKeys[K])
	return result
}

// Entries returns a snapshot of all key/value pairs.
func (m *IntMap[K]) Entries() map[K]int64 {
	var result map[K]int64
	m.ns.query(IntMapEntries{Key: m.name}, &result)
	return result
}

// Set is an unordered collection of unique members.
type Set[V comparable] struct {
	ns   *Namespace
	name string
}

// NewSet creates or retrieves a named set within a namespace.
func NewSet[V comparable](ns *Namespace, name string) *Set[V] {
	return &Set[V]{ns: ns, name: name}
}

// Add inserts member into the set; adding an existing member is a no-op.
func (s *Set[V]) Add(member V) error {
	return s.ns.apply(SetAdd{Key: s.name, Member: member})
}

// Remove deletes member from the set if present.
func (s *Set[V]) Remove(member V) error {
	return s.ns.apply(SetRemove{Key: s.name, Member: member})
}

// Contains reports whether member is in the set.
func (s *Set[V]) Contains(member V) bool {
	var v bool
	s.ns.query(SetContains{Key: s.name, Member: member}, &v)
	return v
}

// Members returns the set's members sorted by their JSON encoding.
func (s *Set[V]) Members() []V {
	var result []V
	s.ns.query(SetMembers{Key: s.name}, &result)
	slices.SortFunc(result, compareKeys[V])
	return result
}

// Map is a keyed collection of values. Last write per key wins.
type Map[K comparable, V any] struct {
	ns   *Namespace
	name string
}

// NewMap creates or retrieves a named map within a namespace.
func NewMap[K comparable, V any](ns *Namespace, name string) *Map[K, V] {
	return &Map[K, V]{ns: ns, name: name}
}

// Set replaces the value for key.
func (m *Map[K, V]) Set(key K, value V) error {
	return m.ns.apply(MapSet{Key: m.name, MapKey: key, Value: value})
}

// Delete removes key from the map.
func (m *Map[K, V]) Delete(key K) error {
	return m.ns.apply(MapDelete{Key: m.name, MapKey: key})
}

// Get returns the value for key and whether it is present.
func (m *Map[K, V]) Get(key K) (V, bool) {
	var result struct {
		Value V
		OK    bool
	}
	m.ns.query(MapGet{Key: m.name, MapKey: key}, &result)
	return result.Value, result.OK
}

// Keys returns the map's keys sorted by their JSON encoding.
func (m *Map[K, V]) Keys() []K {
	var result []K
	m.ns.query(MapKeys{Key: m.name}, &result)
	slices.SortFunc(result, compareKeys[K])
	return result
}

// Entries returns a snapshot of all key/value pairs.
func (m *Map[K, V]) Entries() map[K]V {
	var result map[K]V
	m.ns.query(MapEntries{Key: m.name}, &result)
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

// Append adds value to the end of the list.
func (l *List[V]) Append(value V) error {
	return l.ns.apply(ListAppend{Key: l.name, Value: value})
}

// Entries returns the list's values in append order.
func (l *List[V]) Entries() []V {
	var result []V
	l.ns.query(ListEntries{Key: l.name}, &result)
	return result
}

// Len returns the number of values in the list.
func (l *List[V]) Len() int {
	var v int
	l.ns.query(ListLen{Key: l.name}, &v)
	return v
}

func compareKeys[K comparable](a, b K) int {
	ka, err := json.Marshal(a)
	if err != nil {
		panic(fmt.Sprintf("metadatadb: marshal key %T: %v", a, err))
	}
	kb, err := json.Marshal(b)
	if err != nil {
		panic(fmt.Sprintf("metadatadb: marshal key %T: %v", b, err))
	}
	sa, sb := string(ka), string(kb)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}
