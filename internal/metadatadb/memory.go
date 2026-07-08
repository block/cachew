package metadatadb

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// RegisterMemory registers the in-memory metadata backend.
func RegisterMemory(r *Registry) {
	Register(r, "memory", "In-memory metadata store for testing and single-instance deployments",
		func(ctx context.Context, _ MemoryConfig) (*MemoryBackend, error) {
			logging.FromContext(ctx).InfoContext(ctx, "Constructing in-memory metadata backend")
			return NewMemoryBackend(), nil
		},
	)
}

// MemoryConfig is the configuration for the in-memory metadata backend.
type MemoryConfig struct{}

// MemoryBackend is an in-memory Backend for testing and single-instance
// deployments. Ops are applied directly — there is no sync or persistence.
type MemoryBackend struct {
	mu    sync.RWMutex
	state map[string]map[string]any // namespace -> state
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{state: make(map[string]map[string]any)}
}

func (m *MemoryBackend) Apply(_ context.Context, namespace string, ops ...Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := m.ns(namespace)
	for _, o := range ops {
		applyOp(ns, o)
	}
	return nil
}

func (m *MemoryBackend) Query(_ context.Context, namespace string, q ReadOp, target any) error {
	m.mu.RLock()
	// Do not lazy-create the namespace under a read lock — that races with
	// concurrent Apply on a different namespace. queryState handles a nil
	// map by returning zero values, which is the right answer for a
	// namespace that has never been written.
	ns := m.state[namespace]
	result := queryState(ns, q)
	m.mu.RUnlock()
	return errors.Wrap(jsonUnmarshalInto(result, target), "memory query")
}

func (m *MemoryBackend) Flush(_ context.Context, _ string) error { return nil }
func (m *MemoryBackend) Close(_ context.Context) error           { return nil }

func (m *MemoryBackend) ns(namespace string) map[string]any {
	ns, ok := m.state[namespace]
	if !ok {
		ns = make(map[string]any)
		m.state[namespace] = ns
	}
	return ns
}

// jsonUnmarshalInto marshals src to JSON then unmarshals into target,
// bridging between the internal any-typed state and the caller's typed pointer.
func jsonUnmarshalInto(src any, target any) error {
	if src == nil {
		return nil
	}
	data, err := json.Marshal(src)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}
	return errors.Wrap(json.Unmarshal(data, target), "unmarshal")
}
