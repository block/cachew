package metadatadb

import (
	"context"
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

// MemoryBackend is an in-memory Backend for testing and single-instance deployments, with no sync or persistence.
type MemoryBackend struct {
	mu    sync.RWMutex
	state map[string]map[string]any // namespace -> state
}

// NewMemoryBackend creates an empty in-memory backend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{state: make(map[string]map[string]any)}
}

// Apply implements Backend by applying ops to the namespace's state in order under a single lock.
func (m *MemoryBackend) Apply(_ context.Context, namespace string, ops ...Op) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := m.ns(namespace)
	for _, o := range ops {
		ApplyOp(ns, o)
	}
	return nil
}

// Query implements Backend.
func (m *MemoryBackend) Query(_ context.Context, namespace string, q ReadOp, target any) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return errors.Wrap(QueryStateInto(m.state[namespace], q, target), "memory query")
}

// Flush implements Backend; it is a no-op because there is no remote state.
func (m *MemoryBackend) Flush(_ context.Context, _ string) error { return nil }

// Close implements Backend; it is a no-op.
func (m *MemoryBackend) Close(_ context.Context) error { return nil }

func (m *MemoryBackend) ns(namespace string) map[string]any {
	ns, ok := m.state[namespace]
	if !ok {
		ns = make(map[string]any)
		m.state[namespace] = ns
	}
	return ns
}
