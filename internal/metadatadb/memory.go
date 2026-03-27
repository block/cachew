package metadatadb

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/alecthomas/errors"
)

// MemoryBackend is an in-memory Backend for testing and single-instance
// deployments. It is safe for concurrent use across multiple Store instances.
type MemoryBackend struct {
	mu      sync.Mutex
	data    map[string]json.RawMessage
	tokens  map[string]string
	version atomic.Int64
	locks   map[string]chan struct{}
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data:   make(map[string]json.RawMessage),
		tokens: make(map[string]string),
		locks:  make(map[string]chan struct{}),
	}
}

func (m *MemoryBackend) Load(_ context.Context, namespace string) (json.RawMessage, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[namespace], m.tokens[namespace], nil
}

func (m *MemoryBackend) Store(_ context.Context, namespace string, data json.RawMessage, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tokens[namespace] != token {
		return ErrInvalidToken
	}
	m.data[namespace] = data
	m.tokens[namespace] = strconv.FormatInt(m.version.Add(1), 10)
	return nil
}

func (m *MemoryBackend) Lock(ctx context.Context, namespace string) error {
	for {
		m.mu.Lock()
		ch, locked := m.locks[namespace]
		if !locked {
			m.locks[namespace] = make(chan struct{})
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		}
	}
}

func (m *MemoryBackend) Unlock(_ context.Context, namespace string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.locks[namespace]; ok {
		close(ch)
		delete(m.locks, namespace)
	}
	return nil
}
