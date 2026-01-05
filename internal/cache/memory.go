package cache

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

func init() {
	Register("memory", NewMemoryCache)
}

type MemoryCacheConfig struct {
	LimitMB int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 1GB)." default:"1024"`
	MaxTTL  time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
}

type memoryEntry struct {
	data      []byte
	expiresAt time.Time
}

type memoryCache struct {
	config      MemoryCacheConfig
	mu          sync.RWMutex
	entries     map[Key]*memoryEntry
	currentSize int64
}

func NewMemoryCache(_ context.Context, config MemoryCacheConfig) (Cache, error) {
	return &memoryCache{
		config:  config,
		entries: make(map[Key]*memoryEntry),
	}, nil
}

func (m *memoryCache) Open(_ context.Context, key Key) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[key]
	if !exists {
		return nil, os.ErrNotExist
	}

	if time.Now().After(entry.expiresAt) {
		return nil, os.ErrNotExist
	}

	return io.NopCloser(bytes.NewReader(entry.data)), nil
}

func (m *memoryCache) Create(_ context.Context, key Key, ttl time.Duration) (io.WriteCloser, error) {
	if ttl == 0 {
		ttl = m.config.MaxTTL
	}

	writer := &memoryWriter{
		cache:     m,
		key:       key,
		buf:       &bytes.Buffer{},
		expiresAt: time.Now().Add(ttl),
	}

	return writer, nil
}

func (m *memoryCache) Delete(_ context.Context, key Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.entries[key]
	if !exists {
		return os.ErrNotExist
	}
	m.currentSize -= int64(len(entry.data))
	delete(m.entries, key)
	return nil
}

func (m *memoryCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = nil
	return nil
}

type memoryWriter struct {
	cache     *memoryCache
	key       Key
	buf       *bytes.Buffer
	expiresAt time.Time
	closed    bool
}

func (w *memoryWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("writer closed")
	}
	return errors.WithStack2(w.buf.Write(p))
}

func (w *memoryWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	w.cache.mu.Lock()
	defer w.cache.mu.Unlock()

	newSize := int64(w.buf.Len())
	limitBytes := int64(w.cache.config.LimitMB) * 1024 * 1024

	// Remove old entry size if it exists
	oldSize := int64(0)
	if oldEntry, exists := w.cache.entries[w.key]; exists {
		oldSize = int64(len(oldEntry.data))
	}

	// Evict entries if needed to make room
	if limitBytes > 0 {
		neededSpace := w.cache.currentSize - oldSize + newSize - limitBytes
		if neededSpace > 0 {
			w.cache.evictOldest(neededSpace)
		}
	}

	w.cache.currentSize -= oldSize
	w.cache.entries[w.key] = &memoryEntry{
		data:      w.buf.Bytes(),
		expiresAt: w.expiresAt,
	}
	w.cache.currentSize += newSize

	return nil
}

func (m *memoryCache) evictOldest(neededSpace int64) {
	type entryInfo struct {
		key       Key
		size      int64
		expiresAt time.Time
	}

	var entries []entryInfo
	for k, e := range m.entries {
		entries = append(entries, entryInfo{
			key:       k,
			size:      int64(len(e.data)),
			expiresAt: e.expiresAt,
		})
	}

	// Sort by expiry time (earliest first)
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].expiresAt.After(entries[j].expiresAt) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	freedSpace := int64(0)
	for _, e := range entries {
		if freedSpace >= neededSpace {
			break
		}
		m.currentSize -= e.size
		delete(m.entries, e.key)
		freedSpace += e.size
	}
}
