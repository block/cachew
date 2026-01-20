package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

func init() {
	Register("memory", NewMemory)
}

type MemoryConfig struct {
	LimitMB int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 1GB)." default:"1024"`
	MaxTTL  time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
}

type memoryEntry struct {
	data      []byte
	expiresAt time.Time
	headers   textproto.MIMEHeader
}

type Memory struct {
	config      MemoryConfig
	mu          sync.RWMutex
	entries     map[Key]*memoryEntry
	currentSize int64
}

func NewMemory(ctx context.Context, config MemoryConfig) (*Memory, error) {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing in-memory Cache", "limit-mb", config.LimitMB, "max-ttl", config.MaxTTL)
	return &Memory{
		config:  config,
		entries: make(map[Key]*memoryEntry),
	}, nil
}

func (m *Memory) String() string { return fmt.Sprintf("memory:%dMB", m.config.LimitMB) }

func (m *Memory) Stat(_ context.Context, key Key) (textproto.MIMEHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[key]
	if !exists {
		return nil, os.ErrNotExist
	}

	if time.Now().After(entry.expiresAt) {
		return nil, os.ErrNotExist
	}

	return entry.headers, nil
}

func (m *Memory) Open(_ context.Context, key Key) (io.ReadCloser, textproto.MIMEHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[key]
	if !exists {
		return nil, nil, os.ErrNotExist
	}

	if time.Now().After(entry.expiresAt) {
		return nil, nil, os.ErrNotExist
	}

	return io.NopCloser(bytes.NewReader(entry.data)), entry.headers, nil
}

func (m *Memory) Create(ctx context.Context, key Key, headers textproto.MIMEHeader, ttl time.Duration) (io.WriteCloser, error) {
	if ttl == 0 {
		ttl = m.config.MaxTTL
	}

	writer := &memoryWriter{
		cache:     m,
		key:       key,
		buf:       &bytes.Buffer{},
		expiresAt: time.Now().Add(ttl),
		headers:   headers,
		ctx:       ctx,
	}

	return writer, nil
}

func (m *Memory) Delete(_ context.Context, key Key) error {
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

func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = nil
	return nil
}

type memoryWriter struct {
	cache     *Memory
	key       Key
	buf       *bytes.Buffer
	expiresAt time.Time
	headers   textproto.MIMEHeader
	closed    bool
	ctx       context.Context
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

	// Check if context was cancelled
	if err := w.ctx.Err(); err != nil {
		return errors.Wrap(err, "create operation cancelled")
	}

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
		headers:   w.headers,
	}
	w.cache.currentSize += newSize

	return nil
}

func (m *Memory) evictOldest(neededSpace int64) {
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
