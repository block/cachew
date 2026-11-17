package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// RegisterMemory registers the in-memory cache backend.
func RegisterMemory(r *Registry) {
	Register(
		r,
		"memory",
		"Caches objects in memory, with a maximum size limit and LRU eviction",
		NewMemory,
	)
}

// MemoryConfig is the HCL configuration for the in-memory cache backend.
type MemoryConfig struct {
	LimitMB int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 1GB)." default:"1024"`
	MaxTTL  time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
}

type memoryEntry struct {
	data      []byte
	expiresAt time.Time
	headers   http.Header
}

// Memory is a Cache that holds objects in process memory, evicting the soonest-expiring entries when full.
type Memory struct {
	config      MemoryConfig
	namespace   Namespace
	mu          *sync.RWMutex
	entries     map[Namespace]map[Key]*memoryEntry // namespace -> key -> entry
	currentSize *atomic.Int64
}

// NewMemory creates an empty in-memory cache from config.
func NewMemory(ctx context.Context, config MemoryConfig) (*Memory, error) {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing in-memory Cache", "limit-mb", config.LimitMB, "max-ttl", config.MaxTTL)
	return &Memory{
		config:      config,
		mu:          &sync.RWMutex{},
		entries:     make(map[Namespace]map[Key]*memoryEntry),
		currentSize: &atomic.Int64{},
	}, nil
}

func (m *Memory) String() string { return fmt.Sprintf("memory:%dMB", m.config.LimitMB) }

// Stat returns stored headers plus Content-Length; missing or expired entries return os.ErrNotExist.
func (m *Memory) Stat(_ context.Context, key Key, opts ...Option) (http.Header, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nsEntries, nsExists := m.entries[m.namespace]
	if !nsExists {
		return nil, os.ErrNotExist
	}

	entry, exists := nsEntries[key]
	if !exists {
		return nil, os.ErrNotExist
	}

	if time.Now().After(entry.expiresAt) {
		return nil, os.ErrNotExist
	}

	headers := maps.Clone(entry.headers)
	headers.Set("Content-Length", strconv.Itoa(len(entry.data)))
	if h, err := conditionalShortCircuit(headers, opts); err != nil {
		return h, err
	}
	return headers, nil
}

// Open returns a reader over the stored bytes, honouring conditional and Range options.
func (m *Memory) Open(_ context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nsEntries, nsExists := m.entries[m.namespace]
	if !nsExists {
		return nil, nil, os.ErrNotExist
	}

	entry, exists := nsEntries[key]
	if !exists {
		return nil, nil, os.ErrNotExist
	}

	if time.Now().After(entry.expiresAt) {
		return nil, nil, os.ErrNotExist
	}

	headers := maps.Clone(entry.headers)
	headers.Set("Content-Length", strconv.Itoa(len(entry.data)))
	if h, err := conditionalShortCircuit(headers, opts); err != nil {
		return nil, h, err
	}

	start, length, partial, rangeErr := rangeShortCircuit(headers, int64(len(entry.data)), opts)
	if rangeErr != nil {
		return nil, headers, rangeErr
	}
	data := entry.data
	if partial {
		data = data[start : start+length]
	}
	return io.NopCloser(bytes.NewReader(data)), headers, nil
}

// Create buffers writes in memory and stores the entry on Close; a zero ttl uses MaxTTL.
func (m *Memory) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	if ttl == 0 {
		ttl = m.config.MaxTTL
	}

	now := time.Now()
	// Clone (to avoid concurrent map writes) and drop transport headers.
	clonedHeaders := httputil.FilterHeaders(headers, httputil.TransportHeaders...)
	if clonedHeaders.Get("Last-Modified") == "" {
		clonedHeaders.Set("Last-Modified", now.UTC().Format(http.TimeFormat))
	}
	if err := setCreateETag(clonedHeaders, opts...); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancelCause(ctx)

	writer := &memoryWriter{
		cache:     m,
		namespace: m.namespace,
		key:       key,
		buf:       &bytes.Buffer{},
		expiresAt: now.Add(ttl),
		headers:   clonedHeaders,
		ctx:       ctx,
		cancel:    cancel,
	}

	return writer, nil
}

// UpdateMetadata merges headers and resets the TTL without copying the stored bytes.
func (m *Memory) UpdateMetadata(_ context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) error {
	if ttl == 0 {
		ttl = m.config.MaxTTL
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.entries[m.namespace][key]
	if !exists || time.Now().After(entry.expiresAt) {
		return os.ErrNotExist
	}
	merged, err := updatedHeaders(entry.headers, headers, opts)
	if err != nil {
		return err
	}
	entry.headers = merged
	entry.expiresAt = time.Now().Add(ttl)
	return nil
}

// Delete removes an entry, returning os.ErrNotExist if it is absent.
func (m *Memory) Delete(_ context.Context, key Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	nsEntries, nsExists := m.entries[m.namespace]
	if !nsExists {
		return os.ErrNotExist
	}

	entry, exists := nsEntries[key]
	if !exists {
		return os.ErrNotExist
	}
	m.currentSize.Add(-int64(len(entry.data)))
	delete(nsEntries, key)
	return nil
}

// Invalidate deletes the entry, treating a missing entry as success.
func (m *Memory) Invalidate(ctx context.Context, key Key) error {
	err := m.Delete(ctx, key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.WithStack(err)
}

// Close releases the entry map; the cache must not be written to afterwards.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = nil
	return nil
}

// Stats reports entry count and bytes held across all namespaces, with capacity from LimitMB.
func (m *Memory) Stats(_ context.Context) (Stats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	totalObjects := int64(0)
	for _, nsEntries := range m.entries {
		totalObjects += int64(len(nsEntries))
	}

	return Stats{
		Objects:  totalObjects,
		Size:     m.currentSize.Load(),
		Capacity: int64(m.config.LimitMB) * 1024 * 1024,
	}, nil
}

func (m *Memory) evictOldest(neededSpace int64) {
	type entryInfo struct {
		namespace Namespace
		key       Key
		size      int64
		expiresAt time.Time
	}

	var entries []entryInfo
	for ns, nsEntries := range m.entries {
		for k, e := range nsEntries {
			entries = append(entries, entryInfo{
				namespace: ns,
				key:       k,
				size:      int64(len(e.data)),
				expiresAt: e.expiresAt,
			})
		}
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
		m.currentSize.Add(-e.size)
		delete(m.entries[e.namespace], e.key)
		freedSpace += e.size
	}
}

type memoryWriter struct {
	cache     *Memory
	namespace Namespace
	key       Key
	buf       *bytes.Buffer
	expiresAt time.Time
	headers   http.Header
	closed    bool
	ctx       context.Context
	cancel    context.CancelCauseFunc
}

func (w *memoryWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("writer closed")
	}
	n, err := w.buf.Write(p)
	return n, errors.WithStack(err)
}

func (w *memoryWriter) Abort(err error) error {
	w.cancel(err)
	return w.Close()
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

	// Ensure namespace map exists
	if w.cache.entries[w.namespace] == nil {
		w.cache.entries[w.namespace] = make(map[Key]*memoryEntry)
	}
	nsEntries := w.cache.entries[w.namespace]

	// Remove old entry size if it exists
	oldSize := int64(0)
	if oldEntry, exists := nsEntries[w.key]; exists {
		oldSize = int64(len(oldEntry.data))
	}

	// Evict entries if needed to make room
	if limitBytes > 0 {
		neededSpace := w.cache.currentSize.Load() - oldSize + newSize - limitBytes
		if neededSpace > 0 {
			w.cache.evictOldest(neededSpace)
		}
	}

	w.cache.currentSize.Add(-oldSize)
	// Copy the buffer data to avoid holding a reference to the buffer's internal slice
	data := make([]byte, w.buf.Len())
	copy(data, w.buf.Bytes())
	w.buf.Reset()
	nsEntries[w.key] = &memoryEntry{
		data:      data,
		expiresAt: w.expiresAt,
		headers:   w.headers,
	}
	w.cache.currentSize.Add(newSize)

	return nil
}

// Namespace creates a namespaced view of the memory cache.
func (m *Memory) Namespace(namespace Namespace) Cache {
	c := *m
	c.namespace = namespace
	return &c
}

// ListNamespaces returns all unique namespaces in the memory cache.
func (m *Memory) ListNamespaces(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	namespaces := make([]string, 0, len(m.entries))
	for ns := range m.entries {
		if ns != "" {
			namespaces = append(namespaces, string(ns))
		}
	}
	return namespaces, nil
}
