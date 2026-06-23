// Package cache provides a framework for implementing and registering different cache backends.
package cache

import (
	"context"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/client"
)

// Namespace identifies a logical partition within a cache or metadata store.
type Namespace = client.Namespace

// ValidateNamespace checks that a namespace name is valid.
func ValidateNamespace(name string) error { return errors.WithStack(client.ValidateNamespace(name)) }

// ParseNamespace validates and returns a Namespace from a plain string.
func ParseNamespace(name string) (Namespace, error) {
	return errors.WithStack2(client.ParseNamespace(name))
}

// Writer extends io.WriteCloser with abort semantics for cache writes.
type Writer = client.CacheWriter

// ErrNotFound is returned when a cache backend is not found.
var ErrNotFound = errors.New("cache backend not found")

// Option configures conditional parameters on a cache Open or Stat.
type Option = client.RequestOption

// IfMatch sets the If-Match precondition. Open/Stat return ErrPreconditionFailed
// if the stored ETag does not match.
func IfMatch(etag string) Option { return client.IfMatch(etag) }

// IfNoneMatch sets the If-None-Match precondition. Open/Stat return
// ErrNotModified when the stored ETag matches.
func IfNoneMatch(etag string) Option { return client.IfNoneMatch(etag) }

// Range requests a single half-open byte range [start, end) from Open. A
// negative end means "to the end of the object". The returned headers carry a
// Content-Range; Open returns ErrRangeNotSatisfiable if the range lies outside
// the object. Stat ignores Range.
func Range(start, end int64) Option { return client.Range(start, end) }

// IfRange gates Range on the stored ETag: the range is only applied when etag
// matches, otherwise the full object is returned.
func IfRange(etag string) Option { return client.IfRange(etag) }

// ErrNotModified is returned by Open/Stat when an If-None-Match precondition is
// satisfied.
var ErrNotModified = client.ErrNotModified

// ErrPreconditionFailed is returned by Open/Stat when an If-Match precondition
// is not met.
var ErrPreconditionFailed = client.ErrPreconditionFailed

// ErrRangeNotSatisfiable is returned by Open when a requested Range lies outside
// the object.
var ErrRangeNotSatisfiable = client.ErrRangeNotSatisfiable

// ErrStatsUnavailable is returned when a cache backend cannot provide statistics.
var ErrStatsUnavailable = client.ErrStatsUnavailable

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, vars map[string]string) (Cache, error)
}

type Registry struct {
	registry map[string]registryEntry
}

func NewRegistry() *Registry {
	return &Registry{
		registry: make(map[string]registryEntry),
	}
}

// Factory is a function that creates a new cache instance from the given hcl-tagged configuration struct.
type Factory[Config any, C Cache] func(ctx context.Context, config Config) (C, error)

// Register a cache factory function.
func Register[Config any, C Cache](r *Registry, id, description string, factory Factory[Config, C]) {
	var c Config
	schema, err := hcl.BlockSchema(id, &c)
	if err != nil {
		panic(err)
	}
	block := schema.Entries[0].(*hcl.Block) //nolint:errcheck // This seems spurious
	block.Comments = hcl.CommentList{description}
	r.registry[id] = registryEntry{
		schema: block,
		factory: func(ctx context.Context, config *hcl.Block, vars map[string]string) (Cache, error) {
			var cfg Config
			transformer := func(defaultValue string) string {
				return os.Expand(defaultValue, func(key string) string { return vars[key] })
			}
			if err := hcl.UnmarshalBlock(config, &cfg, hcl.WithDefaultTransformer(transformer)); err != nil {
				return nil, errors.WithStack(err)
			}
			return factory(ctx, cfg)
		},
	}
}

// Schema returns the schema for all registered cache backends.
// Each entry is wrapped as a "cache <name> { ... }" block.
func (r *Registry) Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range r.registry {
		wrapped := &hcl.Block{
			Name:     "cache",
			Labels:   append([]string{entry.schema.Name}, entry.schema.Labels...),
			Body:     entry.schema.Body,
			Comments: entry.schema.Comments,
			Repeated: true,
		}
		ast.Entries = append(ast.Entries, wrapped)
	}
	return ast
}

func (r *Registry) Exists(name string) bool {
	_, ok := r.registry[name]
	return ok
}

// Create a new cache instance from the given name and configuration.
//
// Will return "ErrNotFound" if the cache backend is not found.
func (r *Registry) Create(ctx context.Context, name string, config *hcl.Block, vars map[string]string) (Cache, error) {
	if entry, ok := r.registry[name]; ok {
		return errors.WithStack2(entry.factory(ctx, config, vars))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

// Key represents a unique identifier for a cached object.
type Key = client.Key

// ParseKey from its hex-encoded string form.
func ParseKey(key string) (Key, error) { return errors.WithStack2(client.ParseKey(key)) }

// NewKey returns the SHA256 of s.
func NewKey(s string) Key { return client.NewKey(s) }

// Stats contains health and usage statistics for a cache.
type Stats = client.Stats

// A Cache knows how to retrieve, create and delete objects from a cache.
//
// Objects in the cache are not guaranteed to persist and implementations may delete them at any time.
type Cache interface {
	// String describes the Cache implementation.
	String() string
	// Namespace creates a namespaced view of this cache.
	// All operations on the returned cache will use the given namespace prefix.
	Namespace(namespace Namespace) Cache
	// Stat returns the headers of an existing object in the cache.
	//
	// Expired files MUST not be returned.
	// Must return os.ErrNotExist if the file does not exist.
	//
	// Conditional opts are evaluated against the stored ETag: a satisfied
	// If-None-Match returns ErrNotModified (with headers); a failed If-Match
	// returns ErrPreconditionFailed.
	Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error)
	// Open an existing file in the cache.
	//
	// Expired files MUST NOT be returned.
	// The returned headers MUST include a Last-Modified header.
	// Must return os.ErrNotExist if the file does not exist.
	//
	// Conditional opts are evaluated against the stored ETag: a satisfied
	// If-None-Match returns ErrNotModified (with headers, no body); a failed
	// If-Match returns ErrPreconditionFailed.
	//
	// A Range opt requests a single byte range: on success the returned
	// headers carry Content-Range and a Content-Length of the range, and the
	// reader yields only those bytes. A range outside the object returns
	// ErrRangeNotSatisfiable (with headers carrying Content-Range: bytes */N).
	Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error)
	// Create a new file in the cache.
	//
	// If "ttl" is zero, a maximum TTL MUST be used by the implementation.
	//
	// The file MUST NOT be available for read until completely written and closed.
	//
	// If the context is cancelled the object MUST NOT be made available in the cache.
	Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (Writer, error)
	// Delete a file from the cache.
	//
	// MUST be atomic.
	Delete(ctx context.Context, key Key) error
	// Stats returns health and usage statistics for the cache.
	Stats(ctx context.Context) (Stats, error)
	// ListNamespaces returns all unique namespaces in the cache in order.
	ListNamespaces(ctx context.Context) ([]string, error)
	// Close the Cache.
	Close() error
}

// WriteFunc is a convenience wrapper around Cache.Create that handles aborting
// the write on error. The provided function receives a writer; if it returns an
// error the cache entry is discarded. On success the entry is committed.
func WriteFunc(ctx context.Context, c Cache, key Key, headers http.Header, ttl time.Duration, fn func(w io.Writer) error) error {
	w, err := c.Create(ctx, key, headers, ttl)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := fn(w); err != nil {
		return errors.Join(err, w.Abort(err))
	}
	return errors.WithStack(w.Close())
}
