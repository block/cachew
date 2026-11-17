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

// ReadAtCloser is an io.ReaderAt that must be closed.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// PendingReader is implemented by a Writer whose bytes can be read before commit.
//
// The object stays invisible to Open and Stat until Close succeeds.
type PendingReader interface {
	// OpenPending returns a reader over the uncommitted bytes, or ErrPendingReadUnsupported.
	//
	// It must be called before Close or Abort; the reader sees every returned Write and outlives the commit.
	OpenPending() (ReadAtCloser, error)
}

// ErrPendingReadUnsupported is returned by PendingReader.OpenPending when a write cannot be read before commit.
var ErrPendingReadUnsupported = errors.New("pending reads unsupported")

// ErrNotFound is returned when a cache backend is not found.
var ErrNotFound = errors.New("cache backend not found")

// Option configures conditional parameters on a cache Open or Stat.
type Option = client.RequestOption

// RequestOptions is the resolved set of conditional and range parameters for an Open or Stat.
type RequestOptions = client.RequestOptions

// NewRequestOptions applies opts and returns the resulting RequestOptions.
func NewRequestOptions(opts ...Option) RequestOptions { return client.NewRequestOptions(opts...) }

// RangeOutcome classifies how a Range request should be answered, as returned by RequestOptions.ResolveRange.
type RangeOutcome = client.RangeOutcome

const (
	// RangeFull indicates the full object should be served.
	RangeFull = client.RangeFull
	// RangePartial indicates a single satisfiable byte range.
	RangePartial = client.RangePartial
	// RangeNotSatisfiable indicates the range lies outside the object.
	RangeNotSatisfiable = client.RangeNotSatisfiable
)

// IfMatch sets the If-Match precondition; Open/Stat return ErrPreconditionFailed if the stored ETag differs.
func IfMatch(etag string) Option { return client.IfMatch(etag) }

// IfNoneMatch sets the If-None-Match precondition; Open/Stat return ErrNotModified if the stored ETag matches.
func IfNoneMatch(etag string) Option { return client.IfNoneMatch(etag) }

// Range requests a single half-open byte range [start, end) from Open; a negative end means "to the end".
//
// The returned headers carry a Content-Range; Open returns ErrRangeNotSatisfiable if the range lies outside
// the object. Stat ignores Range.
func Range(start, end int64) Option { return client.Range(start, end) }

// IfRange applies Range only when the stored ETag matches etag; otherwise the full object is returned.
func IfRange(etag string) Option { return client.IfRange(etag) }

// ErrNotModified is returned by Open/Stat when an If-None-Match precondition is satisfied.
var ErrNotModified = client.ErrNotModified

// ErrPreconditionFailed is returned by Open/Stat when an If-Match precondition is not met.
var ErrPreconditionFailed = client.ErrPreconditionFailed

// ErrRangeNotSatisfiable is returned by Open when a requested Range lies outside the object.
var ErrRangeNotSatisfiable = client.ErrRangeNotSatisfiable

// ErrStatsUnavailable is returned when a cache backend cannot provide statistics.
var ErrStatsUnavailable = client.ErrStatsUnavailable

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, vars map[string]string) (Cache, error)
}

// Registry maps cache backend names to their configuration schemas and factories.
type Registry struct {
	registry map[string]registryEntry
}

// NewRegistry returns an empty Registry for Register to populate.
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

// Schema returns the schema for all registered cache backends, each wrapped as a "cache <name> { ... }" block.
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

// Exists reports whether a cache backend with the given name is registered.
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
	// Namespace returns a view of this cache whose operations all use the given namespace prefix.
	Namespace(namespace Namespace) Cache
	// Stat returns the headers of an existing object; expired or missing objects MUST return os.ErrNotExist.
	//
	// Conditional opts are evaluated against the stored ETag: a satisfied If-None-Match returns
	// ErrNotModified (with headers); a failed If-Match returns ErrPreconditionFailed.
	Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error)
	// Open an existing object; headers MUST include Last-Modified, expired or missing objects return os.ErrNotExist.
	//
	// Conditional opts behave as in Stat (ErrNotModified has no body). A Range yields only those bytes with matching
	// Content-Range and Content-Length, or ErrRangeNotSatisfiable with "Content-Range: bytes */N" if out of bounds.
	Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error)
	// Create a new object; if "ttl" is zero the implementation MUST use its maximum TTL.
	//
	// The object MUST NOT be readable until completely written and closed, and MUST NOT be made available at all if
	// the context is cancelled.
	Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error)
	// UpdateMetadata merges headers and resets the TTL (zero means maximum) without rewriting the body.
	//
	// Returns os.ErrNotExist for missing or expired entries. MUST NOT change the ETag, which tiers compare to detect
	// stale copies. An IfMatch opt prevents refreshing an entry replaced since the caller validated it.
	UpdateMetadata(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) error
	// Delete an object from the cache; MUST be atomic.
	Delete(ctx context.Context, key Key) error
	// Invalidate evicts a stale local copy from the cache.
	//
	// Unlike Delete, it MUST NOT remove authoritative shared storage. Missing objects count as invalidated.
	Invalidate(ctx context.Context, key Key) error
	// Stats returns health and usage statistics for the cache.
	Stats(ctx context.Context) (Stats, error)
	// ListNamespaces returns all unique namespaces in the cache in order.
	ListNamespaces(ctx context.Context) ([]string, error)
	// Close the Cache.
	Close() error
}

// WriteFunc writes an entry via fn, committing it on success and aborting it if fn returns an error.
func WriteFunc(ctx context.Context, c Cache, key Key, headers http.Header, ttl time.Duration, fn func(w io.Writer) error, opts ...Option) error {
	w, err := c.Create(ctx, key, headers, ttl, opts...)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := fn(w); err != nil {
		return errors.Join(err, w.Abort(err))
	}
	return errors.WithStack(w.Close())
}
