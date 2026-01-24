// Package cache provides a framework for implementing and registering different cache backends.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"
)

// ErrNotFound is returned when a cache backend is not found.
var ErrNotFound = errors.New("cache backend not found")

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block) (Cache, error)
}

var registry = map[string]registryEntry{}

// Factory is a function that creates a new cache instance from the given hcl-tagged configuration struct.
type Factory[Config any, C Cache] func(ctx context.Context, config Config) (C, error)

// Register a cache factory function.
func Register[Config any, C Cache](id, description string, factory Factory[Config, C]) {
	var c Config
	schema, err := hcl.BlockSchema(id, &c)
	if err != nil {
		panic(err)
	}
	block := schema.Entries[0].(*hcl.Block) //nolint:errcheck // This seems spurious
	block.Comments = hcl.CommentList{description}
	registry[id] = registryEntry{
		schema: block,
		factory: func(ctx context.Context, config *hcl.Block) (Cache, error) {
			var cfg Config
			if err := hcl.UnmarshalBlock(config, &cfg); err != nil {
				return nil, errors.WithStack(err)
			}
			return factory(ctx, cfg)
		},
	}
}

// Schema returns the schema for all registered cache backends.
func Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range registry {
		ast.Entries = append(ast.Entries, entry.schema)
	}
	return ast
}

// Create a new cache instance from the given name and configuration.
//
// Will return "ErrNotFound" if the cache backend is not found.
func Create(ctx context.Context, name string, config *hcl.Block) (Cache, error) {
	if entry, ok := registry[name]; ok {
		return errors.WithStack2(entry.factory(ctx, config))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

// Key represents a unique identifier for a cached object.
type Key [32]byte

// ParseKey from its hex-encoded string form.
func ParseKey(key string) (Key, error) {
	var k Key
	return k, k.UnmarshalText([]byte(key))
}

func NewKey(url string) Key { return Key(sha256.Sum256([]byte(url))) }

func (k *Key) String() string { return hex.EncodeToString(k[:]) }

func (k *Key) UnmarshalText(text []byte) error {
	// Try to decode as SHA256 hex encoded string
	if len(text) == 64 {
		bytes, err := hex.DecodeString(string(text))
		if err == nil && len(bytes) == len(*k) {
			copy(k[:], bytes)
			return nil
		}
	}
	// If not valid hex, treat as string and SHA256 it
	*k = NewKey(string(text))
	return nil
}

func (k *Key) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

// FilterTransportHeaders returns a copy of the given headers with standard HTTP transport headers removed.
// These headers are typically added by HTTP clients/servers and should not be cached.
func FilterTransportHeaders(headers http.Header) http.Header {
	filtered := make(http.Header)
	for key, values := range headers {
		// Skip standard HTTP headers added by transport layer or that shouldn't be cached
		if key == "Content-Length" || key == "Date" || key == "Accept-Encoding" ||
			key == "User-Agent" || key == "Transfer-Encoding" || key == "Time-To-Live" {
			continue
		}
		filtered[key] = values
	}
	return filtered
}

// A Cache knows how to retrieve, create and delete objects from a cache.
//
// Objects in the cache are not guaranteed to persist and implementations may delete them at any time.
type Cache interface {
	// String describes the Cache implementation.
	String() string
	// Stat returns the headers of an existing object in the cache.
	//
	// Expired files MUST not be returned.
	// Must return os.ErrNotExist if the file does not exist.
	Stat(ctx context.Context, key Key) (http.Header, error)
	// Open an existing file in the cache.
	//
	// Expired files MUST NOT be returned.
	// The returned headers MUST include a Last-Modified header.
	// Must return os.ErrNotExist if the file does not exist.
	Open(ctx context.Context, key Key) (io.ReadCloser, http.Header, error)
	// Create a new file in the cache.
	//
	// If "ttl" is zero, a maximum TTL MUST be used by the implementation.
	//
	// The file MUST NOT be available for read until completely written and closed.
	//
	// If the context is cancelled the object MUST NOT be made available in the cache.
	Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (io.WriteCloser, error)
	// Delete a file from the cache.
	//
	// MUST be atomic.
	Delete(ctx context.Context, key Key) error
	// Close the Cache.
	Close() error
}
