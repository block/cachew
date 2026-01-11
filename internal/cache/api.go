// Package cache provides a framework for implementing and registering different cache backends.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/textproto"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"
)

// ErrNotFound is returned when a cache backend is not found.
var ErrNotFound = errors.New("cache backend not found")

var registry = map[string]func(ctx context.Context, config *hcl.Block) (Cache, error){}

// Factory is a function that creates a new cache instance from the given hcl-tagged configuration struct.
type Factory[Config any, C Cache] func(ctx context.Context, config Config) (C, error)

// Register a cache factory function.
func Register[Config any, C Cache](id string, factory Factory[Config, C]) {
	registry[id] = func(ctx context.Context, config *hcl.Block) (Cache, error) {
		var cfg Config
		if err := hcl.UnmarshalBlock(config, &cfg); err != nil {
			return nil, errors.WithStack(err)
		}
		return factory(ctx, cfg)
	}
}

// Create a new cache instance from the given name and configuration.
//
// Will return "ErrNotFound" if the cache backend is not found.
func Create(ctx context.Context, name string, config *hcl.Block) (Cache, error) {
	if factory, ok := registry[name]; ok {
		return errors.WithStack2(factory(ctx, config))
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
	bytes, err := hex.DecodeString(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	if len(bytes) != len(*k) {
		return errors.New("invalid key length")
	}
	copy(k[:], bytes)
	return nil
}

func (k *Key) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

// FilterTransportHeaders returns a copy of the given headers with standard HTTP transport headers removed.
// These headers are typically added by HTTP clients/servers and should not be cached.
func FilterTransportHeaders(headers textproto.MIMEHeader) textproto.MIMEHeader {
	filtered := make(textproto.MIMEHeader)
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
type Cache interface {
	// String describes the Cache implementation.
	String() string
	// Open an existing file in the cache.
	//
	// Expired files SHOULD not be returned.
	// Must return os.ErrNotExist if the file does not exist.
	Open(ctx context.Context, key Key) (io.ReadCloser, textproto.MIMEHeader, error)
	// Create a new file in the cache.
	//
	// If "ttl" is zero, a maximum TTL MUST be used by the implementation.
	//
	// The file MUST not be available for read until completely written and closed.
	//
	// If the context is cancelled the object MUST not be made available in the cache.
	Create(ctx context.Context, key Key, headers textproto.MIMEHeader, ttl time.Duration) (io.WriteCloser, error)
	// Delete a file from the cache.
	//
	// MUST be atomic.
	Delete(ctx context.Context, key Key) error
	// Close the Cache.
	Close() error
}
