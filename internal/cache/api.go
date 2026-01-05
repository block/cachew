// Package cache provides a framework for implementing and registering different cache backends.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"
)

var registry = map[string]func(config *hcl.Block) (Cache, error){}

// Factory is a function that creates a new cache instance from the given hcl-tagged configuration struct.
type Factory[Config any, C Cache] func(ctx context.Context, config Config) (C, error)

// Register a cache factory function.
func Register[Config any, C Cache](id string, factory Factory[Config, C]) {
	registry[id] = func(config *hcl.Block) (Cache, error) {
		var cfg Config
		if err := hcl.UnmarshalBlock(config, &cfg); err != nil {
			return nil, errors.WithStack(err)
		}
		return factory(context.Background(), cfg)
	}
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

// A Cache knows how to retrieve, create and delete objects from a cache.
type Cache interface {
	// Open an existing file in the cache.
	//
	// Expired files SHOULD not be returned.
	// Must return os.ErrNotExist if the file does not exist.
	Open(ctx context.Context, key Key) (io.ReadCloser, error)
	// Create a new file in the cache.
	//
	// If "ttl" is zero, a maximum TTL MUST be used by the implementation.
	//
	// The file MUST not be available for read until completely written and closed.
	Create(ctx context.Context, key Key, ttl time.Duration) (io.WriteCloser, error)
	// Delete a file from the cache.
	//
	// MUST be atomic.
	Delete(ctx context.Context, key Key) error
	// Close the Cache.
	Close() error
}
