package cache

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

//nolint:gochecknoglobals
var (
	ttlBucketName       = []byte("ttl")
	headersBucketName   = []byte("headers")
	namespaceBucketName = []byte("namespace")
)

// diskMetaDB manages expiration times and headers for cache entries using bbolt.
type diskMetaDB struct {
	db *bbolt.DB
}

// compositeKey creates a unique database key from namespace and cache key.
func compositeKey(namespace string, key Key) []byte {
	if namespace == "" {
		return key[:]
	}
	// Format: "namespace/hexkey"
	hexKey := key.String()
	return []byte(namespace + "/" + hexKey)
}

// newDiskMetaDB creates a new bbolt-backed metadata storage for the disk cache.
func newDiskMetaDB(dbPath string) (*diskMetaDB, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.Errorf("failed to open bbolt database: %w", err)
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(ttlBucketName); err != nil {
			return errors.WithStack(err)
		}
		if _, err := tx.CreateBucketIfNotExists(headersBucketName); err != nil {
			return errors.WithStack(err)
		}
		if _, err := tx.CreateBucketIfNotExists(namespaceBucketName); err != nil {
			return errors.WithStack(err)
		}
		return nil
	}); err != nil {
		return nil, errors.Join(errors.Errorf("failed to create buckets: %w", err), db.Close())
	}

	return &diskMetaDB{db: db}, nil
}

func (s *diskMetaDB) setTTL(namespace string, key Key, expiresAt time.Time) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	dbKey := compositeKey(namespace, key)
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		return errors.WithStack(ttlBucket.Put(dbKey, ttlBytes))
	}))
}

func (s *diskMetaDB) set(key Key, namespace string, expiresAt time.Time, headers http.Header) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	headersBytes, err := json.Marshal(headers)
	if err != nil {
		return errors.Errorf("failed to encode headers: %w", err)
	}

	dbKey := compositeKey(namespace, key)
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Put(dbKey, ttlBytes); err != nil {
			return errors.WithStack(err)
		}

		headersBucket := tx.Bucket(headersBucketName)
		if err := headersBucket.Put(dbKey, headersBytes); err != nil {
			return errors.WithStack(err)
		}

		namespaceBucket := tx.Bucket(namespaceBucketName)
		return errors.WithStack(namespaceBucket.Put(dbKey, []byte(namespace)))
	}))
}

func (s *diskMetaDB) getTTL(namespace string, key Key) (time.Time, error) {
	var expiresAt time.Time
	dbKey := compositeKey(namespace, key)
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		ttlBytes := bucket.Get(dbKey)
		if ttlBytes == nil {
			return fs.ErrNotExist
		}
		return errors.WithStack(expiresAt.UnmarshalBinary(ttlBytes))
	})
	return expiresAt, errors.WithStack(err)
}

func (s *diskMetaDB) getHeaders(namespace string, key Key) (http.Header, error) {
	var headers http.Header
	dbKey := compositeKey(namespace, key)
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(headersBucketName)
		headersBytes := bucket.Get(dbKey)
		if headersBytes == nil {
			return fs.ErrNotExist
		}
		return errors.WithStack(json.Unmarshal(headersBytes, &headers))
	})
	return headers, errors.WithStack(err)
}

func (s *diskMetaDB) delete(namespace string, key Key) error {
	dbKey := compositeKey(namespace, key)
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Delete(dbKey); err != nil {
			return errors.WithStack(err)
		}

		headersBucket := tx.Bucket(headersBucketName)
		if err := headersBucket.Delete(dbKey); err != nil {
			return errors.WithStack(err)
		}

		namespaceBucket := tx.Bucket(namespaceBucketName)
		return errors.WithStack(namespaceBucket.Delete(dbKey))
	}))
}

func (s *diskMetaDB) deleteAll(entries []evictEntryKey) error {
	if len(entries) == 0 {
		return nil
	}
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		headersBucket := tx.Bucket(headersBucketName)
		namespaceBucket := tx.Bucket(namespaceBucketName)

		for _, entry := range entries {
			dbKey := compositeKey(entry.namespace, entry.key)
			if err := ttlBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete TTL: %w", err)
			}
			if err := headersBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete headers: %w", err)
			}
			if err := namespaceBucket.Delete(dbKey); err != nil {
				return errors.Errorf("failed to delete namespace: %w", err)
			}
		}
		return nil
	}))
}

func (s *diskMetaDB) walk(fn func(key Key, namespace string, expiresAt time.Time) error) error {
	return errors.WithStack(s.db.View(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if ttlBucket == nil {
			return nil
		}
		return ttlBucket.ForEach(func(k, v []byte) error {
			var namespace string
			var key Key

			// Check format: composite "namespace/hexkey" or raw 32-byte key
			slashIdx := bytes.IndexByte(k, '/')
			switch {
			case slashIdx >= 0:
				// Composite key: "namespace/hexkey"
				namespace = string(k[:slashIdx])
				hexKey := string(k[slashIdx+1:])
				if len(hexKey) != 64 {
					return nil
				}
				if err := key.UnmarshalText([]byte(hexKey)); err != nil {
					return nil //nolint:nilerr
				}
			case len(k) == 32:
				// Raw key (empty namespace)
				copy(key[:], k)
			default:
				return nil
			}

			var expiresAt time.Time
			if err := expiresAt.UnmarshalBinary(v); err != nil {
				return nil //nolint:nilerr
			}

			return fn(key, namespace, expiresAt)
		})
	}))
}

func (s *diskMetaDB) count() (int64, error) {
	var count int64
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		if bucket == nil {
			return nil
		}
		count = int64(bucket.Stats().KeyN)
		return nil
	})
	return count, errors.WithStack(err)
}

func (s *diskMetaDB) close() error {
	if err := s.db.Close(); err != nil {
		return errors.Errorf("failed to close bbolt database: %w", err)
	}
	return nil
}

func (s *diskMetaDB) listNamespaces() ([]string, error) {
	namespaceSet := make(map[string]bool)
	err := s.db.View(func(tx *bbolt.Tx) error {
		namespaceBucket := tx.Bucket(namespaceBucketName)
		if namespaceBucket == nil {
			return nil
		}
		return namespaceBucket.ForEach(func(_, v []byte) error {
			if len(v) > 0 {
				namespaceSet[string(v)] = true
			}
			return nil
		})
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	namespaces := make([]string, 0, len(namespaceSet))
	for ns := range namespaceSet {
		namespaces = append(namespaces, ns)
	}
	return namespaces, nil
}
