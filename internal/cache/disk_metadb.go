package cache

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

var (
	ttlBucketName     = []byte("ttl")
	headersBucketName = []byte("headers")
)

// diskMetaDB manages expiration times and headers for cache entries using bbolt.
type diskMetaDB struct {
	db *bbolt.DB
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
		return nil
	}); err != nil {
		return nil, errors.Join(errors.Errorf("failed to create buckets: %w", err), db.Close())
	}

	return &diskMetaDB{db: db}, nil
}

func (s *diskMetaDB) setTTL(key Key, expiresAt time.Time) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		return errors.WithStack(ttlBucket.Put(key[:], ttlBytes))
	}))
}

func (s *diskMetaDB) set(key Key, expiresAt time.Time, headers http.Header) error {
	ttlBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal TTL: %w", err)
	}

	headersBytes, err := json.Marshal(headers)
	if err != nil {
		return errors.Errorf("failed to encode headers: %w", err)
	}

	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Put(key[:], ttlBytes); err != nil {
			return errors.WithStack(err)
		}

		headersBucket := tx.Bucket(headersBucketName)
		return errors.WithStack(headersBucket.Put(key[:], headersBytes))
	}))
}

func (s *diskMetaDB) getTTL(key Key) (time.Time, error) {
	var expiresAt time.Time
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		ttlBytes := bucket.Get(key[:])
		if ttlBytes == nil {
			return fs.ErrNotExist
		}
		return errors.WithStack(expiresAt.UnmarshalBinary(ttlBytes))
	})
	return expiresAt, errors.WithStack(err)
}

func (s *diskMetaDB) getHeaders(key Key) (http.Header, error) {
	var headers http.Header
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(headersBucketName)
		headersBytes := bucket.Get(key[:])
		if headersBytes == nil {
			return fs.ErrNotExist
		}
		return errors.WithStack(json.Unmarshal(headersBytes, &headers))
	})
	return headers, errors.WithStack(err)
}

func (s *diskMetaDB) delete(key Key) error {
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		if err := ttlBucket.Delete(key[:]); err != nil {
			return errors.WithStack(err)
		}

		headersBucket := tx.Bucket(headersBucketName)
		return errors.WithStack(headersBucket.Delete(key[:]))
	}))
}

func (s *diskMetaDB) deleteAll(keys []Key) error {
	if len(keys) == 0 {
		return nil
	}
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		ttlBucket := tx.Bucket(ttlBucketName)
		headersBucket := tx.Bucket(headersBucketName)

		for _, key := range keys {
			if err := ttlBucket.Delete(key[:]); err != nil {
				return errors.Errorf("failed to delete TTL: %w", err)
			}
			if err := headersBucket.Delete(key[:]); err != nil {
				return errors.Errorf("failed to delete headers: %w", err)
			}
		}
		return nil
	}))
}

func (s *diskMetaDB) walk(fn func(key Key, expiresAt time.Time) error) error {
	return errors.WithStack(s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			if len(k) != 32 {
				return nil
			}
			var key Key
			copy(key[:], k)
			var expiresAt time.Time
			if err := expiresAt.UnmarshalBinary(v); err != nil {
				return nil //nolint:nilerr
			}
			return fn(key, expiresAt)
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
