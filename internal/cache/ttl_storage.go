package cache

import (
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

var ttlBucketName = []byte("ttl")

// ttlStorage manages expiration times for cache entries using bbolt.
type ttlStorage struct {
	db *bbolt.DB
}

// newTTLStorage creates a new bbolt-backed TTL storage.
func newTTLStorage(dbPath string) (*ttlStorage, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, errors.Errorf("failed to open bbolt database: %w", err)
	}

	// Create the bucket if it doesn't exist
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(ttlBucketName)
		return errors.WithStack(err)
	})
	if err != nil {
		return nil, errors.Join(errors.Errorf("failed to create ttl bucket: %w", err), db.Close())
	}

	return &ttlStorage{db: db}, nil
}

func (s *ttlStorage) set(key Key, expiresAt time.Time) error {
	expiresAtBytes, err := expiresAt.MarshalBinary()
	if err != nil {
		return errors.Errorf("failed to marshal expiration time: %w", err)
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		return bucket.Put(key[:], expiresAtBytes)
	})
	if err != nil {
		return errors.Errorf("failed to set expiration time: %w", err)
	}

	return nil
}

func (s *ttlStorage) get(key Key) (time.Time, error) {
	var expiresAt time.Time

	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		expiresAtBytes := bucket.Get(key[:])
		if expiresAtBytes == nil {
			return errors.New("key not found")
		}
		return errors.WithStack(expiresAt.UnmarshalBinary(expiresAtBytes))
	})
	if err != nil {
		return time.Time{}, errors.Errorf("failed to get expiration time: %w", err)
	}

	return expiresAt, nil
}

func (s *ttlStorage) delete(key Key) error {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		return bucket.Delete(key[:])
	})
	if err != nil {
		return errors.Errorf("failed to delete expiration time: %w", err)
	}
	return nil
}

func (s *ttlStorage) deleteAll(keys []Key) error {
	if len(keys) == 0 {
		return nil
	}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(ttlBucketName)
		for _, key := range keys {
			if err := bucket.Delete(key[:]); err != nil {
				return errors.Errorf("failed to delete expiration time: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("failed to delete expiration times: %w", err)
	}
	return nil
}

func (s *ttlStorage) walk(fn func(key Key, expiresAt time.Time) error) error {
	err := s.db.View(func(tx *bbolt.Tx) error {
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
	})
	if err != nil {
		return errors.Errorf("failed to walk TTL entries: %w", err)
	}
	return nil
}

func (s *ttlStorage) close() error {
	if err := s.db.Close(); err != nil {
		return errors.Errorf("failed to close bbolt database: %w", err)
	}
	return nil
}
