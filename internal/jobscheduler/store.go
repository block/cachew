package jobscheduler

import (
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

//nolint:gochecknoglobals
var scheduleBucketName = []byte("schedule")

// ScheduleStore persists the last execution time of periodic jobs.
type ScheduleStore interface {
	GetLastRun(key string) (time.Time, bool, error)
	SetLastRun(key string, t time.Time) error
	Close() error
}

type boltScheduleStore struct {
	db *bbolt.DB
}

// NewScheduleStore creates a bbolt-backed schedule store at the given database path.
func NewScheduleStore(dbPath string) (ScheduleStore, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "open scheduler database")
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(scheduleBucketName)
		return errors.WithStack(err)
	}); err != nil {
		return nil, errors.Join(errors.Wrap(err, "create schedule bucket"), db.Close())
	}
	return &boltScheduleStore{db: db}, nil
}

func (s *boltScheduleStore) GetLastRun(key string) (time.Time, bool, error) {
	var t time.Time
	var found bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(scheduleBucketName)
		data := bucket.Get([]byte(key))
		if data == nil {
			return nil
		}
		found = true
		return errors.WithStack(t.UnmarshalBinary(data))
	})
	return t, found, errors.WithStack(err)
}

func (s *boltScheduleStore) SetLastRun(key string, t time.Time) error {
	data, err := t.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshal time")
	}
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		return errors.WithStack(tx.Bucket(scheduleBucketName).Put([]byte(key), data))
	}))
}

func (s *boltScheduleStore) Close() error {
	return errors.WithStack(s.db.Close())
}
