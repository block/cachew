package cache

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"

	"github.com/block/sfptc/internal/logging"
)

func init() {
	Register("disk", NewDisk)
}

type DiskConfig struct {
	Root          string        `hcl:"root" help:"Root directory for the disk storage."`
	LimitMB       int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 10GB)." default:"10240"`
	MaxTTL        time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
	EvictInterval time.Duration `hcl:"evict-interval,optional" help:"Interval at which to check files for eviction (defaults to 1 minute)." default:"1m"`
}

type Disk struct {
	logger      *slog.Logger
	config      DiskConfig
	ttl         *ttlStorage
	size        atomic.Int64
	runEviction chan struct{}
	stop        context.CancelFunc
}

var _ Cache = (*Disk)(nil)

// NewDisk creates a new disk-based cache instance.
//
// config.Root MUST be set.
//
// This [Cache] implementation stores cache entries under a directory. If total usage exceeds the limit, entries are
// evicted based on their last access time. TTLs are stored in a bbolt database. If an entry exceeds its
// TTL or the default, it is evicted. The implementation is safe for concurrent use within a single Go process.
func NewDisk(ctx context.Context, config DiskConfig) (*Disk, error) {
	// Validate config
	if config.Root == "" {
		return nil, errors.New("root directory is required")
	}
	err := kong.ApplyDefaults(&config)
	if err != nil {
		return nil, errors.Errorf("failed to apply defaults: %w", err)
	}
	config.Root, err = filepath.Abs(config.Root)
	if err != nil {
		return nil, errors.Errorf("failed to get absolute path for cache root: %w", err)
	}

	if err := os.MkdirAll(config.Root, 0750); err != nil {
		return nil, errors.Errorf("failed to create cache root: %w", err)
	}

	// Open TTL storage
	ttl, err := newTTLStorage(filepath.Join(config.Root, "metadata.db"))
	if err != nil {
		return nil, errors.Errorf("failed to create TTL storage: %w", err)
	}

	// Determine the initial size.
	var size int64
	err = filepath.Walk(config.Root, func(_ string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		// Skip metadata.db file
		if info.Name() == "metadata.db" {
			return nil
		}
		size += info.Size()
		return nil
	})
	if err != nil {
		return nil, errors.Errorf("failed to walk cache root: %w", err)
	}

	logger := logging.FromContext(ctx)

	ctx, stop := context.WithCancel(ctx)

	disk := &Disk{
		logger:      logger,
		config:      config,
		ttl:         ttl,
		runEviction: make(chan struct{}),
		stop:        stop,
	}
	disk.size.Store(size)

	go disk.evictionLoop(ctx)

	return disk, nil
}

func (d *Disk) String() string { return "disk:" + d.config.Root }

func (d *Disk) Close() error {
	d.stop()
	if d.ttl != nil {
		return d.ttl.close()
	}
	return nil
}

func (d *Disk) Size() int64 {
	return d.size.Load()
}

func (d *Disk) Create(_ context.Context, key Key, ttl time.Duration) (io.WriteCloser, error) {
	if ttl > d.config.MaxTTL || ttl == 0 {
		ttl = d.config.MaxTTL
	}

	path := d.keyToPath(key)
	fullPath := filepath.Join(d.config.Root, path)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, errors.Errorf("failed to create directory %s: %w", dir, err)
	}

	tempPath := fullPath + ".tmp"
	f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, errors.Errorf("failed to create temp file: %w", err)
	}

	expiresAt := time.Now().Add(ttl)

	return &diskWriter{
		disk:      d,
		file:      f,
		key:       key,
		path:      fullPath,
		tempPath:  tempPath,
		expiresAt: expiresAt,
	}, nil
}

func (d *Disk) Delete(_ context.Context, key Key) error {
	path := d.keyToPath(key)
	fullPath := filepath.Join(d.config.Root, path)

	// Check if file is expired
	expired := false
	expiresAt, err := d.ttl.get(key)
	if err == nil && time.Now().After(expiresAt) {
		expired = true
	}

	info, err := os.Stat(fullPath)
	if err != nil {
		return errors.Errorf("failed to stat file: %w", err)
	}

	if err := os.Remove(fullPath); err != nil {
		return errors.Errorf("failed to remove file: %w", err)
	}

	// Remove TTL metadata
	if err := d.ttl.delete(key); err != nil {
		return errors.Errorf("failed to delete TTL metadata: %w", err)
	}

	d.size.Add(-info.Size())

	if expired {
		return errors.Errorf("%s: %w", path, fs.ErrNotExist)
	}
	return nil
}

func (d *Disk) Open(ctx context.Context, key Key) (io.ReadCloser, error) {
	path := d.keyToPath(key)
	fullPath := filepath.Join(d.config.Root, path)

	f, err := os.Open(fullPath)
	if err != nil {
		return nil, errors.Errorf("failed to open file: %w", err)
	}

	expiresAt, err := d.ttl.get(key)
	if err != nil {
		return nil, errors.Join(errors.Errorf("failed to get expiration time: %w", err), f.Close())
	}

	now := time.Now()
	if now.After(expiresAt) {
		return nil, errors.Join(fs.ErrNotExist, f.Close(), d.Delete(ctx, key))
	}

	// Reset expiration time to implement LRU
	ttl := min(expiresAt.Sub(now), d.config.MaxTTL)
	newExpiresAt := now.Add(ttl)

	if err := d.ttl.set(key, newExpiresAt); err != nil {
		return nil, errors.Join(errors.Errorf("failed to update expiration time: %w", err), f.Close())
	}

	return f, nil
}

func (d *Disk) keyToPath(key Key) string {
	hexKey := key.String()
	// Use first two hex digits as directory, full hex as filename
	return filepath.Join(hexKey[:2], hexKey)
}

func (d *Disk) evictionLoop(ctx context.Context) {
	ticker := time.NewTicker(d.config.EvictInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.evict(); err != nil {
				d.logger.ErrorContext(ctx, "eviction failed", "error", err)
			}
		case <-d.runEviction:
			if err := d.evict(); err != nil {
				d.logger.ErrorContext(ctx, "eviction failed", "error", err)
			}
		}
	}
}

func (d *Disk) evict() error {
	type fileInfo struct {
		key        Key
		path       string
		size       int64
		expiresAt  time.Time
		accessedAt time.Time
	}

	var remainingFiles []fileInfo
	var expiredKeys []Key
	now := time.Now()

	err := d.ttl.walk(func(key Key, expiresAt time.Time) error {
		path := d.keyToPath(key)
		fullPath := filepath.Join(d.config.Root, path)

		info, err := os.Stat(fullPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				expiredKeys = append(expiredKeys, key)
			}
			return nil
		}

		if now.After(expiresAt) {
			if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return errors.Errorf("failed to delete expired file %s: %w", path, err)
			}
			expiredKeys = append(expiredKeys, key)
			d.size.Add(-info.Size())
		} else {
			remainingFiles = append(remainingFiles, fileInfo{
				key:        key,
				path:       path,
				size:       info.Size(),
				expiresAt:  expiresAt,
				accessedAt: info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return errors.Errorf("failed to walk TTL entries: %w", err)
	}

	if err := d.ttl.deleteAll(expiredKeys); err != nil {
		return errors.Errorf("failed to delete TTL metadata: %w", err)
	}

	limitBytes := int64(d.config.LimitMB) * 1024 * 1024
	if d.size.Load() <= limitBytes {
		return nil
	}

	// Sort by access time (oldest first)
	sort.Slice(remainingFiles, func(i, j int) bool {
		return remainingFiles[i].accessedAt.Before(remainingFiles[j].accessedAt)
	})

	var sizeEvictedKeys []Key
	for _, f := range remainingFiles {
		if d.size.Load() <= limitBytes {
			break
		}

		fullPath := filepath.Join(d.config.Root, f.path)
		if err := os.Remove(fullPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errors.Errorf("failed to delete file during size eviction %s: %w", f.path, err)
		}
		sizeEvictedKeys = append(sizeEvictedKeys, f.key)
		d.size.Add(-f.size)
	}

	if err := d.ttl.deleteAll(sizeEvictedKeys); err != nil {
		return errors.Errorf("failed to delete TTL metadata: %w", err)
	}

	return nil
}

type diskWriter struct {
	disk      *Disk
	file      *os.File
	key       Key
	path      string
	tempPath  string
	expiresAt time.Time
	size      int64
}

func (w *diskWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, errors.WithStack(err)
}

func (w *diskWriter) Close() error {
	if err := w.file.Close(); err != nil {
		return errors.Errorf("failed to close file: %w", err)
	}

	if err := os.Rename(w.tempPath, w.path); err != nil {
		return errors.Errorf("failed to rename temp file: %w", err)
	}

	if err := w.disk.ttl.set(w.key, w.expiresAt); err != nil {
		return errors.Join(errors.Errorf("failed to set expiration time: %w", err), os.Remove(w.path))
	}

	w.disk.size.Add(w.size)

	select {
	case w.disk.runEviction <- struct{}{}:
	default:
	}

	return nil
}
