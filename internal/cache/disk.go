package cache

import (
	"context"
	"fmt"
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
	"github.com/pkg/xattr"

	"github.com/block/sfptc/internal/logging"
)

const expiresAtXAttr = "user.expires-at"

func init() {
	Register("disk", NewDisk)
}

type DiskConfig struct {
	Root          string        `hcl:"root" help:"Root directory for the disk storage."`
	LimitMB       int           `hcl:"limit-mb,optional" help:"Maximum size of the disk cache in megabytes (defaults to 1GB)." default:"1024"`
	MaxTTL        time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the disk cache (defaults to 1 hour)." default:"1h"`
	EvictInterval time.Duration `hcl:"evict-interval,optional" help:"Interval at which to check files for eviction (defaults to 1 minute)." default:"1m"`
}

type Disk struct {
	logger      *slog.Logger
	config      DiskConfig
	root        *os.Root
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
// evicted based on their last access time. TTLs are stored in extended file attributes (xattr). If an entry exceeds its
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

	// Check if the filesystem supports xattr's by creating a temporary test file.
	f, err := os.CreateTemp(config.Root, ".xattr-test-*")
	if err != nil {
		return nil, errors.Errorf("failed to create xattr test file: %w", err)
	}
	testFile := f.Name()
	if err := xattr.FSet(f, "user.limit-mb", fmt.Appendf(nil, "%x", config.LimitMB)); err != nil {
		return nil, errors.Join(errors.Errorf("fatal: xattrs are not supported on %s: %w", config.Root, err), f.Close(), os.Remove(testFile))
	}
	_ = f.Close()
	_ = os.Remove(testFile)

	// Open an os.Root to "chroot" access.
	root, err := os.OpenRoot(config.Root)
	if err != nil {
		return nil, errors.Errorf("failed to open cache root: %w", err)
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
		root:        root,
		runEviction: make(chan struct{}),
		stop:        stop,
	}
	disk.size.Store(size)

	go disk.evictionLoop(ctx)

	return disk, nil
}

func (d *Disk) Close() error {
	d.stop()
	return d.root.Close()
}

func (d *Disk) Size() int64 {
	return d.size.Load()
}

func (d *Disk) Create(_ context.Context, path string, ttl time.Duration) (io.WriteCloser, error) {
	if ttl > d.config.MaxTTL || ttl == 0 {
		ttl = d.config.MaxTTL
	}

	path = d.normalizePath(path)

	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := d.root.MkdirAll(dir, 0755); err != nil {
			return nil, errors.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	tempPath := path + ".tmp"
	f, err := d.root.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, errors.Errorf("failed to create temp file: %w", err)
	}

	expiresAt := time.Now().Add(ttl)

	return &diskWriter{
		disk:      d,
		file:      f,
		path:      path,
		tempPath:  tempPath,
		expiresAt: expiresAt,
	}, nil
}

func (d *Disk) Delete(_ context.Context, path string) error {
	path = d.normalizePath(path)

	// Check if file is expired
	expired := false
	fullPath := filepath.Join(d.config.Root, path)
	expiresAtBytes, err := xattr.Get(fullPath, expiresAtXAttr)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fs.ErrNotExist
		}
		// Continue with deletion even if we can't read xattr
	} else {
		var expiresAt time.Time
		if err := expiresAt.UnmarshalBinary(expiresAtBytes); err == nil {
			if time.Now().After(expiresAt) {
				expired = true
			}
		}
	}

	info, err := d.root.Stat(path)
	if err != nil {
		return errors.Errorf("failed to stat file: %w", err)
	}

	if err := d.root.Remove(path); err != nil {
		return errors.Errorf("failed to remove file: %w", err)
	}

	d.size.Add(-info.Size())

	if expired {
		return errors.Errorf("%s: %w", path, fs.ErrNotExist)
	}
	return nil
}

func (d *Disk) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	path = d.normalizePath(path)

	f, err := d.root.Open(path)
	if err != nil {
		return nil, errors.Errorf("failed to open file: %w", err)
	}

	expiresAtBytes, err := xattr.FGet(f, expiresAtXAttr)
	if err != nil {
		return nil, errors.Join(errors.Errorf("failed to get expiration time: %w", err), f.Close())
	}

	var expiresAt time.Time
	if err := expiresAt.UnmarshalBinary(expiresAtBytes); err != nil {
		return nil, errors.Join(errors.Errorf("failed to unmarshal expiration time: %w", err), f.Close())
	}

	now := time.Now()
	if now.After(expiresAt) {
		return nil, errors.Join(fs.ErrNotExist, f.Close(), d.Delete(ctx, path))
	}

	// Reset expiration time to implement LRU
	ttl := min(expiresAt.Sub(now), d.config.MaxTTL)
	newExpiresAt := now.Add(ttl)
	newExpiresAtBytes, err := newExpiresAt.MarshalBinary()
	if err != nil {
		return nil, errors.Join(errors.Errorf("failed to marshal new expiration time: %w", err), f.Close())
	}

	if err := xattr.FSet(f, expiresAtXAttr, newExpiresAtBytes); err != nil {
		return nil, errors.Join(errors.Errorf("failed to update expiration time: %w", err), f.Close())
	}

	return f, nil
}

func (d *Disk) normalizePath(path string) string {
	path = filepath.Clean(path)
	if filepath.IsAbs(path) {
		path = path[1:]
	}
	return path
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
		path       string
		size       int64
		expiresAt  time.Time
		accessedAt time.Time
	}

	var files []fileInfo
	now := time.Now()

	err := filepath.Walk(d.config.Root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(d.config.Root, path)
		if err != nil {
			return err
		}

		expiresAtBytes, err := xattr.Get(path, expiresAtXAttr)
		if err != nil {
			return nil //nolint:nilerr
		}

		var expiresAt time.Time
		if err := expiresAt.UnmarshalBinary(expiresAtBytes); err != nil {
			return nil //nolint:nilerr
		}

		files = append(files, fileInfo{
			path:       relPath,
			size:       info.Size(),
			expiresAt:  expiresAt,
			accessedAt: info.ModTime(),
		})

		return nil
	})

	if err != nil {
		return errors.Errorf("failed to walk cache directory: %w", err)
	}

	var remainingFiles []fileInfo

	for _, f := range files {
		if now.After(f.expiresAt) {
			if err := d.Delete(context.Background(), f.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return errors.Errorf("failed to delete expired file %s: %w", f.path, err)
			}
		} else {
			remainingFiles = append(remainingFiles, f)
		}
	}

	limitBytes := int64(d.config.LimitMB) * 1024 * 1024
	if d.size.Load() <= limitBytes {
		return nil
	}

	// Sort by access time (oldest first)
	sort.Slice(remainingFiles, func(i, j int) bool {
		return remainingFiles[i].accessedAt.Before(remainingFiles[j].accessedAt)
	})

	for _, f := range remainingFiles {
		if d.size.Load() <= limitBytes {
			break
		}

		if err := d.Delete(context.Background(), f.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return errors.Errorf("failed to delete file during size eviction %s: %w", f.path, err)
		}
	}

	return nil
}

type diskWriter struct {
	disk      *Disk
	file      *os.File
	path      string
	tempPath  string
	expiresAt time.Time
	size      int64
}

func (w *diskWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *diskWriter) Close() error {
	expiresAtBytes, err := w.expiresAt.MarshalBinary()
	if err != nil {
		return errors.Join(errors.Errorf("failed to marshal expiration time: %w", err), w.file.Close())
	}

	if err := xattr.FSet(w.file, expiresAtXAttr, expiresAtBytes); err != nil {
		return errors.Join(errors.Errorf("failed to set expiration time: %w", err), w.file.Close())
	}

	if err := w.file.Close(); err != nil {
		return errors.Errorf("failed to close file: %w", err)
	}

	if err := w.disk.root.Rename(w.tempPath, w.path); err != nil {
		return errors.Errorf("failed to rename temp file: %w", err)
	}

	w.disk.size.Add(w.size)

	select {
	case w.disk.runEviction <- struct{}{}:
	default:
	}

	return nil
}
