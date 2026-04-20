package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/alecthomas/errors"
	"github.com/bmatcuk/doublestar/v4"
)

// SaveOption configures Save.
type SaveOption interface{ applySave(*saveConfig) }

// RestoreOption configures Restore.
type RestoreOption interface{ applyRestore(*restoreConfig) }

// SaveRestoreOption configures both Save and Restore.
type SaveRestoreOption interface {
	SaveOption
	RestoreOption
}

type saveConfig struct {
	ttl          time.Duration
	exclude      []string
	extraHeaders http.Header
	zstdThreads  int
}

type restoreConfig struct {
	zstdThreads int
}

type ttlOpt time.Duration

func (o ttlOpt) applySave(c *saveConfig) { c.ttl = time.Duration(o) }

// WithTTL sets the TTL on the uploaded object. Zero (the default) uses the
// server default.
func WithTTL(d time.Duration) SaveOption { return ttlOpt(d) }

type excludeOpt []string

func (o excludeOpt) applySave(c *saveConfig) { c.exclude = append(c.exclude, o...) }

// WithExclude adds tar --exclude patterns applied during Save.
func WithExclude(patterns ...string) SaveOption { return excludeOpt(patterns) }

type extraHeadersOpt http.Header

func (o extraHeadersOpt) applySave(c *saveConfig) {
	if c.extraHeaders == nil {
		c.extraHeaders = make(http.Header)
	}
	for k, values := range o {
		for _, v := range values {
			c.extraHeaders.Add(k, v)
		}
	}
}

// WithExtraHeaders merges additional headers into the upload request.
func WithExtraHeaders(h http.Header) SaveOption { return extraHeadersOpt(h) }

type zstdThreadsOpt int

func (o zstdThreadsOpt) applySave(c *saveConfig)       { c.zstdThreads = int(o) }
func (o zstdThreadsOpt) applyRestore(c *restoreConfig) { c.zstdThreads = int(o) }

// WithZstdThreads sets zstd parallelism. Zero (the default) uses all CPU
// cores.
func WithZstdThreads(n int) SaveRestoreOption { return zstdThreadsOpt(n) }

// Save archives the given paths within baseDir and uploads the tar+zstd
// stream under key. Any existing object at key is overwritten.
func (c *Client) Save(ctx context.Context, key Key, baseDir string, paths []string, opts ...SaveOption) error {
	var cfg saveConfig
	for _, opt := range opts {
		opt.applySave(&cfg)
	}

	headers := make(http.Header)
	headers.Set("Content-Type", "application/zstd")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(baseDir)+".tar.zst"))
	for k, values := range cfg.extraHeaders {
		for _, v := range values {
			headers.Set(k, v)
		}
	}

	wc, err := c.Create(ctx, key, headers, cfg.ttl)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}
	if err := Archive(ctx, wc, baseDir, paths, cfg.exclude, cfg.zstdThreads); err != nil {
		return errors.Join(err, wc.Close())
	}
	return errors.Wrap(wc.Close(), "failed to close writer")
}

// Restore downloads the archive stored under key and extracts it into
// baseDir. Returns (false, nil) on cache miss so callers can populate
// baseDir and then Save.
func (c *Client) Restore(ctx context.Context, key Key, baseDir string, opts ...RestoreOption) (bool, error) {
	var cfg restoreConfig
	for _, opt := range opts {
		opt.applyRestore(&cfg)
	}

	rc, _, err := c.Open(ctx, key)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "failed to open object")
	}
	defer rc.Close() //nolint:errcheck

	if err := Extract(ctx, rc, baseDir, cfg.zstdThreads); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// HashFiles returns a Key derived from the contents of all regular files
// matched by the given glob patterns. Patterns use doublestar syntax, so **
// matches any number of path segments (e.g. "**/go.sum"). Matches are
// deduplicated and sorted by path, and each file's path and contents are
// folded into the digest so that content or path changes invalidate the key.
// Directories and non-regular matches are skipped. Returns an error if no
// regular files match any pattern, to avoid silently producing a constant
// key on typos.
func HashFiles(patterns ...string) (Key, error) {
	if len(patterns) == 0 {
		return Key{}, errors.New("at least one pattern is required")
	}
	seen := make(map[string]struct{})
	var paths []string
	for _, pattern := range patterns {
		matches, err := doublestar.FilepathGlob(pattern)
		if err != nil {
			return Key{}, errors.Wrapf(err, "invalid pattern %q", pattern)
		}
		for _, match := range matches {
			match = filepath.Clean(match)
			info, err := os.Lstat(match)
			if err != nil {
				return Key{}, errors.Wrapf(err, "failed to stat %q", match)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if _, ok := seen[match]; ok {
				continue
			}
			seen[match] = struct{}{}
			paths = append(paths, match)
		}
	}
	if len(paths) == 0 {
		return Key{}, errors.Errorf("no regular files matched patterns %v", patterns)
	}
	slices.Sort(paths)

	h := sha256.New()
	for _, path := range paths {
		if err := hashFile(h, path); err != nil {
			return Key{}, err
		}
	}
	var key Key
	copy(key[:], h.Sum(nil))
	return key, nil
}

func hashFile(h io.Writer, path string) error {
	if _, err := h.Write([]byte(path)); err != nil {
		return errors.Wrap(err, "failed to hash path")
	}
	if _, err := h.Write([]byte{0}); err != nil {
		return errors.Wrap(err, "failed to hash separator")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", path)
	}
	defer f.Close() //nolint:errcheck
	if _, err := io.Copy(h, f); err != nil {
		return errors.Wrapf(err, "failed to hash %q", path)
	}
	if _, err := h.Write([]byte{0}); err != nil {
		return errors.Wrap(err, "failed to hash separator")
	}
	return nil
}
