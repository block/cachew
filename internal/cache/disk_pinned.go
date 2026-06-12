package cache

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/errors"
)

var _ PinnedRangeCache = (*Disk)(nil)

// Pin returns a pin token for a disk entry, but only when it carries a content
// ETag (set by S3 and persisted via tiered backfill). Locally generated entries
// never round-tripped through S3 have no content ETag and are reported absent so
// callers fall back to the authoritative shared tier.
func (d *Disk) Pin(_ context.Context, key Key) (PinnedObject, error) {
	d.replaceMu.RLock()
	defer d.replaceMu.RUnlock()
	headers, err := d.db.getHeaders(d.namespace, key)
	if err != nil {
		return PinnedObject{}, os.ErrNotExist
	}
	etag := headers.Get(ContentETagHeader)
	if etag == "" {
		return PinnedObject{}, os.ErrNotExist
	}
	expiresAt, err := d.db.getTTL(d.namespace, key)
	if err != nil || time.Now().After(expiresAt) {
		return PinnedObject{}, os.ErrNotExist
	}
	info, err := os.Stat(filepath.Join(d.config.Root, d.keyToPath(d.namespace, key)))
	if err != nil {
		return PinnedObject{}, os.ErrNotExist
	}
	headers.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	return PinnedObject{Pin: pinETagPrefix + etag, Size: info.Size(), Headers: headers}, nil
}

// OpenPinnedRange serves bytes from start of the local copy, with the file's
// total size, but only when its stored content ETag matches the pin. Any
// mismatch, missing ETag, expiry, or absent file returns os.ErrNotExist so the
// tiered cache falls through to S3; a start at or past EOF returns
// ErrRangeNotSatisfiable.
//
// The returned *os.File is seeked to start; the caller bounds reads to the range
// length (io.CopyN), which keeps the sendfile zero-copy path.
func (d *Disk) OpenPinnedRange(ctx context.Context, key Key, pin string, start, _ int64) (io.ReadCloser, int64, error) {
	etag, ok := strings.CutPrefix(pin, pinETagPrefix)
	if !ok {
		return nil, 0, errors.Errorf("unsupported pin token %q", pin)
	}
	// Hold replaceMu across the metadata read and file open so the opened fd
	// matches the validated ETag; the fd then survives any later rename-over.
	d.replaceMu.RLock()
	defer d.replaceMu.RUnlock()
	headers, err := d.db.getHeaders(d.namespace, key)
	if err != nil || headers.Get(ContentETagHeader) != etag {
		return nil, 0, os.ErrNotExist
	}
	expiresAt, err := d.db.getTTL(d.namespace, key)
	if err != nil {
		return nil, 0, os.ErrNotExist
	}
	if time.Now().After(expiresAt) {
		return nil, 0, errors.Join(os.ErrNotExist, d.Delete(ctx, key))
	}
	f, err := os.Open(filepath.Join(d.config.Root, d.keyToPath(d.namespace, key)))
	if err != nil {
		return nil, 0, os.ErrNotExist
	}
	info, err := f.Stat()
	if err != nil {
		return nil, 0, errors.Join(errors.Errorf("stat %s: %w", key, err), f.Close())
	}
	if start >= info.Size() {
		return nil, info.Size(), errors.Join(ErrRangeNotSatisfiable, f.Close())
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, 0, errors.Join(errors.Errorf("seek to %d: %w", start, err), f.Close())
	}
	return f, info.Size(), nil
}
