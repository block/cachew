package cache

import (
	"context"
	"io"
	"net/http"

	"github.com/alecthomas/errors"
)

// ErrPinStale is returned when the pinned object revision no longer exists
// (e.g. the snapshot was regenerated mid-download). Callers should re-pin and
// restart the transfer.
var ErrPinStale = errors.New("pinned object revision is stale")

// ErrRangeNotSatisfiable is returned when the requested range starts at or past
// the end of the object.
var ErrRangeNotSatisfiable = errors.New("pinned range not satisfiable")

// ContentETagHeader carries an object's content ETag across cache tiers. S3
// surfaces its native (content-derived) ETag under this header; tiered backfill
// then stores it alongside the disk copy, so a disk-served range and an
// S3-served range of the same revision report the same pin token.
const ContentETagHeader = "X-Cachew-Content-Etag"

// pinETagPrefix tags ETag-based pin tokens. Keeping the token prefixed lets a
// "version:" form (S3 VersionId) be added later without changing the wire format.
const pinETagPrefix = "etag:"

// ETagPin builds a pin token from a content ETag.
func ETagPin(etag string) string { return pinETagPrefix + etag }

// PinnedObject describes an immutable revision of a cached object. The opaque
// Pin token lets a client fetch byte ranges that all resolve to the same
// revision, so parallel ranges stitch correctly even across cachew replicas.
type PinnedObject struct {
	// Pin is an opaque token identifying the object revision. It is echoed back
	// to the client and supplied on subsequent range requests.
	Pin string
	// Size is the total object size in bytes.
	Size int64
	// Headers are the stored object headers (Content-Type, Last-Modified, etc.).
	Headers http.Header
}

// PinnedRangeCache is implemented by caches that can serve a byte range from a
// specific immutable object revision. It backs parallel snapshot downloads:
// the client pins once, then fetches ranges concurrently against that revision.
type PinnedRangeCache interface {
	// Pin stats the object and returns an opaque revision token plus size and
	// headers. Returns os.ErrNotExist if the object is absent.
	Pin(ctx context.Context, key Key) (PinnedObject, error)
	// OpenPinnedRange returns a reader positioned at start for the revision
	// identified by pin, along with the object's total size. The caller must
	// read at most min(end, total-1)-start+1 bytes; a backend that can bound
	// cheaply (S3 via Range) does so, while a disk backend returns the seeked
	// file so the caller's io.CopyN keeps the sendfile path. Returns
	// ErrPinStale if the revision changed, ErrRangeNotSatisfiable if start is at
	// or past total, or os.ErrNotExist if absent.
	OpenPinnedRange(ctx context.Context, key Key, pin string, start, end int64) (r io.ReadCloser, total int64, err error)
}
