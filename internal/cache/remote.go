package cache

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// HeaderFunc returns headers to attach to each outgoing request.
type HeaderFunc = client.HeaderFunc

// NewHTTPClient creates an *http.Client that attaches headerFunc headers
// to every outgoing request.
func NewHTTPClient(headerFunc HeaderFunc) *http.Client { return client.NewHTTPClient(headerFunc) }

// Remote implements Cache as a client for the remote cache server, wrapping
// a *client.Client.
type Remote struct {
	c *client.Client
}

var _ Cache = (*Remote)(nil)

// NewRemote creates a new remote cache client. If headerFunc is non-nil,
// its returned headers are added to every outgoing request.
func NewRemote(baseURL string, headerFunc HeaderFunc) *Remote {
	return &Remote{c: client.New(baseURL, headerFunc)}
}

func (r *Remote) String() string { return r.c.String() }

func (r *Remote) Namespace(namespace Namespace) Cache {
	return &Remote{c: r.c.Namespace(namespace)}
}

// Remote ranged reads at least 2*remoteRangeChunkSize long fan out into
// parallel sub-range requests, because a single HTTP stream is limited to a
// fraction of the available bandwidth. Chunks stay below the remote's own S3
// large-range threshold (2*minRangePartSize) so each sub-range maps to a
// single upstream request rather than fanning out twice. Memory is bounded by
// the reader's reorder window: 2*concurrency*chunkSize per read.
const (
	remoteRangeChunkSize   = minRangePartSize
	remoteRangeConcurrency = 8
)

func (r *Remote) Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	if remoteRangeMayFanOut(NewRequestOptions(opts...)) {
		return r.parallelRangedOpen(ctx, key, opts)
	}
	rc, h, err := r.c.Open(ctx, key, opts...)
	return rc, h, errors.WithStack(err)
}

// remoteRangeMayFanOut reports whether the request's Range could span enough
// bytes to be worth a Stat plus parallel fan-out. The raw range spec is
// resolved against an unbounded object, so explicit and suffix ranges report
// their exact requested length and open-ended ranges an effectively infinite
// one; If-Range gating needs the stored ETag and is deferred to the resolve
// against the real object.
func remoteRangeMayFanOut(ro RequestOptions) bool {
	if ro.Range == "" {
		return false
	}
	ro.IfRange = ""
	_, length, outcome := ro.ResolveRange(math.MaxInt64, "")
	return outcome == RangePartial && length >= 2*remoteRangeChunkSize
}

// parallelRangedOpen serves a large ranged read with parallel sub-range
// requests pinned to the stored ETag. A preliminary Stat resolves the
// request's conditionals and range against the object's real size and
// revision; requests the policy cannot pin or split degrade to a single
// delegated stream.
func (r *Remote) parallelRangedOpen(ctx context.Context, key Key, opts []Option) (io.ReadCloser, http.Header, error) {
	headers, err := r.c.Stat(ctx, key, opts...)
	if err != nil {
		return nil, headers, errors.WithStack(err)
	}
	size, sizeErr := strconv.ParseInt(headers.Get("Content-Length"), 10, 64)
	etag := headers.Get(ETagKey)
	if sizeErr != nil || etag == "" {
		rc, h, err := r.c.Open(ctx, key, opts...)
		return rc, h, errors.WithStack(err)
	}
	start, length, partial, rangeErr := rangeShortCircuit(headers, size, opts)
	if rangeErr != nil {
		return nil, headers, errors.WithStack(rangeErr)
	}
	if !partial || length < 2*remoteRangeChunkSize {
		rc, h, err := r.c.Open(ctx, key, opts...)
		return rc, h, errors.WithStack(err)
	}
	window := newCacheObjectWindow(r.c, key, start, length, etag)
	rc, err := client.ParallelGetReader(ctx, window, Key{}, remoteRangeChunkSize, remoteRangeConcurrency)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return rc, headers, nil
}

func (r *Remote) Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error) {
	return errors.WithStack2(r.c.Stat(ctx, key, opts...))
}

func (r *Remote) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	return errors.WithStack2(r.c.Create(ctx, key, headers, ttl, opts...))
}

func (r *Remote) Delete(ctx context.Context, key Key) error {
	return errors.WithStack(r.c.Delete(ctx, key))
}

func (r *Remote) Invalidate(ctx context.Context, key Key) error {
	return errors.WithStack(r.c.Invalidate(ctx, key))
}

func (r *Remote) Stats(ctx context.Context) (Stats, error) {
	return errors.WithStack2(r.c.Stats(ctx))
}

func (r *Remote) ListNamespaces(ctx context.Context) ([]string, error) {
	return errors.WithStack2(r.c.ListNamespaces(ctx))
}

func (r *Remote) Close() error { return errors.WithStack(r.c.Close()) }
