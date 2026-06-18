package cache

import (
	"context"
	"io"
	"net/http"
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

func (r *Remote) Open(ctx context.Context, key Key, conds ...OpenOption) (io.ReadCloser, http.Header, error) {
	rc, h, err := r.c.Open(ctx, key, conds...)
	return rc, h, errors.WithStack(err)
}

func (r *Remote) Stat(ctx context.Context, key Key) (http.Header, error) {
	return errors.WithStack2(r.c.Stat(ctx, key))
}

func (r *Remote) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (Writer, error) {
	return errors.WithStack2(r.c.Create(ctx, key, headers, ttl))
}

func (r *Remote) Delete(ctx context.Context, key Key) error {
	return errors.WithStack(r.c.Delete(ctx, key))
}

func (r *Remote) Stats(ctx context.Context) (Stats, error) {
	return errors.WithStack2(r.c.Stats(ctx))
}

func (r *Remote) ListNamespaces(ctx context.Context) ([]string, error) {
	return errors.WithStack2(r.c.ListNamespaces(ctx))
}

func (r *Remote) Close() error { return errors.WithStack(r.c.Close()) }
