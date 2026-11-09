// Package spool shares one in-flight HTTP response between concurrent requests,
// which stream it while it is still being produced.
package spool

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

// fileSuffix marks spool files so New can remove only its own leftovers from a
// directory that may be shared with other data.
const fileSuffix = ".spool"

// Config configures the spool Registry.
type Config struct {
	Dir         string        `hcl:"dir,optional" help:"Directory for spool files." default:"${CACHEW_STATE}/spools"`
	FillTimeout time.Duration `hcl:"fill-timeout,optional" help:"Maximum duration of a single shared fill. 0 disables the limit." default:"30m"`
	ErrorWindow time.Duration `hcl:"error-window,optional" help:"Maximum time a reusable error response is shared with later requests. 0 shares it only with requests already waiting." default:"5s"`
}

// Provider lazily constructs the process-wide Registry.
type Provider func() (*Registry, error)

// NewProvider returns a Registry singleton provider function.
func NewProvider(ctx context.Context, config Config) Provider {
	return sync.OnceValues(func() (*Registry, error) {
		return New(ctx, config)
	})
}

// Registry owns every in-flight fill in the process. Fills run detached from
// the requests that started them, so Close is the only way to cancel them.
type Registry struct {
	config Config
	ctx    context.Context
	cancel context.CancelFunc
	fills  sync.WaitGroup
}

// New creates a Registry, removing spool files left behind by a previous process.
// ctx's cancellation is ignored so shutdown does not truncate responses; use Close.
func New(ctx context.Context, config Config) (*Registry, error) {
	if err := os.MkdirAll(config.Dir, 0o750); err != nil {
		return nil, errors.Wrap(err, "create spool directory")
	}
	stale, err := filepath.Glob(filepath.Join(config.Dir, "*"+fileSuffix))
	if err != nil {
		return nil, errors.Wrap(err, "find stale spool files")
	}
	for _, path := range stale {
		if err := os.Remove(path); err != nil {
			return nil, errors.Wrap(err, "remove stale spool file")
		}
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return &Registry{config: config, ctx: ctx, cancel: cancel}, nil
}

// NewGroup returns a key space whose keys never collide with another group's.
func (r *Registry) NewGroup(retention Retention) *Group {
	return &Group{registry: r, retention: retention, entries: map[string]*entry{}}
}

// Close cancels in-flight fills and waits for them to return. Call it after the
// HTTP server has drained, because a cancelled fill truncates its response.
func (r *Registry) Close() {
	r.cancel()
	r.fills.Wait()
}

// start runs fill for e. The fill keeps req's values, such as the logger and
// trace span, but not its cancellation.
func (r *Registry) start(e *entry, req *http.Request, fill FillFunc) {
	ctx := context.WithoutCancel(req.Context())
	var cancel context.CancelFunc
	if r.config.FillTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.config.FillTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	stop := context.AfterFunc(r.ctx, cancel)
	r.fills.Go(func() {
		defer cancel()
		defer stop()
		w := &Writer{entry: e, dir: r.config.Dir, header: http.Header{}}
		e.finish(w, runFill(ctx, w, fill))
	})
}

// runFill converts http.ErrAbortHandler panics into errors. Fills reuse handler
// code such as httputil.ReverseProxy, but run outside the server's recovery.
func runFill(ctx context.Context, w *Writer, fill FillFunc) (err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		abort, ok := recovered.(error)
		if !ok || !errors.Is(abort, http.ErrAbortHandler) {
			panic(recovered)
		}
		err = errors.Errorf("spool fill aborted: %w", abort)
	}()
	return fill(ctx, w)
}
