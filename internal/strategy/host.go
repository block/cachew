package strategy

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/spool"
	"github.com/block/cachew/internal/strategy/handler"
)

func RegisterHost(r *Registry, spools spool.Provider) {
	Register(r, "host", "A generic host-based proxying strategy.", func(ctx context.Context, config HostConfig, c cache.Cache, mux Mux) (*Host, error) {
		registry, err := spools()
		if err != nil {
			return nil, errors.Wrap(err, "create spool registry")
		}
		return NewHost(ctx, config, c, mux, registry)
	})
}

// HostConfig represents the configuration for the Host strategy.
//
// In HCL it looks something like this:
//
//	host {
//		target = "https://github.com/"
//	}
//
// In this example, the strategy will be mounted under "/github.com".
type HostConfig struct {
	Target    string               `hcl:"target,label" help:"The target URL to proxy requests to."`
	Headers   map[string]string    `hcl:"headers,optional" help:"Headers to add to upstream requests."`
	Fallbacks []HostFallbackConfig `hcl:"fallback,block" help:"Ordered, trusted fallback upstreams."`
}

// HostFallbackConfig maps a primary path prefix to a trusted mirror URL.
type HostFallbackConfig struct {
	Target     string `hcl:"target,label" help:"Fallback base URL."`
	PathPrefix string `hcl:"path-prefix,optional" help:"Primary path prefix to replace with the fallback URL path (defaults to /)."`
}

// The Host [Strategy] forwards all GET requests to the specified host, caching the response payloads.
type Host struct {
	target  *url.URL
	cache   cache.Cache
	client  *http.Client
	prefix  string
	headers map[string]string
}

var _ Strategy = (*Host)(nil)

func NewHost(_ context.Context, config HostConfig, cache cache.Cache, mux Mux, spools *spool.Registry) (*Host, error) {
	u, err := url.Parse(config.Target)
	if err != nil {
		return nil, errors.Errorf("invalid target URL: %w", err)
	}
	prefix := "/" + u.Host + u.EscapedPath()
	h := &Host{
		target:  u,
		cache:   cache,
		client:  httputil.UpstreamClient(),
		prefix:  prefix,
		headers: config.Headers,
	}

	if len(config.Fallbacks) > 0 {
		transport := &hostFallbackTransport{client: httputil.UpstreamClient()}
		for _, config := range config.Fallbacks {
			target, err := url.Parse(config.Target)
			if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") ||
				target.User != nil || target.RawQuery != "" || target.Fragment != "" {
				return nil, errors.Errorf("invalid fallback URL %q: expected an HTTP(S) base URL without credentials, query or fragment", config.Target)
			}
			pathPrefix := strings.TrimSuffix(config.PathPrefix, "/")
			if pathPrefix != "" && !strings.HasPrefix(pathPrefix, "/") {
				return nil, errors.Errorf("invalid fallback path-prefix %q: must start with /", config.PathPrefix)
			}
			transport.fallbacks = append(transport.fallbacks, hostFallback{target: target, prefix: pathPrefix})
		}
		h.client.Transport = transport
	}

	hdlr := handler.New(h.client, cache, spools).Metrics("host", u.Hostname()).
		CacheKey(func(r *http.Request) string {
			return h.buildTargetURL(r).String()
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			if err := httputil.ValidatePath(r.URL.Path); err != nil {
				return nil, httputil.Errorf(http.StatusBadRequest, "%w", err)
			}
			targetURL := h.buildTargetURL(r)
			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, targetURL.String(), nil)
			if err != nil {
				return nil, errors.Wrap(err, "creating upstream request")
			}
			for k, v := range h.headers {
				req.Header.Set(k, v)
			}
			return req, nil
		})

	mux.Handle("GET "+prefix+"/", hdlr)
	return h, nil
}

func (d *Host) String() string { return "host:" + d.target.Host + d.target.Path }

// buildTargetURL constructs the target URL from the incoming request.
func (d *Host) buildTargetURL(r *http.Request) *url.URL {
	// Strip the prefix from the request path
	path := r.URL.Path
	if len(path) >= len(d.prefix) {
		path = path[len(d.prefix):]
	}
	if path == "" {
		path = "/"
	}

	targetURL := *d.target
	targetURL.Path = path
	targetURL.RawQuery = r.URL.RawQuery
	return &targetURL
}

type hostFallback struct {
	target *url.URL
	prefix string
}

type hostFallbackTransport struct {
	client    *http.Client
	fallbacks []hostFallback
}

func (t *hostFallbackTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var retryAt time.Time
	do := func(req *http.Request) (*http.Response, error) {
		resp, err := t.client.Do(req)
		if err == nil && resp.StatusCode == http.StatusTooManyRequests {
			until, valid := httputil.RetryAfterDeadline(resp.Header.Get("Retry-After"), time.Now(), 0)
			if valid && (retryAt.IsZero() || until.Before(retryAt)) {
				retryAt = until
			}
		}
		return resp, errors.WithStack(err)
	}
	resp, err := do(r)
	for _, fallback := range t.fallbacks {
		if r.Context().Err() != nil || (err == nil && resp.StatusCode != http.StatusTooManyRequests &&
			(resp.StatusCode < 500 || resp.StatusCode > 599)) {
			break
		}
		path := r.URL.Path
		if fallback.prefix != "" && path != fallback.prefix && !strings.HasPrefix(path, fallback.prefix+"/") {
			continue
		}
		if resp != nil {
			const maxDrain = 2 << 10
			if resp.ProtoMajor == 1 && resp.ContentLength <= maxDrain {
				body := resp.Body
				closed := make(chan struct{})
				timer := time.AfterFunc(100*time.Millisecond, func() {
					_ = body.Close()
					close(closed)
				})
				_, _ = io.CopyN(io.Discard, body, maxDrain) //nolint:errcheck // A failed drain must not prevent fallback.
				if !timer.Stop() {
					<-closed
				}
			}
			_ = resp.Body.Close()
		}
		req := r.Clone(r.Context())
		target := *fallback.target
		target.Path = strings.TrimSuffix(target.Path, "/") + "/" + strings.TrimPrefix(strings.TrimPrefix(path, fallback.prefix), "/")
		target.RawPath = ""
		target.RawQuery = r.URL.RawQuery
		req.URL = &target
		req.Host = ""
		resp, err = do(req)
	}
	if err == nil && resp.StatusCode == http.StatusTooManyRequests && !retryAt.IsZero() {
		resp.Header.Set("Retry-After", strconv.FormatInt(int64(math.Ceil(max(0, time.Until(retryAt).Seconds()))), 10))
	}
	return resp, errors.WithStack(err)
}
