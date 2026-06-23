// Package client provides a standalone HTTP client for the Cachew cache server.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPStatusError is returned when the server responds with an unexpected
// HTTP status code. Callers can use errors.As to inspect the status code.
type HTTPStatusError struct {
	StatusCode int
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("unexpected status code: %d", e.StatusCode)
}

// Is allows errors.Is to match HTTPStatusError against sentinel errors.
func (e *HTTPStatusError) Is(target error) bool {
	switch target { //nolint:errorlint // comparing sentinel values, not wrapped errors
	case ErrNotModified:
		return e.StatusCode == http.StatusNotModified
	case ErrPreconditionFailed:
		return e.StatusCode == http.StatusPreconditionFailed
	case ErrRangeNotSatisfiable:
		return e.StatusCode == http.StatusRequestedRangeNotSatisfiable
	default:
		return false
	}
}

// transportHeaders are headers added by the HTTP transport layer that should
// not be surfaced as cached-object metadata on responses.
var transportHeaders = []string{ //nolint:gochecknoglobals
	"Date",
	"Accept-Encoding",
	"User-Agent",
	"Transfer-Encoding",
	"Time-To-Live",
}

// CacheWriter extends io.WriteCloser with the ability to abort an in-progress
// cache write. Exactly one of Close or Abort must be called.
//
// Close commits the data to the cache. Abort discards the in-progress write,
// ensuring the object is never made visible in the cache. Both methods are
// idempotent after the first call.
type CacheWriter interface {
	io.WriteCloser
	// Abort discards the in-progress write and releases resources.
	// The provided error is recorded as the cause of cancellation.
	// The object MUST NOT be made available in the cache after Abort.
	Abort(err error) error
}

// HeaderFunc returns headers to attach to each outgoing request.
type HeaderFunc func() http.Header

// NewHTTPClient creates an *http.Client that attaches headerFunc headers
// to every outgoing request. Useful for callers that need to talk to
// non-API endpoints (e.g. /git/) with the same auth as the cache client.
//
// The returned client is instrumented with otelhttp so each outgoing request
// produces a child span and propagates the W3C traceparent header. When no
// tracer provider is configured, otelhttp falls back to a no-op tracer, so
// this is cost-free for callers that have not opted in to tracing.
func NewHTTPClient(headerFunc HeaderFunc) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:errcheck
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100

	var rt http.RoundTripper = otelhttp.NewTransport(transport)
	if headerFunc != nil {
		rt = &headerTransport{base: rt, headerFunc: headerFunc}
	}
	return &http.Client{Transport: rt}
}

type headerTransport struct {
	base       http.RoundTripper
	headerFunc HeaderFunc
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for key, values := range t.headerFunc() {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return t.base.RoundTrip(req) //nolint:wrapcheck
}

// Client is an HTTP client for a Cachew cache server. Its method set mirrors
// the cache.Cache interface, so it can be used as the transport for a remote
// cache backend.
type Client struct {
	baseURL   string
	http      *http.Client
	namespace Namespace
}

// New creates a Client against the given base URL (e.g. "http://localhost:8080").
// If headerFunc is non-nil, its returned headers are added to every outgoing
// request.
func New(baseURL string, headerFunc HeaderFunc) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    NewHTTPClient(headerFunc),
	}
}

// NewWithHTTPClient creates a Client against baseURL using the supplied
// *http.Client. Callers are responsible for configuring authentication on
// the supplied client (e.g. via a custom RoundTripper).
func NewWithHTTPClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpClient,
	}
}

// HTTP returns the underlying HTTP client, for callers needing to talk to
// non-API endpoints with the same auth configuration.
func (c *Client) HTTP() *http.Client { return c.http }

// BaseURL returns the cachew server root URL this client targets.
func (c *Client) BaseURL() string { return c.baseURL }

// String describes the client.
func (c *Client) String() string { return "remote:" + c.baseURL }

// Namespace returns a derived client that targets the given namespace.
func (c *Client) Namespace(namespace Namespace) *Client {
	return &Client{
		baseURL:   c.baseURL,
		http:      c.http,
		namespace: namespace,
	}
}

func (c *Client) resolvedNamespace() Namespace {
	if c.namespace == "" {
		return DefaultNamespace
	}
	return c.namespace
}

func (c *Client) objectURL(key Key) string {
	return fmt.Sprintf("%s/api/v1/object/%s/%s", c.baseURL, c.resolvedNamespace(), key.String())
}

// Open retrieves an object from the cache server. Accepts optional
// [RequestOption]s such as [IfNoneMatch] for conditional requests.
func (c *Client) Open(ctx context.Context, key Key, opts ...RequestOption) (io.ReadCloser, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create request")
	}
	NewRequestOptions(opts...).applyToRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to execute request")
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, filterHeaders(resp.Header, transportHeaders...), nil

	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(os.ErrNotExist, resp.Body.Close())

	case http.StatusNotModified:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, filterHeaders(resp.Header, transportHeaders...), errors.Join(ErrNotModified, resp.Body.Close())

	case http.StatusRequestedRangeNotSatisfiable:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, filterHeaders(resp.Header, transportHeaders...), errors.Join(ErrRangeNotSatisfiable, resp.Body.Close())

	case http.StatusPreconditionFailed:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(ErrPreconditionFailed, resp.Body.Close())

	default:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode}), resp.Body.Close())
	}
}

// Stat retrieves headers for an object from the cache server. Accepts optional
// [RequestOption]s such as [IfNoneMatch] for conditional requests.
func (c *Client) Stat(ctx context.Context, key Key, opts ...RequestOption) (http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(key), nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}
	NewRequestOptions(opts...).applyToRequest(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return filterHeaders(resp.Header, transportHeaders...), nil
	case http.StatusNotFound:
		return nil, os.ErrNotExist
	case http.StatusNotModified:
		return filterHeaders(resp.Header, transportHeaders...), ErrNotModified
	case http.StatusPreconditionFailed:
		return nil, ErrPreconditionFailed
	default:
		return nil, errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode})
	}
}

// Create stores a new object in the cache server. The returned CacheWriter
// must be closed to commit the upload. Call Abort instead of Close to discard
// the in-progress write and ensure the object is never made visible.
func (c *Client) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (CacheWriter, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	pr, pw := io.Pipe()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.objectURL(key), pr)
	if err != nil {
		cancel(err)
		return nil, errors.Join(errors.Wrap(err, "failed to create request"), pr.Close(), pw.Close())
	}

	maps.Copy(req.Header, headers)

	if ttl > 0 {
		req.Header.Set("Time-To-Live", ttl.String())
	}

	wc := &writeCloser{
		pw:     pw,
		done:   make(chan error, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	go func() {
		resp, err := c.http.Do(req)
		if err != nil {
			wc.done <- errors.Wrap(err, "failed to execute request")
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		_ = resp.Body.Close()                 //nolint:gosec

		if resp.StatusCode != http.StatusOK {
			wc.done <- errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode})
			return
		}

		wc.done <- nil
	}()

	return wc, nil
}

// Delete removes an object from the cache server.
func (c *Client) Delete(ctx context.Context, key Key) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(key), nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return os.ErrNotExist
	}

	if resp.StatusCode != http.StatusOK {
		return errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode})
	}

	return nil
}

// Close releases resources held by the client.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// Stats retrieves cache statistics from the server.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/stats", nil)
	if err != nil {
		return Stats{}, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Stats{}, errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotImplemented {
		return Stats{}, ErrStatsUnavailable
	}

	if resp.StatusCode != http.StatusOK {
		return Stats{}, errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode})
	}

	var stats Stats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return Stats{}, errors.Wrap(err, "failed to decode stats response")
	}

	return stats, nil
}

// ListNamespaces requests the namespace list from the server.
func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/namespaces", nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) //nolint:errcheck
		return nil, errors.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	var namespaces []string
	if err := json.NewDecoder(resp.Body).Decode(&namespaces); err != nil {
		return nil, errors.WithStack(err)
	}

	return namespaces, nil
}

// filterHeaders returns a copy of headers with the specified keys removed.
func filterHeaders(headers http.Header, skip ...string) http.Header {
	skipSet := make(map[string]bool, len(skip))
	for _, s := range skip {
		skipSet[http.CanonicalHeaderKey(s)] = true
	}
	filtered := make(http.Header, len(headers))
	for key, values := range headers {
		if skipSet[http.CanonicalHeaderKey(key)] {
			continue
		}
		filtered[key] = values
	}
	return filtered
}

// writeCloser wraps a pipe writer and waits for the HTTP request to complete.
type writeCloser struct {
	pw       *io.PipeWriter
	done     chan error
	ctx      context.Context
	cancel   context.CancelCauseFunc
	once     sync.Once
	closeErr error
}

func (wc *writeCloser) Write(p []byte) (int, error) {
	n, err := wc.pw.Write(p)
	return n, errors.WithStack(err)
}

func (wc *writeCloser) Abort(err error) error {
	wc.cancel(err)
	return wc.Close()
}

// Close is safe to call multiple times and from multiple goroutines (e.g. a
// deferred Close racing an explicit Abort): the underlying close-and-wait
// runs at most once, so the second caller will not deadlock waiting on a
// drained <-wc.done channel.
func (wc *writeCloser) Close() error {
	wc.once.Do(func() {
		wc.closeErr = wc.doClose()
	})
	return wc.closeErr
}

func (wc *writeCloser) doClose() error {
	// Close the upload pipe so the goroutine driving c.http.Do can finish.
	// If the caller's ctx is cancelled or the upload was Abort'd, propagate
	// the cause via CloseWithError so any in-flight Write returns the cause
	// rather than EOF; otherwise do a clean Close. context.Cause is
	// preferred over ctx.Err so an Abort(cause) propagates the caller's
	// reason rather than a generic context.Canceled — when the parent
	// context cancels independently, Cause falls back to ctx.Err so
	// behaviour matches the previous code in that path.
	ctxCause := context.Cause(wc.ctx)
	var pipeCloseErr error
	if ctxCause != nil {
		_ = wc.pw.CloseWithError(ctxCause)
	} else {
		pipeCloseErr = wc.pw.Close()
	}

	// Always wait for the request goroutine to finish so connection
	// resources are released and we can inspect the server's response.
	serverErr := <-wc.done

	// Prefer the server's typed *HTTPStatusError when present. The server
	// can respond with an authoritative status (e.g. 403) before the local
	// ctx is cancelled or the pipe is torn down — that response is more
	// meaningful to the caller than the local symptom (a downstream
	// "broken pipe" or context cancellation can fire as a consequence of
	// the server closing the connection after writing its response).
	// Without this preference, callers that match on *HTTPStatusError to
	// classify e.g. "save not authorized" lose the signal entirely.
	var statusErr *HTTPStatusError
	if errors.As(serverErr, &statusErr) {
		return errors.Wrap(serverErr, "request failed")
	}
	if ctxCause != nil {
		return errors.Wrap(ctxCause, "create operation cancelled")
	}
	if pipeCloseErr != nil {
		return errors.Wrap(pipeCloseErr, "failed to close pipe writer")
	}
	if serverErr != nil {
		return errors.Wrap(serverErr, "request failed")
	}
	return nil
}
