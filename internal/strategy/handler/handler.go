package handler

import (
	"cmp"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// CacheKeyParts holds the components used to build a cache key. Path is the
// primary identifier (typically the upstream URL) and Vary captures
// request-derived dimensions like Accept-Encoding.
type CacheKeyParts struct {
	Path string
	Vary map[string]string
}

func NewCacheKeyParts(path string) CacheKeyParts {
	return CacheKeyParts{Path: path, Vary: make(map[string]string)}
}

func (p CacheKeyParts) Key() cache.Key {
	if len(p.Vary) == 0 {
		return cache.NewKey(p.Path)
	}
	keys := make([]string, 0, len(p.Vary))
	for k := range p.Vary {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(p.Path)
	for _, k := range keys {
		b.WriteByte('\n')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(p.Vary[k])
	}
	return cache.NewKey(b.String())
}

// Handler provides a fluent API for creating cache-backed HTTP handlers.
//
// Example usage:
//
//	h := handler.New(client, cache).
//		CacheKey(func(r *http.Request) string {
//			return "custom-key"
//		}).
//		Transform(func(r *http.Request) (*http.Request, error) {
//			// Modify request before fetching
//			return modifiedRequest, nil
//		})
type Handler struct {
	client        *http.Client
	cache         cache.Cache
	cacheKeyFunc  func(*http.Request) CacheKeyParts
	transformFunc func(*http.Request) (*http.Request, error)
	errorHandler  func(error, http.ResponseWriter, *http.Request)
	ttlFunc       func(*http.Request) time.Duration
	varyFunc      func(*http.Request) map[string]string
	verifyFunc    func(*http.Request) Verifier
	revalidation  *revalidation
	parallel      *parallelFetch
}

type revalidation struct {
	header   string
	freshFor func(*http.Request) time.Duration
}

// checkedAtHeader records when a revalidated entry was last confirmed against
// the upstream. It is internal bookkeeping and never sent to clients.
const checkedAtHeader = "X-Cachew-Checked-At"

// New creates a new Handler with the given HTTP client and cache.
// By default:
// - Cache key is derived from the request URL
// - No request transformation is performed
// - Standard error handling is used.
func New(client *http.Client, c cache.Cache) *Handler {
	return &Handler{
		client: client,
		cache:  c,
		cacheKeyFunc: func(r *http.Request) CacheKeyParts {
			return NewCacheKeyParts(r.URL.String())
		},
		transformFunc: func(r *http.Request) (*http.Request, error) {
			return r, nil
		},
		errorHandler: defaultErrorHandler,
		ttlFunc: func(_ *http.Request) time.Duration {
			return 0
		},
	}
}

// CacheKey sets the function used to determine the cache key for a request.
// The function receives the original incoming request.
func (h *Handler) CacheKey(f func(*http.Request) string) *Handler {
	h.cacheKeyFunc = func(r *http.Request) CacheKeyParts {
		return NewCacheKeyParts(f(r))
	}
	return h
}

// Transform sets the function used to transform the incoming request before fetching.
// This is where you can modify the request URL, headers, etc.
// The function receives the original incoming request and should return the request
// that will be sent to the upstream server.
func (h *Handler) Transform(f func(*http.Request) (*http.Request, error)) *Handler {
	h.transformFunc = f
	return h
}

// OnError sets a custom error handler for the built handler.
// If not set, a default error handler is used.
func (h *Handler) OnError(f func(error, http.ResponseWriter, *http.Request)) *Handler {
	h.errorHandler = f
	return h
}

// TTL sets the function used to determine the cache TTL for a request.
// The function receives the original incoming request.
// If not set or returns 0, the cache's default/maximum TTL is used.
func (h *Handler) TTL(f func(*http.Request) time.Duration) *Handler {
	h.ttlFunc = f
	return h
}

// Vary keys the cache on request headers that change the upstream response,
// so a variant fetched for one client is never served to a client that asked
// for another. Accept-Encoding is always included.
func (h *Handler) Vary(f func(*http.Request) map[string]string) *Handler {
	h.varyFunc = f
	return h
}

// Verifier checks a response body before it is committed to the cache.
type Verifier interface {
	io.Writer
	Verify() error
}

// Verify keeps corrupt or truncated upstream responses out of the shared
// cache, where they would otherwise be served to every later client. The
// requesting client still receives the bytes. Return nil for requests whose
// expected content is unknown.
func (h *Handler) Verify(f func(*http.Request) Verifier) *Handler {
	h.verifyFunc = f
	return h
}

// Revalidate caches mutable upstream resources without serving them stale
// indefinitely or refetching unchanged content. Unlike TTL expiry, a stale
// entry is kept: it is revalidated with a cheap HEAD and served as-is when
// the upstream is unchanged or unavailable.
//
// header names the response header that identifies the content (e.g.
// Docker-Content-Digest). It cannot be ETag, which the cache replaces with
// its own. freshFor returns how long an entry may be served without
// revalidation; zero marks it as immutable.
func (h *Handler) Revalidate(header string, freshFor func(*http.Request) time.Duration) *Handler {
	h.revalidation = &revalidation{header: header, freshFor: freshFor}
	return h
}

// ParallelFetch speeds up cold fills of large objects from upstreams that
// limit throughput per connection. It is opt-in because it multiplies
// upstream requests and only helps some upstreams. Objects that fit in one
// chunk cost a single request. Ranges are reassembled in order, so caching,
// streaming and verification are unaffected.
func (h *Handler) ParallelFetch(chunkSize int64, concurrency int) *Handler {
	h.parallel = &parallelFetch{chunkSize: chunkSize, concurrency: concurrency}
	return h
}

// ServeHTTP implements http.Handler.
// The handler will:
// 1. Determine the cache key using the configured function
// 2. Check if the content exists in cache
// 3. If cached, stream from cache
// 4. If not cached, transform the request and fetch from upstream
// 5. Cache the response while streaming to the client.
//
// HEAD misses are forwarded upstream without caching, so metadata probes
// never trigger full downloads.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	logger := logging.FromContext(ctx)

	parts := h.cacheKeyFunc(r)
	// Upstreams may return different content based on Accept-Encoding (e.g.
	// gzip-compressed vs uncompressed). Without this, the first variant cached
	// is served to all clients, breaking those that expect the other encoding.
	if ae := r.Header.Get("Accept-Encoding"); ae != "" {
		parts.Vary["Accept-Encoding"] = ae
	}
	if h.varyFunc != nil {
		maps.Copy(parts.Vary, h.varyFunc(r))
	}
	key := parts.Key()

	logger.DebugContext(ctx, "Processing request", "cache_key", parts.Path)

	served, err := h.serveCached(w, r, key)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to serve from cache", "cache_key", parts.Path, "error", err)
	}
	if served {
		return
	}

	if r.Method == http.MethodHead {
		h.forwardHead(w, r)
		return
	}
	if err := h.fetchAndCache(w, r, key); err != nil {
		logger.ErrorContext(ctx, "Failed to fetch and cache", "cache_key", parts.Path, "error", err)
	}
}

func (h *Handler) serveCached(w http.ResponseWriter, r *http.Request, key cache.Key) (bool, error) {
	ctx := r.Context()
	opts := httputil.ConditionalOptions(r)
	if r.Method == http.MethodHead {
		headers, err := h.cache.Stat(ctx, key, opts...)
		if cacheEntryExists(err) && !h.fresh(r, key, headers) {
			return false, nil
		}
		if httputil.ServeCacheStat(w, clientHeaders(headers), err) {
			return true, nil
		}
		return h.cacheMiss(w, r, err), nil
	}
	cr, headers, err := h.cache.Open(ctx, key, opts...)
	if cacheEntryExists(err) && !h.fresh(r, key, headers) {
		if cr != nil {
			return false, errors.Wrap(cr.Close(), "close stale entry")
		}
		return false, nil
	}
	if handled, _, serveErr := httputil.ServeCacheHit(w, clientHeaders(headers), cr, err); handled {
		logging.FromContext(ctx).DebugContext(ctx, "Cache hit")
		return true, errors.WithStack(serveErr)
	}
	return h.cacheMiss(w, r, err), nil
}

// cacheMiss reports whether a failed cache lookup was answered with an error,
// rather than being a miss to fill from upstream.
func (h *Handler) cacheMiss(w http.ResponseWriter, r *http.Request, err error) bool {
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	h.errorHandler(httputil.Errorf(http.StatusInternalServerError, "failed to open cache: %w", err), w, r)
	return true
}

func cacheEntryExists(err error) bool {
	return err == nil || errors.Is(err, cache.ErrNotModified) || errors.Is(err, cache.ErrPreconditionFailed) ||
		errors.Is(err, cache.ErrRangeNotSatisfiable)
}

func clientHeaders(headers http.Header) http.Header {
	if headers.Get(checkedAtHeader) == "" {
		return headers
	}
	headers = headers.Clone()
	headers.Del(checkedAtHeader)
	return headers
}

// fresh reports whether a cached entry may be served, revalidating it against
// the upstream once its freshness lapses.
func (h *Handler) fresh(r *http.Request, key cache.Key, headers http.Header) bool {
	if h.revalidation == nil || headers == nil {
		return true
	}
	freshFor := h.revalidation.freshFor(r)
	if freshFor == 0 {
		return true
	}
	checkedAt, err := strconv.ParseInt(headers.Get(checkedAtHeader), 10, 64)
	if err == nil && time.Since(time.Unix(checkedAt, 0)) < freshFor {
		return true
	}
	return h.revalidate(r, key, headers)
}

// revalidate reports whether the upstream still serves the cached content.
// Upstream failures count as unchanged: a stale copy beats an error.
func (h *Handler) revalidate(r *http.Request, key cache.Key, headers http.Header) bool {
	ctx := r.Context()
	logger := logging.FromContext(ctx)
	req, err := h.upstreamRequest(r)
	if err != nil {
		logger.WarnContext(ctx, "Serving stale entry, revalidation request failed", "error", err)
		return true
	}
	req.Method = http.MethodHead
	// The client's own validators describe its copy, not the cached one.
	req.Header.Del("If-None-Match")
	req.Header.Del("If-Match")
	resp, err := h.client.Do(req)
	if err != nil {
		logger.WarnContext(ctx, "Serving stale entry, revalidation failed", "error", err)
		return true
	}
	_ = resp.Body.Close() //nolint:errcheck // HEAD has no body to lose
	switch {
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError:
		logger.WarnContext(ctx, "Serving stale entry, upstream unavailable", "status", resp.StatusCode)
		return true
	case resp.StatusCode != http.StatusOK:
		return false
	}
	stored := headers.Get(h.revalidation.header)
	if stored == "" || resp.Header.Get(h.revalidation.header) != stored {
		return false
	}
	update := http.Header{checkedAtHeader: {strconv.FormatInt(time.Now().Unix(), 10)}}
	// IfMatch stops a concurrent refetch of changed content from being marked
	// as checked on the strength of this comparison.
	err = h.cache.UpdateMetadata(ctx, key, update, h.ttlFunc(r), cache.IfMatch(headers.Get(cache.ETagKey)))
	if err != nil && !errors.Is(err, cache.ErrPreconditionFailed) && !errors.Is(err, os.ErrNotExist) {
		logger.WarnContext(ctx, "Failed to record revalidation", "error", err)
	}
	return true
}

// upstreamRequest transforms r and forwards its safe headers. Range and
// If-Range are never forwarded: whole objects are cached and ranges are cut
// locally.
func (h *Handler) upstreamRequest(r *http.Request) (*http.Request, error) {
	req, err := h.transformFunc(r)
	if err != nil {
		return nil, err
	}
	req = req.Clone(r.Context())
	// Forward safe headers from the original request, without overwriting headers set by transform.
	forwardable := httputil.FilterHeaders(r.Header, slices.Concat(httputil.HopByHopHeaders, []string{"Range", "If-Range"})...)
	for key, values := range forwardable {
		if req.Header.Get(key) == "" {
			req.Header[key] = values
		}
	}
	req.Header.Del("Range")
	req.Header.Del("If-Range")
	return req, nil
}

func (h *Handler) forwardHead(w http.ResponseWriter, r *http.Request) {
	req, err := h.upstreamRequest(r)
	if err != nil {
		h.errorHandler(err, w, r)
		return
	}
	req.Method = http.MethodHead
	resp, err := h.client.Do(req)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err), w, r)
		return
	}
	_ = resp.Body.Close() //nolint:errcheck // HEAD has no body to lose
	maps.Copy(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
}

func (h *Handler) fetchAndCache(w http.ResponseWriter, r *http.Request, key cache.Key) error {
	logging.FromContext(r.Context()).DebugContext(r.Context(), "Cache miss, fetching from upstream")

	upstreamReq, err := h.upstreamRequest(r)
	if err != nil {
		h.errorHandler(err, w, r)
		return nil
	}
	if h.parallel != nil {
		upstreamReq.Header.Set("Range", h.parallel.firstChunkRange())
	}

	resp, err := h.client.Do(upstreamReq)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err), w, r)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusPartialContent && h.parallel != nil {
		resp, err = h.parallel.fetch(r.Context(), h.client, resp)
		if err != nil {
			h.errorHandler(httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err), w, r)
			return nil
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		return h.streamNonOKResponse(w, resp)
	}

	return h.streamAndCache(w, r, key, resp)
}

func (h *Handler) streamNonOKResponse(w http.ResponseWriter, resp *http.Response) error {
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		return errors.Wrap(err, "stream non-OK response")
	}
	return nil
}

// streamAndCache streams resp to the client while writing it to the cache.
// Cache failures never interrupt the client stream: the cache entry is
// discarded and the response is streamed in full.
func (h *Handler) streamAndCache(w http.ResponseWriter, r *http.Request, key cache.Key, resp *http.Response) error {
	maps.Copy(w.Header(), resp.Header)

	headers := maps.Clone(resp.Header)
	if h.revalidation != nil {
		headers.Set(checkedAtHeader, strconv.FormatInt(time.Now().Unix(), 10))
	}
	cw, err := h.cache.Create(r.Context(), key, headers, h.ttlFunc(r))
	if err != nil {
		_, copyErr := io.Copy(w, resp.Body)
		return errors.Join(errors.Wrap(err, "create cache entry"), errors.Wrap(copyErr, "stream response"))
	}
	out := writeRangeHeader(w, r, resp)

	tee := &cacheTee{w: cw}
	pr, pw := io.Pipe()
	sinks := []io.Writer{pw, tee}
	var verifier Verifier
	if h.verifyFunc != nil {
		verifier = h.verifyFunc(r)
	}
	if verifier != nil {
		sinks = append(sinks, verifier)
	}
	cacheErrCh := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(io.MultiWriter(sinks...), resp.Body)
		abortErr := cmp.Or(copyErr, tee.err)
		if abortErr == nil && verifier != nil {
			abortErr = errors.Wrap(verifier.Verify(), "verify response")
		}
		if abortErr != nil {
			cacheErrCh <- errors.Join(tee.err, abortErr, cw.Abort(abortErr))
		} else {
			cacheErrCh <- cw.Close()
		}
		pw.CloseWithError(copyErr)
	}()

	_, copyErr := io.Copy(out, pr)
	closeErr := pr.Close()
	return errors.Join(
		errors.Wrap(errors.Join(copyErr, closeErr), "stream response"),
		errors.Wrap(<-cacheErrCh, "cache response"),
	)
}

// writeRangeHeader answers a ranged request from the full upstream body, which
// is still cached in full. It writes the status and returns the writer that
// receives the body.
func writeRangeHeader(w http.ResponseWriter, r *http.Request, resp *http.Response) io.Writer {
	opts := client.NewRequestOptions(httputil.ConditionalOptions(r)...)
	size := resp.ContentLength
	if opts.Range == "" || size < 0 {
		return w
	}
	start, length, outcome := opts.ResolveRange(size, resp.Header.Get(cache.ETagKey))
	switch outcome {
	case client.RangeFull:
		return w
	case client.RangeNotSatisfiable:
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return io.Discard
	case client.RangePartial:
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)
	return &windowWriter{w: w, start: start, end: start + length}
}

// windowWriter passes through only the bytes in [start, end) of the stream
// written to it, while reporting every write as complete.
type windowWriter struct {
	w          io.Writer
	start, end int64
	offset     int64
}

func (ww *windowWriter) Write(p []byte) (int, error) {
	n := int64(len(p))
	lo := min(max(ww.start-ww.offset, 0), n)
	hi := min(max(ww.end-ww.offset, 0), n)
	ww.offset += n
	if lo < hi {
		if _, err := ww.w.Write(p[lo:hi]); err != nil {
			return 0, errors.WithStack(err)
		}
	}
	return len(p), nil
}

type cacheTee struct {
	w   io.Writer
	err error
}

func (t *cacheTee) Write(p []byte) (int, error) {
	if t.err == nil {
		_, t.err = t.w.Write(p)
	}
	return len(p), nil
}

func defaultErrorHandler(err error, w http.ResponseWriter, r *http.Request) {
	if h, ok := errors.AsType[httputil.HTTPResponder](err); ok {
		h.WriteHTTP(w, r)
	} else {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}
