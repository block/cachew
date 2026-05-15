package handler

import (
	"io"
	"maps"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/errors"

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
}

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

// ServeHTTP implements http.Handler.
// The handler will:
// 1. Determine the cache key using the configured function
// 2. Check if the content exists in cache
// 3. If cached, stream from cache
// 4. If not cached, transform the request and fetch from upstream
// 5. Cache the response while streaming to the client.
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
	key := parts.Key()

	logger.DebugContext(ctx, "Processing request", "cache_key", parts.Path)

	served, err := h.serveCached(w, r, key)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to serve from cache", "error", err)
	}
	if served {
		return
	}

	if err := h.fetchAndCache(w, r, key); err != nil {
		logger.ErrorContext(ctx, "Failed to fetch and cache", "error", err)
	}
}

func (h *Handler) serveCached(w http.ResponseWriter, r *http.Request, key cache.Key) (bool, error) {
	cr, headers, err := h.cache.Open(r.Context(), key)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			h.errorHandler(httputil.Errorf(http.StatusInternalServerError, "failed to open cache: %w", err), w, r)
			return true, nil
		}
		return false, nil
	}

	logging.FromContext(r.Context()).DebugContext(r.Context(), "Cache hit")
	defer cr.Close()
	maps.Copy(w.Header(), headers)
	if _, err := io.Copy(w, cr); err != nil {
		return true, errors.Wrap(err, "stream from cache")
	}
	return true, nil
}

func (h *Handler) fetchAndCache(w http.ResponseWriter, r *http.Request, key cache.Key) error {
	logging.FromContext(r.Context()).DebugContext(r.Context(), "Cache miss, fetching from upstream")

	upstreamReq, err := h.transformFunc(r)
	if err != nil {
		h.errorHandler(err, w, r)
		return nil
	}

	// Forward safe headers from the original request, without overwriting headers set by transform.
	forwardable := httputil.FilterHeaders(r.Header, httputil.HopByHopHeaders...)
	for key, values := range forwardable {
		if upstreamReq.Header.Get(key) == "" {
			upstreamReq.Header[key] = values
		}
	}

	resp, err := h.client.Do(upstreamReq)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err), w, r)
		return nil
	}
	defer resp.Body.Close()

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

func (h *Handler) streamAndCache(w http.ResponseWriter, r *http.Request, key cache.Key, resp *http.Response) error {
	ttl := h.ttlFunc(r)
	responseHeaders := maps.Clone(resp.Header)
	cw, err := h.cache.Create(r.Context(), key, responseHeaders, ttl)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusInternalServerError, "failed to create cache entry: %w", err), w, r)
		return nil
	}

	pr, pw := io.Pipe()
	go func() {
		mw := io.MultiWriter(pw, cw)
		_, copyErr := io.Copy(mw, resp.Body)
		var closeErr error
		if copyErr != nil {
			closeErr = cw.Abort(copyErr)
		} else {
			closeErr = cw.Close()
		}
		pw.CloseWithError(errors.Join(copyErr, closeErr))
	}()

	maps.Copy(w.Header(), resp.Header)
	_, copyErr := io.Copy(w, pr)
	closeErr := pr.Close()
	return errors.Wrap(errors.Join(copyErr, closeErr), "stream and cache response")
}

func defaultErrorHandler(err error, w http.ResponseWriter, r *http.Request) {
	if h, ok := errors.AsType[httputil.HTTPResponder](err); ok {
		h.WriteHTTP(w, r)
	} else {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}
