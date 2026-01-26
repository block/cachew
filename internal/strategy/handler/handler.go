package handler

import (
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

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
	cacheKeyFunc  func(*http.Request) string
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
		cacheKeyFunc: func(r *http.Request) string {
			return r.URL.String()
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
	h.cacheKeyFunc = f
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
	logger := logging.FromContext(r.Context())

	cacheKeyStr := h.cacheKeyFunc(r)
	key := cache.NewKey(cacheKeyStr)

	logger.DebugContext(r.Context(), "Processing request", slog.String("cache_key", cacheKeyStr))

	if h.serveCached(w, r, key, logger) {
		return
	}

	h.fetchAndCache(w, r, key, logger)
}

func (h *Handler) serveCached(w http.ResponseWriter, r *http.Request, key cache.Key, logger *slog.Logger) bool {
	cr, headers, err := h.cache.Open(r.Context(), key)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			h.errorHandler(httputil.Errorf(http.StatusInternalServerError, "failed to open cache: %w", err), w, r)
			return true
		}
		return false
	}

	logger.DebugContext(r.Context(), "Cache hit")
	defer cr.Close()
	maps.Copy(w.Header(), headers)
	if _, err := io.Copy(w, cr); err != nil {
		logger.ErrorContext(r.Context(), "Failed to stream from cache", slog.String("error", err.Error()))
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "Failed to stream from cache", "error", err.Error())
	}
	return true
}

func (h *Handler) fetchAndCache(w http.ResponseWriter, r *http.Request, key cache.Key, logger *slog.Logger) {
	logger.DebugContext(r.Context(), "Cache miss, fetching from upstream")

	upstreamReq, err := h.transformFunc(r)
	if err != nil {
		h.errorHandler(err, w, r)
		return
	}

	resp, err := h.client.Do(upstreamReq)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err), w, r)
		return
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.ErrorContext(r.Context(), "Failed to close response body", slog.String("error", closeErr.Error()))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		h.streamNonOKResponse(w, resp, logger)
		return
	}

	h.streamAndCache(w, r, key, resp, logger)
}

func (h *Handler) streamNonOKResponse(w http.ResponseWriter, resp *http.Response, logger *slog.Logger) {
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		logger.ErrorContext(resp.Request.Context(), "Failed to stream error response", slog.String("error", err.Error()))
	}
}

func (h *Handler) streamAndCache(w http.ResponseWriter, r *http.Request, key cache.Key, resp *http.Response, logger *slog.Logger) {
	ttl := h.ttlFunc(r)
	responseHeaders := maps.Clone(resp.Header)
	cw, err := h.cache.Create(r.Context(), key, responseHeaders, ttl)
	if err != nil {
		h.errorHandler(httputil.Errorf(http.StatusInternalServerError, "failed to create cache entry: %w", err), w, r)
		return
	}

	pr, pw := io.Pipe()
	go func() {
		mw := io.MultiWriter(pw, cw)
		_, copyErr := io.Copy(mw, resp.Body)
		closeErr := errors.Join(cw.Close(), resp.Body.Close())
		pw.CloseWithError(errors.Join(copyErr, closeErr))
	}()

	maps.Copy(w.Header(), resp.Header)
	if _, err := io.Copy(w, pr); err != nil {
		logger.ErrorContext(r.Context(), "Failed to stream response", slog.String("error", err.Error()))
	}
	if closeErr := pr.Close(); closeErr != nil {
		logger.ErrorContext(r.Context(), "Failed to close pipe", slog.String("error", closeErr.Error()))
	}
}

func defaultErrorHandler(err error, w http.ResponseWriter, r *http.Request) {
	if h, ok := errors.AsType[httputil.HTTPResponder](err); ok {
		h.WriteHTTP(w, r)
	} else {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, err.Error())
	}
}
