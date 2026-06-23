package strategy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

func RegisterAPIV1(r *Registry) {
	Register(r, "apiv1", "The stable API of the cache server.", NewAPIV1)
}

var _ Strategy = (*APIV1)(nil)

// The APIV1 strategy represents v1 of the proxy API.
type APIV1 struct {
	cache  cache.Cache
	logger *slog.Logger
}

func NewAPIV1(ctx context.Context, _ struct{}, cache cache.Cache, mux Mux) (*APIV1, error) {
	s := &APIV1{
		logger: logging.FromContext(ctx),
		cache:  cache,
	}
	mux.Handle("GET /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.getObject))
	mux.Handle("HEAD /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.statObject))
	mux.Handle("POST /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.putObject))
	mux.Handle("DELETE /api/v1/object/{namespace}/{key}", http.HandlerFunc(s.deleteObject))
	mux.Handle("GET /api/v1/stats", http.HandlerFunc(s.getStats))
	mux.Handle("GET /api/v1/namespaces", http.HandlerFunc(s.getNamespaces))
	return s, nil
}

func (d *APIV1) String() string { return "default" }

func (d *APIV1) statObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	headers, err := namespacedCache.Stat(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
		return
	}

	httputil.ServeCacheStat(w, r, headers)
}

func (d *APIV1) getObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	namespacedCache := d.cache.Namespace(namespace)

	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		d.getObjectRange(w, r, namespacedCache, key, rangeHeader)
		return
	}

	cr, headers, err := namespacedCache.Open(r.Context(), key, 0, -1)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
		return
	}

	if err := httputil.ServeCacheHit(w, r, headers, cr); err != nil {
		d.logger.Error("Failed to serve cache object", "error", err, "key", key)
	}
}

// getObjectRange serves a byte-range request. It stats the object for its total
// size, then opens and serves the resolved range as 206 Partial Content.
// Unsupported or multi-range headers fall back to serving the full object.
func (d *APIV1) getObjectRange(w http.ResponseWriter, r *http.Request, c cache.Cache, key cache.Key, rangeHeader string) {
	statHeaders, err := c.Stat(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to stat cache object", "key", key)
		return
	}

	// Preconditions are evaluated before the Range (RFC 9110 §13.2.2), so a
	// failed If-Match (412) or satisfied If-None-Match (304) takes precedence
	// over both the range and a 416.
	if status := httputil.CheckConditionals(r, statHeaders.Get(httputil.ETagHeader)); status != 0 {
		maps.Copy(w.Header(), statHeaders)
		w.WriteHeader(status)
		return
	}

	// A stale If-Range validator means the client's partial copy is for an older
	// object; serve the full current object so it cannot append fresh bytes onto
	// a stale prefix (RFC 9110 §13.1.5).
	if !httputil.IfRangeAllowsRange(r, statHeaders.Get(httputil.ETagHeader), statHeaders.Get("Last-Modified")) {
		d.serveFull(w, r, c, key)
		return
	}

	// size comes from Stat and is used only to resolve the range and build the
	// Content-Range total. If the object changes between Stat and Open the body
	// from Open is still internally consistent (its own Content-Length); only
	// the Content-Range total could be momentarily stale, which is acceptable
	// for a best-effort cache.
	size, _ := strconv.ParseInt(statHeaders.Get("Content-Length"), 10, 64) //nolint:errcheck
	start, end, ok, satisfiable := httputil.ParseByteRange(rangeHeader, size)
	if !ok {
		// Unsupported or multi-range header — serve the whole object.
		d.serveFull(w, r, c, key)
		return
	}
	if !satisfiable {
		httputil.ServeRangeNotSatisfiable(w, size)
		return
	}

	cr, headers, err := c.Open(r.Context(), key, start, end)
	if err != nil {
		if errors.Is(err, cache.ErrRangeNotSatisfiable) {
			httputil.ServeRangeNotSatisfiable(w, size)
			return
		}
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
		return
	}

	if err := httputil.ServeCachePartial(w, r, headers, cr, start, end, size); err != nil {
		d.logger.Error("Failed to serve cache object range", "error", err, "key", key)
	}
}

// serveFull opens and serves the entire object as a normal cache hit (200),
// used by the range handler when no usable range applies.
func (d *APIV1) serveFull(w http.ResponseWriter, r *http.Request, c cache.Cache, key cache.Key) {
	cr, headers, err := c.Open(r.Context(), key, 0, -1)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", "key", key)
		return
	}
	if err := httputil.ServeCacheHit(w, r, headers, cr); err != nil {
		d.logger.Error("Failed to serve cache object", "error", err, "key", key)
	}
}

func (d *APIV1) putObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	var ttl time.Duration
	ttlh := r.Header.Get("Time-To-Live")
	if ttlh != "" {
		ttl, err = time.ParseDuration(ttlh)
		if err != nil {
			d.httpError(w, http.StatusBadRequest, err, "Invalid Time-To-Live header format, must be in Go duration format eg. 1h")
			return
		}
	}

	// Extract and filter headers from request
	headers := httputil.FilterHeaders(r.Header, httputil.TransportHeaders...)

	namespacedCache := d.cache.Namespace(namespace)
	cw, err := namespacedCache.Create(r.Context(), key, headers, ttl)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create cache writer", "key", key)
		return
	}

	if _, err := io.Copy(cw, r.Body); err != nil {
		d.httpError(w, http.StatusInternalServerError, errors.Join(err, cw.Abort(err)), "Failed to copy request body to cache writer")
		return
	}

	if err := cw.Close(); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to close cache writer")
		return
	}
}

func (d *APIV1) deleteObject(w http.ResponseWriter, r *http.Request) {
	namespace, err := cache.ParseNamespace(r.PathValue("namespace"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid namespace")
		return
	}
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	namespacedCache := d.cache.Namespace(namespace)
	err = namespacedCache.Delete(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "Cache object not found", http.StatusNotFound)
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to delete cache object", "key", key)
		return
	}
}

func (d *APIV1) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.cache.Stats(r.Context())
	if err != nil {
		if errors.Is(err, cache.ErrStatsUnavailable) {
			d.httpError(w, http.StatusNotImplemented, err, "Stats not available for this cache backend")
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to get cache stats")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		d.logger.Error("Failed to encode stats response", "error", err)
	}
}

func (d *APIV1) getNamespaces(w http.ResponseWriter, r *http.Request) {
	namespaces, err := d.cache.ListNamespaces(r.Context())
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to list namespaces")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(namespaces); err != nil {
		d.logger.Error("Failed to encode namespaces response", "error", err)
	}
}

func (d *APIV1) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, "error", err)
	d.logger.Error(message, args...)
	http.Error(w, message, code)
}
