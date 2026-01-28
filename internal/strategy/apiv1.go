package strategy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"time"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
)

func init() {
	Register("apiv1", "The stable API of the cache server.", NewAPIV1)
}

var _ Strategy = (*APIV1)(nil)

// The APIV1 strategy represents v1 of the proxy API.
type APIV1 struct {
	cache  cache.Cache
	logger *slog.Logger
}

func NewAPIV1(ctx context.Context, _ struct{}, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*APIV1, error) {
	s := &APIV1{
		logger: logging.FromContext(ctx),
		cache:  cache,
	}
	mux.Handle("GET /api/v1/object/{key}", http.HandlerFunc(s.getObject))
	mux.Handle("HEAD /api/v1/object/{key}", http.HandlerFunc(s.statObject))
	mux.Handle("POST /api/v1/object/{key}", http.HandlerFunc(s.putObject))
	mux.Handle("DELETE /api/v1/object/{key}", http.HandlerFunc(s.deleteObject))
	mux.Handle("GET /api/v1/stats", http.HandlerFunc(s.getStats))
	return s, nil
}

func (d *APIV1) String() string { return "default" }

func (d *APIV1) statObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	headers, err := d.cache.Stat(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", slog.String("key", key.String()))
		return
	}

	maps.Copy(w.Header(), headers)
	w.WriteHeader(http.StatusOK)
}

func (d *APIV1) getObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	cr, headers, err := d.cache.Open(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", slog.String("key", key.String()))
		return
	}

	maps.Copy(w.Header(), headers)

	_, err = io.Copy(w, cr)
	if err != nil {
		d.logger.Error("Failed to copy cache object to response", slog.String("error", err.Error()), slog.String("key", key.String()))
	}
	if cerr := cr.Close(); cerr != nil {
		d.logger.Error("Failed to close cache reader", slog.String("error", cerr.Error()), slog.String("key", key.String()))
	}
}

func (d *APIV1) putObject(w http.ResponseWriter, r *http.Request) {
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
	headers := cache.FilterTransportHeaders(r.Header)

	cw, err := d.cache.Create(r.Context(), key, headers, ttl)
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to create cache writer", slog.String("key", key.String()))
		return
	}

	if _, err := io.Copy(cw, r.Body); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to copy request body to cache writer")
		return
	}

	if err := cw.Close(); err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to close cache writer")
		return
	}
}

func (d *APIV1) deleteObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		d.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	err = d.cache.Delete(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		d.httpError(w, http.StatusInternalServerError, err, "Failed to delete cache object", slog.String("key", key.String()))
		return
	}
}

func (d *APIV1) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.cache.Stats(r.Context())
	if err != nil {
		d.httpError(w, http.StatusInternalServerError, err, "Failed to get cache stats")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		d.logger.Error("Failed to encode stats response", slog.String("error", err.Error()))
	}
}

func (d *APIV1) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, slog.String("error", err.Error()))
	d.logger.Error(message, args...)
	http.Error(w, message, code)
}
