// Package remote provides the server and client for a remote [cache.Cache] implementation.
package remote

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/logging"
)

// Server side implementation of the cache protocol.
type Server struct {
	cache  cache.Cache
	logger *slog.Logger
	mux    *http.ServeMux
}

var _ http.Handler = (*Server)(nil)

func NewServer(ctx context.Context, cache cache.Cache) *Server {
	s := &Server{
		logger: logging.FromContext(ctx),
		cache:  cache,
		mux:    http.NewServeMux(),
	}
	s.mux.Handle("GET /{key}", http.HandlerFunc(s.getObject))
	s.mux.Handle("POST /{key}", http.HandlerFunc(s.putObject))
	s.mux.Handle("DELETE /{key}", http.HandlerFunc(s.deleteObject))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		s.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	cr, err := s.cache.Open(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		s.httpError(w, http.StatusInternalServerError, err, "Failed to open cache object", slog.String("key", key.String()))
		return
	}

	_, err = io.Copy(w, cr)
	if err != nil {
		s.logger.Error("Failed to copy cache object to response", slog.String("error", err.Error()), slog.String("key", key.String()))
	}
	if cerr := cr.Close(); cerr != nil {
		s.logger.Error("Failed to close cache reader", slog.String("error", cerr.Error()), slog.String("key", key.String()))
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		s.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	var ttl time.Duration
	ttlh := r.Header.Get("Time-To-Live")
	if ttlh != "" {
		ttl, err = time.ParseDuration(ttlh)
		if err != nil {
			s.httpError(w, http.StatusBadRequest, err, "Invalid Time-To-Live header format, must be in Go duration format eg. 1h")
			return
		}
	}

	cw, err := s.cache.Create(r.Context(), key, ttl)
	if err != nil {
		s.httpError(w, http.StatusInternalServerError, err, "Failed to create cache writer", slog.String("key", key.String()))
		return
	}

	if _, err := io.Copy(cw, r.Body); err != nil {
		s.httpError(w, http.StatusInternalServerError, err, "Failed to copy request body to cache writer")
		return
	}

	if err := cw.Close(); err != nil {
		s.httpError(w, http.StatusInternalServerError, err, "Failed to close cache writer")
		return
	}
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	key, err := cache.ParseKey(r.PathValue("key"))
	if err != nil {
		s.httpError(w, http.StatusBadRequest, err, "Invalid key")
		return
	}

	err = s.cache.Delete(r.Context(), key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			s.httpError(w, http.StatusNotFound, err, "Cache object not found", slog.String("key", key.String()))
			return
		}
		s.httpError(w, http.StatusInternalServerError, err, "Failed to delete cache object", slog.String("key", key.String()))
		return
	}
}

func (s *Server) httpError(w http.ResponseWriter, code int, err error, message string, args ...any) {
	args = append(args, slog.String("error", err.Error()))
	s.logger.Error(message, args...)
	http.Error(w, message, code)
}
