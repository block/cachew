// Package server implements the HTTP server for the caching proxy.
package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/remote"
	"github.com/block/sfptc/internal/logging"
)

type Option func(*Server)

type Server struct {
	logger *slog.Logger
	cache  cache.Cache
	mux    *http.ServeMux
	server *remote.Server
}

var _ http.Handler = (*Server)(nil)

func New(ctx context.Context, cache cache.Cache, options ...Option) *Server {
	s := &Server{
		logger: logging.FromContext(ctx),
		cache:  cache,
		mux:    http.NewServeMux(),
		server: remote.NewServer(ctx, cache),
	}
	for _, option := range options {
		option(s)
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}
