// Package config loads HCL configuration and uses that to construct the cache backend, and proxy strategies.
package config

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
	_ "github.com/block/cachew/internal/strategy/git"   // Register git strategy
	_ "github.com/block/cachew/internal/strategy/gomod" // Register gomod strategy
)

type loggingMux struct {
	logger *slog.Logger
	mux    *http.ServeMux
}

func (l *loggingMux) Handle(pattern string, handler http.Handler) {
	l.logger.Debug("Registered strategy handler", "pattern", pattern)
	l.mux.Handle(pattern, handler)
}

func (l *loggingMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	l.logger.Debug("Registered strategy handler", "pattern", pattern)
	l.mux.HandleFunc(pattern, handler)
}

var _ strategy.Mux = (*loggingMux)(nil)

// Schema returns the configuration file schema.
func Schema() *hcl.AST {
	return &hcl.AST{
		Entries: append(strategy.Schema().Entries, cache.Schema().Entries...),
	}
}

// Load HCL configuration and uses that to construct the cache backend, and proxy strategies.
func Load(ctx context.Context, r io.Reader, scheduler jobscheduler.Scheduler, mux *http.ServeMux, vars map[string]string) error {
	logger := logging.FromContext(ctx)
	ast, err := hcl.Parse(r)
	if err != nil {
		return errors.WithStack(err)
	}

	expandVars(ast, vars)

	strategyCandidates := []*hcl.Block{
		// Always enable the default API strategy
		{Name: "apiv1"},
	}

	// First pass, instantiate caches
	var caches []cache.Cache
	for _, node := range ast.Entries {
		switch node := node.(type) {
		case *hcl.Block:
			c, err := cache.Create(ctx, node.Name, node)
			if errors.Is(err, cache.ErrNotFound) {
				strategyCandidates = append(strategyCandidates, node)
				continue
			} else if err != nil {
				return errors.Errorf("%s: %w", node.Pos, err)
			}
			caches = append(caches, c)

		case *hcl.Attribute:
			return errors.Errorf("%s: attributes are not allowed", node.Pos)
		}
	}
	if len(caches) == 0 {
		return errors.Errorf("%s: expected at least one cache backend", ast.Pos)
	}

	cache := cache.MaybeNewTiered(ctx, caches)

	logger.DebugContext(ctx, "Cache backend", "cache", cache)

	// Second pass, instantiate strategies and bind them to the mux.
	for _, block := range strategyCandidates {
		logger := logger.With("strategy", block.Name)
		mlog := &loggingMux{logger: logger, mux: mux}
		_, err := strategy.Create(ctx, block.Name, block, scheduler.WithQueuePrefix(block.Name), cache, mlog)
		if err != nil {
			return errors.Errorf("%s: %w", block.Pos, err)
		}
	}
	return nil
}

func expandVars(ast *hcl.AST, vars map[string]string) {
	_ = hcl.Visit(ast, func(node hcl.Node, next func() error) error { //nolint:errcheck
		attr, ok := node.(*hcl.Attribute)
		if ok {
			switch attr := attr.Value.(type) {
			case *hcl.String:
				attr.Str = os.Expand(attr.Str, func(s string) string { return vars[s] })
			case *hcl.Heredoc:
				attr.Doc = os.Expand(attr.Doc, func(s string) string { return vars[s] })
			}
		}
		return next()
	})
}
