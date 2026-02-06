// Package strategy provides a framework for implementing and registering different caching strategies.
package strategy

import (
	"context"
	"net/http"
	"os"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
)

// ErrNotFound is returned when a strategy is not found.
var ErrNotFound = errors.New("strategy not found")

type Mux interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

type Registry struct {
	registry map[string]registryEntry
}

func NewRegistry() *Registry {
	return &Registry{
		registry: make(map[string]registryEntry),
	}
}

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, cache cache.Cache, mux Mux, vars map[string]string) (Strategy, error)
}

type Factory[Config any, S Strategy] func(ctx context.Context, config Config, cache cache.Cache, mux Mux) (S, error)

// Register a new proxy strategy.
func Register[Config any, S Strategy](r *Registry, id, description string, factory Factory[Config, S]) {
	var c Config
	schema, err := hcl.BlockSchema(id, &c)
	if err != nil {
		panic(err)
	}
	block := schema.Entries[0].(*hcl.Block) //nolint:errcheck // This seems spurious
	block.Comments = hcl.CommentList{description}
	r.registry[id] = registryEntry{
		schema: block,
		factory: func(ctx context.Context, config *hcl.Block, cache cache.Cache, mux Mux, vars map[string]string) (Strategy, error) {
			var cfg Config
			transformer := func(defaultValue string) string {
				return os.Expand(defaultValue, func(key string) string { return vars[key] })
			}
			if err := hcl.UnmarshalBlock(config, &cfg, hcl.AllowExtra(false), hcl.WithDefaultTransformer(transformer)); err != nil {
				return nil, errors.WithStack(err)
			}
			return factory(ctx, cfg, cache, mux)
		},
	}
}

// Schema returns the schema for all registered strategies.
func (r *Registry) Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range r.registry {
		ast.Entries = append(ast.Entries, entry.schema)
	}
	return ast
}

func (r *Registry) Exists(name string) bool {
	_, ok := r.registry[name]
	return ok
}

// Create a new proxy strategy.
//
// Will return "ErrNotFound" if the strategy is not found.
func (r *Registry) Create(
	ctx context.Context,
	name string,
	config *hcl.Block,
	cache cache.Cache,
	mux Mux,
	vars map[string]string,
) (Strategy, error) {
	if entry, ok := r.registry[name]; ok {
		return errors.WithStack2(entry.factory(ctx, config, cache, mux, vars))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

type Strategy interface {
	String() string
}
