// Package strategy provides a framework for implementing and registering different caching strategies.
package strategy

import (
	"context"
	"net/http"
	"os"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/metadatadb"
)

// ErrNotFound is returned when a strategy is not found.
var ErrNotFound = errors.New("strategy not found")

// Mux is the subset of http.ServeMux that strategies use to register their routes.
type Mux interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

// Registry maps strategy names to their HCL schemas and factories.
type Registry struct {
	registry map[string]registryEntry
}

// NewRegistry returns an empty strategy Registry.
func NewRegistry() *Registry {
	return &Registry{
		registry: make(map[string]registryEntry),
	}
}

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, cache cache.Cache, mux Mux, vars map[string]string) (Strategy, error)
}

// Factory creates a strategy from its decoded config, a cache namespaced to the strategy, and a mux for its routes.
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

// Schema returns the schema for all registered strategies, each wrapped as a "strategy <name> { ... }" block.
func (r *Registry) Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range r.registry {
		wrapped := &hcl.Block{
			Name:     "strategy",
			Labels:   append([]string{entry.schema.Name}, entry.schema.Labels...),
			Body:     entry.schema.Body,
			Comments: entry.schema.Comments,
			Repeated: true,
		}
		ast.Entries = append(ast.Entries, wrapped)
	}
	return ast
}

// Exists reports whether a strategy is registered under name.
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
	c cache.Cache,
	mux Mux,
	vars map[string]string,
) (Strategy, error) {
	ns, err := cache.ParseNamespace(name)
	if err != nil {
		return nil, errors.Errorf("strategy %q: %w", name, err)
	}
	if entry, ok := r.registry[name]; ok {
		namespacedCache := c.Namespace(ns)
		return errors.WithStack2(entry.factory(ctx, config, namespacedCache, mux, vars))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

// Strategy is a protocol-aware caching mechanism that serves requests through routes registered at creation.
type Strategy interface {
	String() string
}

// Interceptor is an optional Strategy interface for intercepting requests before ServeMux route matching.
//
// Needed by strategies like the HTTP proxy that inspect r.RequestURI: a "/" catch-all on the mux is
// insufficient because more-specific routes (e.g. /api/v1/) still win for overlapping paths.
type Interceptor interface {
	Strategy
	// Intercept wraps next, intercepting matching requests and delegating all others to next.
	Intercept(next http.Handler) http.Handler
}

// MetadataConsumer is an optional Strategy interface for receiving the metadata store after construction.
//
// config.Load calls SetMetadataStore once the metadata backend is built, avoiding a construction-order
// cycle because both strategies and the metadata backend are created inside config.Load.
type MetadataConsumer interface {
	Strategy
	SetMetadataStore(*metadatadb.Store)
}

// Readier is an optional Strategy interface that gates the /_readiness probe on background warm-up.
//
// The listener and /_liveness come up immediately so the kubelet doesn't restart the pod, but the Service
// load balancer holds traffic until every Readier reports true.
type Readier interface {
	Strategy
	// Ready reports whether the strategy is ready to serve traffic.
	Ready() bool
}
