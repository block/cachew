package metadatadb

import (
	"context"
	"os"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"
)

// ErrNotFound is returned when a metadata backend is not found.
var ErrNotFound = errors.New("metadata backend not found")

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, vars map[string]string) (Backend, error)
}

// Registry holds registered metadata backend factories.
type Registry struct {
	registry map[string]registryEntry
}

// NewRegistry creates a new metadata backend registry.
func NewRegistry() *Registry {
	return &Registry{
		registry: make(map[string]registryEntry),
	}
}

// Factory is a function that creates a new Backend from the given hcl-tagged configuration struct.
type Factory[Config any, B Backend] func(ctx context.Context, config Config) (B, error)

// Register a metadata backend factory function.
func Register[Config any, B Backend](r *Registry, id, description string, factory Factory[Config, B]) {
	var c Config
	schema, err := hcl.BlockSchema(id, &c)
	if err != nil {
		panic(err)
	}
	block := schema.Entries[0].(*hcl.Block) //nolint:errcheck // This seems spurious
	block.Comments = hcl.CommentList{description}
	r.registry[id] = registryEntry{
		schema: block,
		factory: func(ctx context.Context, config *hcl.Block, vars map[string]string) (Backend, error) {
			var cfg Config
			transformer := func(defaultValue string) string {
				return os.Expand(defaultValue, func(key string) string { return vars[key] })
			}
			if err := hcl.UnmarshalBlock(config, &cfg, hcl.WithDefaultTransformer(transformer)); err != nil {
				return nil, errors.WithStack(err)
			}
			return factory(ctx, cfg)
		},
	}
}

// Schema returns the schema for all registered metadata backends.
func (r *Registry) Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range r.registry {
		wrapped := &hcl.Block{
			Name:     "metadata",
			Labels:   append([]string{entry.schema.Name}, entry.schema.Labels...),
			Body:     entry.schema.Body,
			Comments: entry.schema.Comments,
		}
		ast.Entries = append(ast.Entries, wrapped)
	}
	return ast
}

// Exists returns true if a backend with the given name is registered.
func (r *Registry) Exists(name string) bool {
	_, ok := r.registry[name]
	return ok
}

// Create a new Backend from the given name and configuration.
//
// Returns ErrNotFound if the backend is not found.
func (r *Registry) Create(ctx context.Context, name string, config *hcl.Block, vars map[string]string) (Backend, error) {
	entry, ok := r.registry[name]
	if !ok {
		return nil, errors.Errorf("%s: %w", name, ErrNotFound)
	}
	return errors.WithStack2(entry.factory(ctx, config, vars))
}
