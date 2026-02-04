// Package strategy provides a framework for implementing and registering different caching strategies.
package strategy

import (
	"context"
	"net/http"
	"os"
	"reflect"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
)

// ErrNotFound is returned when a strategy is not found.
var ErrNotFound = errors.New("strategy not found")

type Mux interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

type registryEntry struct {
	schema  *hcl.Block
	factory func(ctx context.Context, config *hcl.Block, scheduler jobscheduler.Scheduler, cache cache.Cache, mux Mux, vars map[string]string) (Strategy, error)
}

var registry = map[string]registryEntry{}

type Factory[Config any, S Strategy] func(ctx context.Context, config Config, scheduler jobscheduler.Scheduler, cache cache.Cache, mux Mux) (S, error)

// Register a new proxy strategy.
func Register[Config any, S Strategy](id, description string, factory Factory[Config, S]) {
	var c Config
	schema, err := hcl.BlockSchema(id, &c)
	if err != nil {
		panic(err)
	}
	block := schema.Entries[0].(*hcl.Block) //nolint:errcheck // This seems spurious
	block.Comments = hcl.CommentList{description}
	registry[id] = registryEntry{
		schema: block,
		factory: func(ctx context.Context, config *hcl.Block, scheduler jobscheduler.Scheduler, cache cache.Cache, mux Mux, vars map[string]string) (Strategy, error) {
			var cfg Config
			if err := hcl.UnmarshalBlock(config, &cfg, hcl.AllowExtra(false)); err != nil {
				return nil, errors.WithStack(err)
			}
			// Expand environment variables in string fields
			expandStructStrings(&cfg, vars)
			return factory(ctx, cfg, scheduler, cache, mux)
		},
	}
}

// Schema returns the schema for all registered strategies.
func Schema() *hcl.AST {
	ast := &hcl.AST{}
	for _, entry := range registry {
		ast.Entries = append(ast.Entries, entry.schema)
	}
	return ast
}

// Create a new proxy strategy.
//
// Will return "ErrNotFound" if the strategy is not found.
func Create(
	ctx context.Context,
	name string,
	config *hcl.Block,
	scheduler jobscheduler.Scheduler,
	cache cache.Cache,
	mux Mux,
	vars map[string]string,
) (Strategy, error) {
	if entry, ok := registry[name]; ok {
		return errors.WithStack2(entry.factory(ctx, config, scheduler.WithQueuePrefix(name), cache, mux, vars))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

type Strategy interface {
	String() string
}

// expandStructStrings expands environment variables in string fields of a struct.
func expandStructStrings(v any, vars map[string]string) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Struct {
		return
	}

	rv = rv.Elem()
	for i := range rv.NumField() {
		field := rv.Field(i)
		if field.Kind() == reflect.String && field.CanSet() {
			expanded := os.Expand(field.String(), func(key string) string {
				if val, ok := vars[key]; ok {
					return val
				}
				return os.Getenv(key)
			})
			field.SetString(expanded)
		}
	}
}
