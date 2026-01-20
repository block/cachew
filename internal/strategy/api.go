// Package strategy provides a framework for implementing and registering different caching strategies.
package strategy

import (
	"context"
	"net/http"

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

var registry = map[string]func(ctx context.Context, scheduler jobscheduler.Scheduler, config *hcl.Block, cache cache.Cache, mux Mux) (Strategy, error){}

type Factory[Config any, S Strategy] func(ctx context.Context, scheduler jobscheduler.Scheduler, config Config, cache cache.Cache, mux Mux) (S, error)

// Register a new proxy strategy.
func Register[Config any, S Strategy](id string, factory Factory[Config, S]) {
	registry[id] = func(ctx context.Context, scheduler jobscheduler.Scheduler, config *hcl.Block, cache cache.Cache, mux Mux) (Strategy, error) {
		var cfg Config
		if err := hcl.UnmarshalBlock(config, &cfg, hcl.AllowExtra(false)); err != nil {
			return nil, errors.WithStack(err)
		}
		return factory(ctx, scheduler, cfg, cache, mux)
	}
}

// Create a new proxy strategy.
//
// Will return "ErrNotFound" if the strategy is not found.
func Create(
	ctx context.Context,
	scheduler jobscheduler.Scheduler,
	name string,
	config *hcl.Block,
	cache cache.Cache,
	mux Mux,
) (Strategy, error) {
	if factory, ok := registry[name]; ok {
		return errors.WithStack2(factory(ctx, scheduler.WithQueuePrefix(name), config, cache, mux))
	}
	return nil, errors.Errorf("%s: %w", name, ErrNotFound)
}

type Strategy interface {
	String() string
}
