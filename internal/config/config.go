// Package config loads HCL configuration and uses that to construct the cache backend, and proxy strategies.
package config

import (
	"context"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
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
func Schema[GlobalConfig any](cr *cache.Registry, sr *strategy.Registry) *hcl.AST {
	globalSchema, err := hcl.Schema(new(GlobalConfig))
	if err != nil {
		panic(err)
	}
	return &hcl.AST{
		Entries: append(globalSchema.Entries, append(sr.Schema().Entries, cr.Schema().Entries...)...),
	}
}

// Split configuration into global config and provider-specific config.
//
// At this point we don't know what config the providers require, so we just pull out the global config and assume
// everything else is for the providers.
func Split[GlobalConfig any](ast *hcl.AST) (global, providers *hcl.AST) {
	globalSchema, err := hcl.Schema(new(GlobalConfig))
	if err != nil {
		panic(err)
	}

	globals := map[string]bool{}
	for _, entry := range globalSchema.Entries {
		switch entry.(type) {
		case *hcl.Attribute, *hcl.Block:
			globals[entry.EntryKey()] = true
		}
	}

	global = &hcl.AST{Pos: ast.Pos}
	providers = &hcl.AST{Pos: ast.Pos}

	for _, node := range ast.Entries {
		switch node := node.(type) {
		case *hcl.Block:
			if globals[node.Name] {
				global.Entries = append(global.Entries, node)
			} else {
				providers.Entries = append(providers.Entries, node)
			}

		case *hcl.Attribute: // Attributes are always for the global config
			global.Entries = append(global.Entries, node)
		}
	}

	return global, providers
}

// Load HCL configuration and use that to construct the cache backend, and proxy strategies.
func Load(
	ctx context.Context,
	cr *cache.Registry,
	sr *strategy.Registry,
	ast *hcl.AST,
	mux *http.ServeMux,
	vars map[string]string,
) error {
	logger := logging.FromContext(ctx)
	ExpandVars(ast, vars)

	strategyCandidates := []*hcl.Block{
		// Always enable the default API strategy
		{Name: "apiv1"},
	}

	// First pass, instantiate caches
	var caches []cache.Cache
	for _, node := range ast.Entries {
		switch node := node.(type) {
		case *hcl.Block:
			c, err := cr.Create(ctx, node.Name, node)
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
		_, err := sr.Create(ctx, block.Name, block, cache, mlog, vars)
		if err != nil {
			return errors.Errorf("%s: %w", block.Pos, err)
		}
	}
	return nil
}

// ParseEnvars returns a map of all environment variables.
func ParseEnvars() map[string]string {
	envars := make(map[string]string)
	for _, env := range os.Environ() {
		if key, value, ok := strings.Cut(env, "="); ok {
			envars[key] = value
		}
	}
	return envars
}

// ExpandVars expands environment variable references in HCL strings and heredocs.
func ExpandVars(ast *hcl.AST, vars map[string]string) {
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

// InjectEnvars walks the schema and for each attribute not present in the config,
// checks for a corresponding environment variable and injects it.
//
// Environment variable names are derived from the path to the attribute:
// prefix + block names + attr name, joined with "_", uppercased, hyphens replaced with "_".
// e.g. prefix="CACHEW", path=["scheduler", "concurrency"] -> "CACHEW_SCHEDULER_CONCURRENCY".
func InjectEnvars(schema *hcl.AST, config *hcl.AST, prefix string, vars map[string]string) {
	container := &entryContainer{ast: config}
	injectEntries(schema.Entries, container, []string{prefix}, vars)
	_ = hcl.AddParentRefs(config) //nolint:errcheck
}

// entryContainer abstracts over AST (top-level) and Block (nested) for inserting entries.
type entryContainer struct {
	ast   *hcl.AST
	block *hcl.Block
}

func (c *entryContainer) entries() hcl.Entries {
	if c.block != nil {
		return c.block.Body
	}
	return c.ast.Entries
}

func (c *entryContainer) append(entry hcl.Entry) {
	if c.block != nil {
		c.block.Body = append(c.block.Body, entry)
	} else {
		c.ast.Entries = append(c.ast.Entries, entry)
	}
}

func (c *entryContainer) findBlock(name string) *entryContainer {
	for _, e := range c.entries() {
		if block, ok := e.(*hcl.Block); ok && block.Name == name {
			return &entryContainer{ast: c.ast, block: block}
		}
	}
	return nil
}

func injectEntries(schemaEntries hcl.Entries, container *entryContainer, path []string, vars map[string]string) {
	for _, entry := range schemaEntries {
		switch entry := entry.(type) {
		case *hcl.Attribute:
			typ, ok := entry.Value.(*hcl.Type)
			if !ok {
				continue
			}
			envarName := pathToEnvar(append(slices.Clone(path), entry.Key))
			val, ok := vars[envarName]
			if !ok {
				continue
			}
			if hasAttr(container.entries(), entry.Key) {
				continue
			}
			hclVal, err := parseValue(val, typ.Type)
			if err != nil {
				continue
			}
			container.append(&hcl.Attribute{Key: entry.Key, Value: hclVal})

		case *hcl.Block:
			child := container.findBlock(entry.Name)
			if child == nil {
				// Create a temporary container; only add the block to the
				// config if at least one envar populated it.
				tmp := &entryContainer{ast: container.ast, block: &hcl.Block{Name: entry.Name}}
				injectEntries(entry.Body, tmp, append(path, entry.Name), vars)
				if len(tmp.block.Body) > 0 {
					container.append(tmp.block)
				}
			} else {
				injectEntries(entry.Body, child, append(path, entry.Name), vars)
			}
		}
	}
}

func pathToEnvar(path []string) string {
	s := strings.Join(path, "_")
	s = strings.ReplaceAll(s, "-", "_")
	return strings.ToUpper(s)
}

func hasAttr(entries hcl.Entries, key string) bool {
	for _, e := range entries {
		if attr, ok := e.(*hcl.Attribute); ok && attr.Key == key {
			return true
		}
	}
	return false
}

func parseValue(raw string, typ string) (hcl.Value, error) {
	switch typ {
	case "string":
		return &hcl.String{Str: raw}, nil
	case "number":
		f, _, err := big.ParseFloat(raw, 10, 256, big.ToNearestEven)
		if err != nil {
			return nil, errors.Wrap(err, raw)
		}
		return &hcl.Number{Float: f}, nil
	case "boolean":
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.Wrap(err, raw)
		}
		return &hcl.Bool{Bool: b}, nil
	default:
		return nil, errors.Errorf("unsupported type %q", typ)
	}
}
