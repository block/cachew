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
	"github.com/block/cachew/internal/metadatadb"
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
func Schema[GlobalConfig any](cr *cache.Registry, mr *metadatadb.Registry, sr *strategy.Registry) *hcl.AST {
	globalSchema, err := hcl.Schema(new(GlobalConfig))
	if err != nil {
		panic(err)
	}
	globalSchema.Entries = append(globalSchema.Entries, sr.Schema().Entries...)
	globalSchema.Entries = append(globalSchema.Entries, cr.Schema().Entries...)
	globalSchema.Entries = append(globalSchema.Entries, mr.Schema().Entries...)
	return globalSchema
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

// unwrapBlock extracts the registry name from a prefixed block (e.g. "cache disk { ... }")
// and returns a copy of the block with Name set to the first label and remaining labels preserved.
func unwrapBlock(block *hcl.Block) (name string, inner *hcl.Block, err error) {
	if len(block.Labels) == 0 {
		return "", nil, errors.Errorf("%s: %s block requires a name label", block.Pos, block.Name)
	}
	inner = &hcl.Block{
		Pos:      block.Pos,
		Name:     block.Labels[0],
		Labels:   block.Labels[1:],
		Body:     block.Body,
		Comments: block.Comments,
	}
	return block.Labels[0], inner, nil
}

type classifiedBlocks struct {
	caches     []*hcl.Block
	metadata   *hcl.Block
	strategies []*hcl.Block
}

func classifyBlocks(ast *hcl.AST) (*classifiedBlocks, error) {
	result := &classifiedBlocks{
		// Always enable the default API strategy
		strategies: []*hcl.Block{{Name: "apiv1"}},
	}
	for _, node := range ast.Entries {
		block, ok := node.(*hcl.Block)
		if !ok {
			return nil, errors.Errorf("%s: attributes are not allowed", node.Position())
		}
		switch block.Name {
		case "cache":
			result.caches = append(result.caches, block)
		case "metadata":
			if result.metadata != nil {
				return nil, errors.Errorf("%s: only one metadata block is allowed", block.Pos)
			}
			result.metadata = block
		case "strategy":
			_, inner, err := unwrapBlock(block)
			if err != nil {
				return nil, err
			}
			result.strategies = append(result.strategies, inner)
		default:
			return nil, errors.Errorf("%s: unknown block %q (expected \"cache\", \"metadata\", or \"strategy\")", block.Pos, block.Name)
		}
	}
	return result, nil
}

// Load HCL configuration and use that to construct the cache backend, and proxy strategies.
// It returns an http.Handler that wraps mux — any loaded strategies that implement
// strategy.Interceptor are applied as middleware before ServeMux route matching, so
// that they can inspect r.RequestURI rather than the path-only r.URL.Path.
func Load(
	ctx context.Context,
	cr *cache.Registry,
	mr *metadatadb.Registry,
	sr *strategy.Registry,
	ast *hcl.AST,
	mux *http.ServeMux,
	vars map[string]string,
) (http.Handler, []strategy.Readier, error) {
	logger := logging.FromContext(ctx)
	expandVars(ast, vars)

	classified, err := classifyBlocks(ast)
	if err != nil {
		return nil, nil, err
	}

	var caches []cache.Cache
	for _, block := range classified.caches {
		name, inner, err := unwrapBlock(block)
		if err != nil {
			return nil, nil, err
		}
		c, err := cr.Create(ctx, name, inner, vars)
		if err != nil {
			return nil, nil, errors.Errorf("%s: %w", block.Pos, err)
		}
		caches = append(caches, c)
	}
	if len(caches) == 0 {
		return nil, nil, errors.Errorf("%s: expected at least one cache backend", ast.Pos)
	}

	if classified.metadata == nil {
		return nil, nil, errors.Errorf("%s: expected a metadata backend", ast.Pos)
	}
	metaName, metaInner, err := unwrapBlock(classified.metadata)
	if err != nil {
		return nil, nil, err
	}
	metadata, err := mr.Create(ctx, metaName, metaInner, vars)
	if err != nil {
		return nil, nil, errors.Errorf("%s: %w", classified.metadata.Pos, err)
	}

	metadataStore := metadatadb.New(ctx, metadata)
	cache := cache.MaybeNewTiered(ctx, caches, metadataStore)

	logger.DebugContext(ctx, "Cache backend", "cache", cache)

	// Second pass, instantiate strategies and bind them to the mux.
	// Collect strategies that implement Interceptor separately — they need
	// to run before ServeMux route matching, not as mux routes. Strategies
	// that implement Readier are tracked so /_readiness can gate on warm-up.
	var interceptors []strategy.Interceptor
	var readiers []strategy.Readier
	for _, block := range classified.strategies {
		name := block.Name
		slogger := logger.With("strategy", name)
		mlog := &loggingMux{logger: slogger, mux: mux}
		s, err := sr.Create(ctx, name, block, cache, mlog, vars)
		if err != nil {
			return nil, nil, errors.Errorf("%s: %w", block.Pos, err)
		}
		if mc, ok := s.(strategy.MetadataConsumer); ok {
			mc.SetMetadataStore(metadataStore)
		}
		if interceptor, ok := s.(strategy.Interceptor); ok {
			interceptors = append(interceptors, interceptor)
		}
		if readier, ok := s.(strategy.Readier); ok {
			readiers = append(readiers, readier)
		}
	}

	// Wrap the mux with interceptors. The last-registered interceptor runs
	// outermost so that registration order matches interception order.
	var h http.Handler = mux
	for i := len(interceptors) - 1; i >= 0; i-- {
		h = interceptors[i].Intercept(h)
	}
	return h, readiers, nil
}

// expandVars expands environment variable references in HCL `*hcl.String`
// and `*hcl.Heredoc` attribute values in-place. It is a low-level helper;
// most callers should go through `InjectEnvars`, which performs both
// schema-driven attribute injection for absent attributes AND placeholder
// expansion in attribute values that the operator wrote with `${VAR}`.
//
// Heredoc handling is required so that templated policy text inside global
// blocks (e.g. `opa { policy = <<EOF ... ${MY_PRINCIPAL} ... EOF }`) reaches
// downstream consumers with placeholders substituted. The native
// `hcl.WithDefaultTransformer` only runs against struct-tag default values
// in `valueFromTag`, never against live AST node strings.
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

// InjectEnvars resolves environment variables against the config AST in two
// passes:
//
//  1. Schema-driven injection: walks the schema and for each attribute not
//     already present in the config, checks for a corresponding environment
//     variable and inserts it as a new attribute. Environment variable names
//     are derived from the path to the attribute: prefix + block names +
//     attribute name, joined with "_", uppercased, hyphens replaced with "_".
//     E.g. prefix="CACHEW", path=["scheduler", "concurrency"] resolves to
//     "CACHEW_SCHEDULER_CONCURRENCY".
//  2. Placeholder expansion: walks the resulting AST and substitutes `${VAR}`
//     references inside `*hcl.String` and `*hcl.Heredoc` attribute values
//     using `os.Expand` semantics (unset names collapse to empty). This
//     covers placeholders the operator wrote into the config file directly,
//     including those inside heredocs (e.g. an OPA policy block).
//
// Both passes operate on the same `vars` map. Pass (1) only ever inserts
// attributes that were absent, so it cannot overwrite operator-written values
// that pass (2) then expands.
func InjectEnvars(schema *hcl.AST, config *hcl.AST, prefix string, vars map[string]string) {
	container := &entryContainer{ast: config}
	injectEntries(schema.Entries, container, []string{prefix}, vars)
	expandVars(config, vars)
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
