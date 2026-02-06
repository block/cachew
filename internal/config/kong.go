package config

import (
	"fmt"
	"io"
	"strings"

	"github.com/alecthomas/hcl/v2"
	"github.com/alecthomas/kong"
)

func KongLoader[GlobalConfig any](r io.Reader) (kong.Resolver, error) {
	ast, err := hcl.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HCL: %w", err)
	}
	return &kongResolver{flattenHCL(ast)}, nil
}

type kongResolver struct {
	values map[string]any
}

var _ kong.Resolver = (*kongResolver)(nil)

func (k *kongResolver) Resolve(_ *kong.Context, _ *kong.Path, flag *kong.Flag) (any, error) {
	name := strings.ReplaceAll(flag.Name, "-", "_")
	if v, ok := k.values[name]; ok {
		return v, nil
	}
	if v, ok := k.values[flag.Name]; ok {
		return v, nil
	}
	return nil, nil //nolint:nilnil
}

func (k *kongResolver) Validate(_ *kong.Application) error { return nil }

// Convert HCL AST to a flattened map of key to value. Each hierarchy in the HCL joined by "-".
//
// eg.
//
//	block {
//		value = "foo"
//	}
//
//	block-with-label label {
//		value = "foo"
//	}
//
// Would flatten to:
//
//	block-value = "foo"
//	block-with-label-label = "foo"
func flattenHCL(node hcl.Node) map[string]any {
	out := map[string]any{}
	flattenNode(out, "", node)
	return out
}

func flattenNode(out map[string]any, prefix string, node hcl.Node) {
	switch node := node.(type) {
	case *hcl.AST:
		for _, entry := range node.Entries {
			flattenNode(out, prefix, entry)
		}

	case *hcl.Block:
		parts := make([]string, 0, 1+len(node.Labels))
		parts = append(parts, node.Name)
		parts = append(parts, node.Labels...)
		key := strings.Join(parts, "-")
		if prefix != "" {
			key = prefix + "-" + key
		}
		for _, entry := range node.Body {
			flattenNode(out, key, entry)
		}

	case *hcl.Attribute:
		key := node.Key
		if prefix != "" {
			key = prefix + "-" + key
		}
		out[key] = hclValue(node.Value)
	}
}

func hclValue(v hcl.Value) any {
	switch v := v.(type) {
	case *hcl.String:
		return v.Str
	case *hcl.Number:
		if v.Float.IsInt() {
			i, _ := v.Float.Int64()
			return i
		}
		f, _ := v.Float.Float64()
		return f
	case *hcl.Bool:
		return v.Bool
	case *hcl.List:
		out := make([]any, len(v.List))
		for i, item := range v.List {
			out[i] = hclValue(item)
		}
		return out
	case *hcl.Map:
		out := map[string]any{}
		for _, entry := range v.Entries {
			out[fmt.Sprintf("%v", hclValue(entry.Key))] = hclValue(entry.Value)
		}
		return out
	case *hcl.Heredoc:
		return v.GetHeredoc()
	default:
		return nil
	}
}
