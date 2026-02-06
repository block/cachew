package config //nolint:testpackage

import (
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"
)

func TestFlattenHCL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]any
	}{
		{
			name:  "SimpleAttribute",
			input: `value = "foo"`,
			expected: map[string]any{
				"value": "foo",
			},
		},
		{
			name: "Block",
			input: `block {
				value = "foo"
			}`,
			expected: map[string]any{
				"block-value": "foo",
			},
		},
		{
			name: "BlockWithLabel",
			input: `block-with-label label {
				value = "foo"
			}`,
			expected: map[string]any{
				"block-with-label-label-value": "foo",
			},
		},
		{
			name: "NestedBlocks",
			input: `outer {
				inner {
					value = "foo"
				}
			}`,
			expected: map[string]any{
				"outer-inner-value": "foo",
			},
		},
		{
			name:  "NumberInt",
			input: `count = 42`,
			expected: map[string]any{
				"count": int64(42),
			},
		},
		{
			name:  "NumberFloat",
			input: `ratio = 3.14`,
			expected: map[string]any{
				"ratio": 3.14,
			},
		},
		{
			name:  "Bool",
			input: `enabled = true`,
			expected: map[string]any{
				"enabled": true,
			},
		},
		{
			name:  "List",
			input: `tags = ["a", "b", "c"]`,
			expected: map[string]any{
				"tags": []any{"a", "b", "c"},
			},
		},
		{
			name:  "Map",
			input: `labels = {x: 1, y: 2}`,
			expected: map[string]any{
				"labels": map[string]any{"x": int64(1), "y": int64(2)},
			},
		},
		{
			name: "MultipleEntries",
			input: `
				name = "test"
				block {
					port = 8080
				}
			`,
			expected: map[string]any{
				"name":       "test",
				"block-port": int64(8080),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, err := hcl.Parse(strings.NewReader(tt.input))
			assert.NoError(t, err)
			actual := flattenHCL(ast)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
