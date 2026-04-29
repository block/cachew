package config //nolint:testpackage

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/strategy"
)

func TestUnwrapBlock(t *testing.T) {
	tests := []struct {
		name           string
		block          *hcl.Block
		expectedName   string
		expectedLabels []string
		expectedErr    string
	}{
		{
			name:           "SimpleBlock",
			block:          &hcl.Block{Name: "cache", Labels: []string{"disk"}},
			expectedName:   "disk",
			expectedLabels: []string{},
		},
		{
			name:           "BlockWithExtraLabels",
			block:          &hcl.Block{Name: "strategy", Labels: []string{"host", "https://ghcr.io"}},
			expectedName:   "host",
			expectedLabels: []string{"https://ghcr.io"},
		},
		{
			name:        "MissingLabel",
			block:       &hcl.Block{Name: "cache"},
			expectedErr: "cache block requires a name label",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, inner, err := unwrapBlock(tt.block)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedName, inner.Name)
			assert.Equal(t, tt.expectedLabels, inner.Labels)
		})
	}
}

func TestInjectEnvars(t *testing.T) {
	type Scheduler struct {
		Concurrency int `hcl:"concurrency"`
	}
	type GitClone struct {
		Depth int    `hcl:"depth"`
		Dir   string `hcl:"dir"`
	}
	type Config struct {
		Bind      string    `hcl:"bind"`
		Scheduler Scheduler `hcl:"scheduler,block"`
		GitClone  GitClone  `hcl:"git-clone,block"`
	}

	schema, err := hcl.Schema(new(Config))
	assert.NoError(t, err)

	tests := []struct {
		name     string
		config   string
		vars     map[string]string
		expected string
	}{
		{
			name:   "InjectTopLevelAttr",
			config: ``,
			vars:   map[string]string{"CACHEW_BIND": "0.0.0.0:9090"},
			expected: `
bind = "0.0.0.0:9090"
`,
		},
		{
			name:   "InjectNestedAttr",
			config: `bind = "127.0.0.1:8080"`,
			vars:   map[string]string{"CACHEW_SCHEDULER_CONCURRENCY": "10"},
			expected: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 10
}
`,
		},
		{
			name: "ExistingAttrNotOverwritten",
			config: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 4
}
`,
			vars: map[string]string{"CACHEW_SCHEDULER_CONCURRENCY": "10"},
			expected: `
bind = "127.0.0.1:8080"

scheduler {
  concurrency = 4
}
`,
		},
		{
			name: "InjectIntoExistingBlock",
			config: `
git-clone {
  depth = 1
}
`,
			vars: map[string]string{"CACHEW_GIT_CLONE_DIR": "/tmp/clones"},
			expected: `
git-clone {
  depth = 1
  dir = "/tmp/clones"
}
`,
		},
		{
			name:   "NoMatchingEnvar",
			config: `bind = "127.0.0.1:8080"`,
			vars:   map[string]string{"UNRELATED_VAR": "foo"},
			expected: `
bind = "127.0.0.1:8080"
`,
		},
		{
			name:     "EmptyBlockNotCreated",
			config:   ``,
			vars:     map[string]string{},
			expected: ``,
		},
		{
			name:   "MultipleInjections",
			config: ``,
			vars: map[string]string{
				"CACHEW_BIND":                  "0.0.0.0:9090",
				"CACHEW_SCHEDULER_CONCURRENCY": "8",
				"CACHEW_GIT_CLONE_DEPTH":       "3",
			},
			expected: `
bind = "0.0.0.0:9090"

scheduler {
  concurrency = 8
}

git-clone {
  depth = 3
}
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := hcl.Parse(strings.NewReader(tt.config))
			assert.NoError(t, err)

			InjectEnvars(schema, config, "CACHEW", tt.vars)

			got, err := hcl.MarshalAST(config)
			assert.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tt.expected), strings.TrimSpace(string(got)))
		})
	}
}

func TestLoadRequiresMetadataBackend(t *testing.T) {
	cr := cache.NewRegistry()
	cache.RegisterMemory(cr)
	mr := metadatadb.NewRegistry()
	metadatadb.RegisterMemory(mr)
	sr := strategy.NewRegistry()

	ast, err := hcl.Parse(strings.NewReader(`cache memory {}`))
	assert.NoError(t, err)

	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	_, _, _, err = Load(ctx, cr, mr, sr, ast, http.NewServeMux(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected a metadata backend")
}
