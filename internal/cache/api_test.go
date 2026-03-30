package cache_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
)

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{name: "Simple", input: "git", valid: true},
		{name: "WithHyphen", input: "go-mod", valid: true},
		{name: "WithUnderscore", input: "go_mod", valid: true},
		{name: "WithNumbers", input: "v2cache", valid: true},
		{name: "UpperCase", input: "GitLFS", valid: true},
		{name: "Empty", input: "", valid: false},
		{name: "DotPrefix", input: ".metadata", valid: false},
		{name: "DotInMiddle", input: "go.mod", valid: false},
		{name: "Slash", input: "a/b", valid: false},
		{name: "Space", input: "a b", valid: false},
		{name: "HyphenPrefix", input: "-foo", valid: false},
		{name: "UnderscorePrefix", input: "_foo", valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cache.ValidateNamespace(tt.input)
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
