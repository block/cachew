package gomod_test

import (
	"testing"

	"github.com/block/cachew/internal/strategy/gomod"
)

func TestModulePathMatcher(t *testing.T) {
	tests := []struct {
		name       string
		patterns   []string
		modulePath string
		want       bool
	}{
		{
			name:       "exact match single pattern",
			patterns:   []string{"github.com/squareup"},
			modulePath: "github.com/squareup",
			want:       true,
		},
		{
			name:       "exact match with multiple patterns",
			patterns:   []string{"github.com/org1", "github.com/squareup", "github.com/org2"},
			modulePath: "github.com/squareup",
			want:       true,
		},
		{
			name:       "prefix match - one level deep",
			patterns:   []string{"github.com/squareup"},
			modulePath: "github.com/squareup/repo",
			want:       true,
		},
		{
			name:       "prefix match - two levels deep",
			patterns:   []string{"github.com/squareup"},
			modulePath: "github.com/squareup/repo/submodule",
			want:       true,
		},
		{
			name:       "prefix match with multiple patterns",
			patterns:   []string{"github.com/org1", "github.com/squareup"},
			modulePath: "github.com/squareup/repo",
			want:       true,
		},
		{
			name:       "wildcard match",
			patterns:   []string{"github.com/squareup/*"},
			modulePath: "github.com/squareup/repo",
			want:       true,
		},
		{
			name:       "wildcard match - multiple levels",
			patterns:   []string{"github.com/*/*"},
			modulePath: "github.com/squareup/repo",
			want:       true,
		},
		{
			name:       "no match - different org",
			patterns:   []string{"github.com/squareup"},
			modulePath: "github.com/other/repo",
			want:       false,
		},
		{
			name:       "no match - different host",
			patterns:   []string{"github.com/squareup"},
			modulePath: "gitlab.com/squareup/repo",
			want:       false,
		},
		{
			name:       "no match - prefix without slash",
			patterns:   []string{"github.com/square"},
			modulePath: "github.com/squareup/repo",
			want:       false,
		},
		{
			name:       "no match - empty patterns",
			patterns:   []string{},
			modulePath: "github.com/squareup/repo",
			want:       false,
		},
		{
			name:       "empty module path",
			patterns:   []string{"github.com/squareup"},
			modulePath: "",
			want:       false,
		},
		{
			name:       "multiple patterns with no match",
			patterns:   []string{"github.com/org1", "github.com/org2", "github.com/org3"},
			modulePath: "github.com/squareup/repo",
			want:       false,
		},
		{
			name:       "pattern with trailing slash",
			patterns:   []string{"github.com/squareup/"},
			modulePath: "github.com/squareup/repo",
			want:       false,
		},
		{
			name:       "gopkg.in pattern",
			patterns:   []string{"gopkg.in/square"},
			modulePath: "gopkg.in/square/go-jose.v2",
			want:       true,
		},
		{
			name:       "nested GitHub org pattern",
			patterns:   []string{"github.com/squareup/internal"},
			modulePath: "github.com/squareup/internal/auth",
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher := gomod.NewModulePathMatcher(tt.patterns)
			got := matcher.IsPrivate(tt.modulePath)
			if got != tt.want {
				t.Errorf("IsPrivate() = %v, want %v", got, tt.want)
			}
		})
	}
}
