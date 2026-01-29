package gomod

import (
	"path"
	"strings"
)

// ModulePathMatcher matches module paths against patterns.
type ModulePathMatcher struct {
	patterns []string
}

// NewModulePathMatcher creates a new matcher with the given patterns.
func NewModulePathMatcher(patterns []string) *ModulePathMatcher {
	return &ModulePathMatcher{patterns: patterns}
}

// IsPrivate checks if a module path matches any private pattern.
func (m *ModulePathMatcher) IsPrivate(modulePath string) bool {
	for _, pattern := range m.patterns {
		matched, err := path.Match(pattern, modulePath)
		if err == nil && matched {
			return true
		}

		if strings.HasPrefix(modulePath, pattern+"/") || modulePath == pattern {
			return true
		}
	}

	return false
}
