package gitcredential_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/gitcredential"
)

func TestNormalizeRepositoryURLScopePreservesGitSuffix(t *testing.T) {
	actual, err := gitcredential.NormalizeRepositoryURLScope("HTTPS://EXAMPLE.COM:443/org/repo.git/")
	assert.NoError(t, err)
	assert.Equal(t, "https://example.com/org/repo.git", actual)
}

func TestNormalizeRepositoryURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{name: "Canonical", input: "HTTPS://Dev.Azure.Com:443/org/project/_git/repo.git/", expected: "https://dev.azure.com/org/project/_git/repo"},
		{name: "Boundary", input: "https://example.com/org/repo-malicious", expected: "https://example.com/org/repo-malicious"},
		{name: "UserInfo", input: "https://user@example.com/org/repo", wantErr: true},
		{name: "Query", input: "https://example.com/org/repo?token=x", wantErr: true},
		{name: "Fragment", input: "https://example.com/org/repo#main", wantErr: true},
		{name: "HTTP", input: "http://example.com/org/repo", wantErr: true},
		{name: "EscapedSeparator", input: "https://example.com/org%2Frepo", wantErr: true},
		{name: "EscapedTraversal", input: "https://example.com/org/%2e%2e/repo", wantErr: true},
		{name: "Traversal", input: "https://example.com/org/../repo", wantErr: true},
		{name: "NonDefaultPort", input: "https://example.com:8443/org/repo", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := gitcredential.NormalizeRepositoryURL(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
