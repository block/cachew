package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
)

func TestGetInsteadOfDisableArgsForURL(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		targetURL string
		skipTest  bool
	}{
		{
			name:      "EmptyURL",
			targetURL: "",
			skipTest:  false,
		},
		{
			name:      "GitHubURL",
			targetURL: "https://github.com/user/repo",
			skipTest:  true, // Skip actual git config test
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipTest {
				t.Skip("Requires git config setup")
			}

			args, err := getInsteadOfDisableArgsForURL(ctx, tt.targetURL)
			assert.NoError(t, err)
			if tt.targetURL == "" {
				assert.Equal(t, 0, len(args))
			}
		})
	}
}

func TestGitCommand(t *testing.T) {
	ctx := context.Background()

	repo := &Repository{
		upstreamURL:        "https://github.com/user/repo",
		credentialProvider: nil,
	}

	cmd, err := repo.GitCommand(ctx, "version")
	assert.NoError(t, err)

	assert.NotZero(t, cmd)
	assert.True(t, len(cmd.Args) >= 2)
	// First arg should be git binary path
	assert.Equal(t, "git", cmd.Args[0])
	// Last arg should be "version"
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

func TestGitCommandWithEmptyURL(t *testing.T) {
	ctx := context.Background()

	repo := &Repository{
		upstreamURL:        "",
		credentialProvider: nil,
	}

	cmd, err := repo.GitCommand(ctx, "version")
	assert.NoError(t, err)

	assert.NotZero(t, cmd)
	assert.Equal(t, "git", cmd.Args[0])
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

type mockCredentialProvider struct {
	authorization string
	scope         string
	matched       bool
	err           error
}

func (m *mockCredentialProvider) Credential(_ context.Context, repositoryURL string) (gitcredential.Credential, bool, error) {
	scope := m.scope
	if scope == "" {
		scope = repositoryURL
	}
	return gitcredential.Credential{Authorization: m.authorization, URLScope: scope}, m.matched, m.err
}

func TestGitCommandProviderFailure(t *testing.T) {
	repo := &Repository{
		upstreamURL: "https://example.com/user/repo",
		credentialProvider: &mockCredentialProvider{
			matched: true,
			err:     errors.New("provider failed"),
		},
	}
	cmd, err := repo.GitCommand(t.Context(), "version")
	assert.Error(t, err)
	assert.Zero(t, cmd)
}

func TestGitCommandRejectsInvalidCredential(t *testing.T) {
	tests := []mockCredentialProvider{
		{authorization: "Bearer token\nInjected: value", matched: true},
		{authorization: "Bearer token", scope: "https://example.com", matched: true},
	}
	for _, provider := range tests {
		repo := &Repository{
			upstreamURL:        "https://example.com/user/repo",
			credentialProvider: &provider,
		}
		cmd, err := repo.GitCommand(t.Context(), "version")
		assert.Error(t, err)
		assert.Zero(t, cmd)
	}
}

func TestGitCommandWithCredentialProvider(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		authorization string
		expectHeader  bool
	}{
		{
			name:          "WithValidAuthorization",
			authorization: "Bearer test123456",
			expectHeader:  true,
		},
		{
			name:         "WithEmptyAuthorization",
			expectHeader: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &Repository{
				upstreamURL: "https://github.com/user/repo",
				credentialProvider: &mockCredentialProvider{
					authorization: tt.authorization,
					matched:       true,
				},
			}

			cmd, err := repo.GitCommand(ctx, "version")
			assert.NoError(t, err)
			assert.NotZero(t, cmd)

			found := false
			for i, arg := range cmd.Args {
				if arg == "-c" && i+1 < len(cmd.Args) && strings.Contains(cmd.Args[i+1], ".extraHeader=Authorization: ") {
					found = true
					assert.True(t, strings.Contains(cmd.Args[i+1], tt.authorization))
					break
				}
			}
			assert.Equal(t, tt.expectHeader, found)
		})
	}
}
