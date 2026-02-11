package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
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

	cmd, err := repo.gitCommand(ctx, "version")
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

	cmd, err := repo.gitCommand(ctx, "version")
	assert.NoError(t, err)

	assert.NotZero(t, cmd)
	assert.Equal(t, "git", cmd.Args[0])
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

type mockCredentialProvider struct {
	token string
	err   error
}

func (m *mockCredentialProvider) GetTokenForURL(_ context.Context, _ string) (string, error) {
	return m.token, m.err
}

func TestGitCommandWithCredentialProvider(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		token         string
		expectHelper  bool
		expectedToken string
	}{
		{
			name:          "WithValidToken",
			token:         "ghp_test123456",
			expectHelper:  true,
			expectedToken: "ghp_test123456",
		},
		{
			name:          "WithTokenContainingSingleQuote",
			token:         "token'with'quotes",
			expectHelper:  true,
			expectedToken: "token'with'quotes",
		},
		{
			name:         "WithEmptyToken",
			token:        "",
			expectHelper: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &Repository{
				upstreamURL: "https://github.com/user/repo",
				credentialProvider: &mockCredentialProvider{
					token: tt.token,
				},
			}

			cmd, err := repo.gitCommand(ctx, "version")
			assert.NoError(t, err)
			assert.NotZero(t, cmd)

			if tt.expectHelper {
				found := false
				for i, arg := range cmd.Args {
					if arg == "-c" && i+1 < len(cmd.Args) {
						if strings.Contains(cmd.Args[i+1], "credential.helper=") {
							found = true
							assert.True(t, strings.Contains(cmd.Args[i+1], "username=x-access-token"))
							break
						}
					}
				}
				assert.True(t, found, "expected credential.helper to be configured")
			}
		})
	}
}
