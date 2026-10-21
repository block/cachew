package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"encoding/base64"
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
	token string
	err   error
}

func (m *mockCredentialProvider) GetTokenForURL(_ context.Context, _ string) (string, error) {
	return m.token, m.err
}

func TestGitCommandWithCredentialProvider(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name             string
		token            string
		expectCredential bool
	}{
		{
			name:             "WithValidToken",
			token:            "ghp_test123456",
			expectCredential: true,
		},
		{
			name:             "WithTokenContainingSingleQuote",
			token:            "token'with'quotes",
			expectCredential: true,
		},
		{
			name:             "WithEmptyToken",
			token:            "",
			expectCredential: false,
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

			cmd, err := repo.GitCommand(ctx, "version")
			assert.NoError(t, err)
			assert.NotZero(t, cmd)

			for _, arg := range cmd.Args {
				assert.False(t, strings.Contains(arg, "extraHeader"))
				if tt.token != "" {
					assert.False(t, strings.Contains(arg, tt.token))
				}
			}
			found := false
			for _, entry := range cmd.Env {
				if strings.HasPrefix(entry, "GIT_CONFIG_KEY_") && strings.HasSuffix(entry, ".extraHeader") {
					found = true
				}
			}
			assert.Equal(t, tt.expectCredential, found)
		})
	}
}

func TestGitCommandCredentialEnvironmentConfiguresGit(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "protocol.version")
	t.Setenv("GIT_CONFIG_VALUE_0", "2")
	repo := &Repository{
		upstreamURL:        "https://github.com/org/repo",
		credentialProvider: &mockCredentialProvider{token: "test-token"},
	}
	cmd, err := repo.GitCommand(t.Context(), "config", "--get-urlmatch", "http.extraHeader", repo.upstreamURL)
	assert.NoError(t, err)
	output, err := cmd.Output()
	assert.NoError(t, err)
	basicToken := base64.StdEncoding.EncodeToString([]byte("x-access-token:test-token"))
	assert.Equal(t, "Authorization: Basic "+basicToken+"\n", string(output))
}

func TestAppendGitConfigEnvUsesNextIndex(t *testing.T) {
	env := appendGitConfigEnv([]string{
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=protocol.version",
		"GIT_CONFIG_VALUE_0=2",
		"GIT_CONFIG_KEY_1=http.version",
		"GIT_CONFIG_VALUE_1=HTTP/2",
	}, "http.https://github.com/org/repo.extraHeader", "Authorization: Basic secret")
	countEntries := 0
	for _, entry := range env {
		if strings.HasPrefix(entry, "GIT_CONFIG_COUNT=") {
			countEntries++
		}
	}
	assert.Equal(t, 1, countEntries)
	assert.Equal(t, "GIT_CONFIG_COUNT=3", env[len(env)-3])
	assert.Equal(t, "GIT_CONFIG_KEY_2=http.https://github.com/org/repo.extraHeader", env[len(env)-2])
	assert.Equal(t, "GIT_CONFIG_VALUE_2=Authorization: Basic secret", env[len(env)-1])
}
