package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"math"
	"strconv"
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

			for _, arg := range cmd.Args {
				assert.False(t, strings.Contains(arg, "extraHeader"))
				if tt.authorization != "" {
					assert.False(t, strings.Contains(arg, tt.authorization))
				}
			}
			found := false
			for _, entry := range cmd.Env {
				if strings.HasPrefix(entry, "GIT_CONFIG_VALUE_") && strings.Contains(entry, tt.authorization) {
					found = true
				}
			}
			assert.Equal(t, tt.expectHeader, found)
		})
	}
}

func TestGitCommandCredentialEnvironmentConfiguresGit(t *testing.T) {
	repo := &Repository{
		upstreamURL: "https://example.com/org/repo",
		credentialProvider: &mockCredentialProvider{
			authorization: "Bearer test-token",
			matched:       true,
		},
	}
	cmd, err := repo.GitCommand(t.Context(), "config", "--get-urlmatch", "http.extraHeader", repo.upstreamURL)
	assert.NoError(t, err)
	output, err := cmd.Output()
	assert.NoError(t, err)
	assert.Equal(t, "Authorization: Bearer test-token\n", string(output))
}

func TestAppendGitConfigEnvPreservesExistingEntries(t *testing.T) {
	env, err := appendGitConfigEnv([]string{
		"PATH=/bin",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=protocol.version",
		"GIT_CONFIG_VALUE_0=2",
	}, "http.https://example.com/org/repo.extraHeader", "Authorization: Bearer secret")
	assert.NoError(t, err)
	assert.Equal(t, "2", mustEnvValue(t, env, "GIT_CONFIG_COUNT"))
	assert.Equal(t, "protocol.version", mustEnvValue(t, env, "GIT_CONFIG_KEY_0"))
	assert.Equal(t, "2", mustEnvValue(t, env, "GIT_CONFIG_VALUE_0"))
	assert.Equal(t, "http.https://example.com/org/repo.extraHeader", mustEnvValue(t, env, "GIT_CONFIG_KEY_1"))
	assert.Equal(t, "Authorization: Bearer secret", mustEnvValue(t, env, "GIT_CONFIG_VALUE_1"))
}

func TestAppendGitConfigEnvRejectsInvalidCount(t *testing.T) {
	for _, count := range []string{"invalid", "-1", strconv.Itoa(math.MaxInt)} {
		_, err := appendGitConfigEnv([]string{"GIT_CONFIG_COUNT=" + count}, "key", "value")
		assert.Error(t, err)
	}
}

func mustEnvValue(t *testing.T, env []string, key string) string {
	t.Helper()
	value, ok := envValue(env, key)
	assert.True(t, ok)
	return value
}
