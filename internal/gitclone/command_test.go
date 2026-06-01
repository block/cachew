package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
)

// testContext returns a context wired with a default slog logger so code paths
// that call logging.FromContext (notably the credential refresh goroutine) do
// not panic in tests.
func testContext(t *testing.T) context.Context {
	t.Helper()
	return logging.ContextWithLogger(context.Background(), slog.Default())
}

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

	cmd, cleanup, err := repo.GitCommand(ctx, "version")
	assert.NoError(t, err)
	t.Cleanup(cleanup)

	assert.NotZero(t, cmd)
	assert.True(t, len(cmd.Args) >= 2)
	assert.Equal(t, "git", cmd.Args[0])
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

func TestGitCommandWithEmptyURL(t *testing.T) {
	ctx := context.Background()

	repo := &Repository{
		upstreamURL:        "",
		credentialProvider: nil,
	}

	cmd, cleanup, err := repo.GitCommand(ctx, "version")
	assert.NoError(t, err)
	t.Cleanup(cleanup)

	assert.NotZero(t, cmd)
	assert.Equal(t, "git", cmd.Args[0])
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

type mockCredentialProvider struct {
	mu    sync.Mutex
	token string
	calls atomic.Int64
	err   error
}

func (m *mockCredentialProvider) GetTokenForURL(_ context.Context, _ string) (string, error) {
	m.calls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.token, m.err
}

func (m *mockCredentialProvider) setToken(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.token = token
}

func TestGitCommandWithCredentialProvider(t *testing.T) {
	ctx := testContext(t)

	tests := []struct {
		name         string
		token        string
		expectHelper bool
	}{
		{
			name:         "WithValidToken",
			token:        "ghp_test123456",
			expectHelper: true,
		},
		{
			name:         "WithTokenContainingSingleQuote",
			token:        "token'with'quotes",
			expectHelper: true,
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

			cmd, cleanup, err := repo.GitCommand(ctx, "version")
			assert.NoError(t, err)
			assert.NotZero(t, cmd)
			t.Cleanup(cleanup)

			helperArg := findCredentialHelperArg(cmd.Args)
			if !tt.expectHelper {
				assert.Equal(t, "", helperArg, "did not expect credential.helper")
				return
			}
			assert.NotEqual(t, "", helperArg, "expected credential.helper to be configured")

			// The helper must NOT contain the literal token (it must point at a
			// file instead) — that is the whole point of the refresh fix.
			assert.False(t, strings.Contains(helperArg, tt.token),
				"credential.helper must not embed the token literal: %q", helperArg)

			path := credentialFilePathFromHelper(t, helperArg)
			contents, err := os.ReadFile(path)
			assert.NoError(t, err)
			assert.Equal(t,
				"username=x-access-token\npassword="+tt.token+"\n",
				string(contents),
				"credential file should contain a complete git credential helper response")

			info, err := os.Stat(path)
			assert.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
				"credential file must be 0600 to avoid leaking tokens to other users")
		})
	}
}

// TestGitCommand_CleanupRemovesCredentialFile verifies that calling the
// returned cleanup function removes the on-disk credential file so we do not
// leak rotated tokens between commands.
func TestGitCommand_CleanupRemovesCredentialFile(t *testing.T) {
	repo := &Repository{
		upstreamURL: "https://github.com/user/repo",
		credentialProvider: &mockCredentialProvider{
			token: "ghs_initial",
		},
	}

	cmd, cleanup, err := repo.GitCommand(testContext(t), "version")
	assert.NoError(t, err)
	assert.NotZero(t, cmd)

	helperArg := findCredentialHelperArg(cmd.Args)
	path := credentialFilePathFromHelper(t, helperArg)
	_, err = os.Stat(path)
	assert.NoError(t, err, "credential file should exist before cleanup")

	cleanup()
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "credential file should be removed by cleanup, got err=%v", err)

	// Idempotency: calling cleanup twice must not panic or error.
	cleanup()
}

// TestGitCommand_RefreshGoroutineUpdatesFile verifies that the background
// refresh goroutine rewrites the credential file when the upstream token
// rotates. Without this behavior a long-running `git lfs fetch` (which the
// snapshot job spawns) would keep using a stale 1-hour token after the
// TokenManager rotates it and fail with "Bad credentials" — the exact
// production incident this change is fixing.
func TestGitCommand_RefreshGoroutineUpdatesFile(t *testing.T) {
	provider := &mockCredentialProvider{token: "ghs_initial"}
	repo := &Repository{
		upstreamURL:        "https://github.com/user/repo",
		credentialProvider: provider,
	}

	// Tighten the refresh interval for the test by using
	// startTokenCredentialFile directly instead of GitCommand, so we do not
	// have to wait 30 seconds for the goroutine to tick.
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	path, cleanup, err := repo.startTokenCredentialFile(ctx, "ghs_initial")
	assert.NoError(t, err)
	t.Cleanup(cleanup)

	provider.setToken("ghs_rotated")

	// Drive the refresh loop manually for deterministic test timing rather
	// than waiting for the production 30s ticker.
	next, changed, err := repo.refreshCredentialFileOnce(ctx, path, "ghs_initial")
	assert.NoError(t, err)
	assert.True(t, changed, "refresh should detect the rotated token and rewrite the file")
	assert.Equal(t, "ghs_rotated", next)

	contents, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t,
		"username=x-access-token\npassword=ghs_rotated\n",
		string(contents),
		"credential file should reflect the rotated token")

	// A subsequent tick with the same token must be a no-op so the goroutine
	// does not churn the file on every cycle.
	next2, changed2, err := repo.refreshCredentialFileOnce(ctx, path, "ghs_rotated")
	assert.NoError(t, err)
	assert.False(t, changed2)
	assert.Equal(t, "ghs_rotated", next2)
}

// TestWriteCredentialFile_Atomic verifies the rename-based atomic-write
// behavior: a reader concurrent with the rewrite should always see a complete
// credential response, never a half-written file. This protects the git
// credential helper from observing a partial token while the refresh
// goroutine is updating the file.
func TestWriteCredentialFile_Atomic(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "cred-*")
	assert.NoError(t, err)
	path := f.Name()
	_ = f.Close()

	assert.NoError(t, writeCredentialFile(path, "ghs_one"))

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				b, err := os.ReadFile(path)
				if err != nil {
					continue
				}
				s := string(b)
				assert.True(t,
					strings.HasPrefix(s, "username=x-access-token\npassword=") && strings.HasSuffix(s, "\n"),
					"reader observed partial write: %q", s)
			}
		}
	}()

	for range 200 {
		assert.NoError(t, writeCredentialFile(path, "ghs_rotated"))
	}
	close(stop)
}

func TestShellSingleQuote(t *testing.T) {
	tests := []struct {
		in, out string
	}{
		{"/tmp/foo", `'/tmp/foo'`},
		{"/tmp/with space", `'/tmp/with space'`},
		{"weird'name", `'weird'\''name'`},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.out, shellSingleQuote(tt.in))
	}
}

// findCredentialHelperArg returns the value portion of the
// `credential.helper=...` entry in a git command's argv, or empty string if
// none is present.
func findCredentialHelperArg(args []string) string {
	for i, a := range args {
		if a == "-c" && i+1 < len(args) && strings.HasPrefix(args[i+1], "credential.helper=") {
			return strings.TrimPrefix(args[i+1], "credential.helper=")
		}
	}
	return ""
}

// credentialFilePathFromHelper extracts the filesystem path from a
// `!cat '...'` credential helper expression and fails the test if it cannot.
func credentialFilePathFromHelper(t *testing.T, helper string) string {
	t.Helper()
	const prefix = "!cat '"
	const suffix = "'"
	assert.True(t, strings.HasPrefix(helper, prefix), "unexpected helper format: %q", helper)
	assert.True(t, strings.HasSuffix(helper, suffix), "unexpected helper format: %q", helper)
	path := strings.TrimSuffix(strings.TrimPrefix(helper, prefix), suffix)
	path = strings.ReplaceAll(path, `'\''`, `'`)
	return path
}
