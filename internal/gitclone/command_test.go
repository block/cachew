package gitclone //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
)

// testContext attaches a default slog logger so the credential refresh
// goroutine does not panic in logging.FromContext.
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

			// The token must live in the file, not in the helper string,
			// so a refresh can rotate it without restarting the subprocess.
			assert.False(t, strings.Contains(helperArg, tt.token),
				"credential.helper must not embed the token literal: %q", helperArg)

			path := credentialFilePathFromHelper(t, helperArg)
			contents, err := os.ReadFile(path)
			assert.NoError(t, err)
			assert.Equal(t,
				"username=x-access-token\npassword="+tt.token+"\n",
				string(contents))

			info, err := os.Stat(path)
			assert.NoError(t, err)
			assert.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
				"credential file must be 0600 to avoid leaking tokens to other users")
		})
	}
}

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

	cleanup() // cleanup must be idempotent
}

func TestGitCommand_RefreshGoroutineUpdatesFile(t *testing.T) {
	provider := &mockCredentialProvider{token: "ghs_initial"}
	repo := &Repository{
		upstreamURL:        "https://github.com/user/repo",
		credentialProvider: provider,
	}

	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	path, cleanup, err := repo.startTokenCredentialFile(ctx, "ghs_initial")
	assert.NoError(t, err)
	t.Cleanup(cleanup)

	provider.setToken("ghs_rotated")

	// Drive one tick synchronously rather than waiting for the 30 s ticker.
	next, changed, err := repo.refreshCredentialFileOnce(ctx, path, "ghs_initial")
	assert.NoError(t, err)
	assert.True(t, changed)
	assert.Equal(t, "ghs_rotated", next)

	contents, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t, "username=x-access-token\npassword=ghs_rotated\n", string(contents))

	// A tick with the same token must not churn the file.
	next2, changed2, err := repo.refreshCredentialFileOnce(ctx, path, "ghs_rotated")
	assert.NoError(t, err)
	assert.False(t, changed2)
	assert.Equal(t, "ghs_rotated", next2)
}

// TestGitCommand_HelperIgnoresHostileGetFile guards a real exploit: git
// appends the credential operation (get/store/erase) as a positional
// argument to `!`-prefixed helpers, so a bare `cat <credfile>` would also
// `cat get` from the worktree and let a file named `get` override our token.
// The helper must absorb that argument; this test exercises the full
// invocation path through `git credential fill` to prove it does.
func TestGitCommand_HelperIgnoresHostileGetFile(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not on PATH")
	}

	repo := &Repository{
		upstreamURL: "https://github.com/user/repo",
		credentialProvider: &mockCredentialProvider{
			token: "REAL_TOKEN",
		},
	}
	cmd, cleanup, err := repo.GitCommand(testContext(t), "version")
	assert.NoError(t, err)
	t.Cleanup(cleanup)

	helperArg := findCredentialHelperArg(cmd.Args)
	assert.NotEqual(t, "", helperArg)

	workDir := t.TempDir()
	for _, op := range []string{"get", "store", "erase"} {
		assert.NoError(t, os.WriteFile(filepath.Join(workDir, op),
			[]byte("password=EVIL_TOKEN_VIA_"+op+"\n"), 0o600))
	}

	gitCmd := exec.Command("git", "-C", workDir,
		"-c", "credential.helper=", // clear any inherited helpers
		"-c", "credential.helper="+helperArg,
		"credential", "fill",
	)
	gitCmd.Stdin = strings.NewReader("url=https://github.com/x/y\n\n")
	out, err := gitCmd.Output()
	assert.NoError(t, err)
	assert.True(t, strings.Contains(string(out), "password=REAL_TOKEN\n"),
		"expected REAL_TOKEN in helper output, got: %s", out)
	assert.False(t, strings.Contains(string(out), "EVIL_TOKEN"),
		"helper output must not include any worktree file content: %s", out)
}

// TestWriteCredentialFile_IgnoresHostileSiblingSymlink ensures that a
// pre-existing `<path>.new` symlink planted by a hostile local user can't
// redirect the rotated token write to attacker-readable storage.
func TestWriteCredentialFile_IgnoresHostileSiblingSymlink(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred")
	assert.NoError(t, writeCredentialFile(path, "ghs_initial"))

	// Plant the symlink the old `path + ".new"` code would have followed.
	sentinel := filepath.Join(dir, "attacker-readable")
	assert.NoError(t, os.WriteFile(sentinel, []byte("unchanged"), 0o600))
	assert.NoError(t, os.Symlink(sentinel, path+".new"))

	assert.NoError(t, writeCredentialFile(path, "ghs_rotated"))

	credBytes, err := os.ReadFile(path)
	assert.NoError(t, err)
	assert.Equal(t, "username=x-access-token\npassword=ghs_rotated\n", string(credBytes))

	sentinelBytes, err := os.ReadFile(sentinel)
	assert.NoError(t, err)
	assert.Equal(t, "unchanged", string(sentinelBytes),
		"writeCredentialFile must not follow a hostile <path>.new symlink")
}

// TestCleanup_WaitsForInFlightRefresh ensures cleanup blocks until the
// refresh goroutine exits so it can't race a rename and leave a stray file
// behind after the caller thinks the credentials have been wiped.
func TestCleanup_WaitsForInFlightRefresh(t *testing.T) {
	prev := credentialFileRefreshInterval
	credentialFileRefreshInterval = time.Millisecond
	t.Cleanup(func() { credentialFileRefreshInterval = prev })

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProvider := func() { releaseOnce.Do(func() { close(release) }) }
	provider := &blockingCredentialProvider{
		token:   "ghs_rotated",
		release: release,
		entered: make(chan struct{}),
	}
	repo := &Repository{
		upstreamURL:        "https://github.com/user/repo",
		credentialProvider: provider,
	}

	path, cleanup, err := repo.startTokenCredentialFile(testContext(t), "ghs_initial")
	assert.NoError(t, err)
	t.Cleanup(func() { releaseProvider(); cleanup() })

	// Wait for the production refresh goroutine to enter the blocking
	// provider, so we know cleanup will hit an in-flight tick.
	<-provider.entered

	cleanupReturned := make(chan struct{})
	go func() {
		cleanup()
		close(cleanupReturned)
	}()

	// cleanup must NOT have returned yet — the goroutine is blocked inside
	// GetTokenForURL and wg.Wait must hold cleanup until it unwinds.
	select {
	case <-cleanupReturned:
		t.Fatal("cleanup returned while a refresh tick was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	releaseProvider()
	<-cleanupReturned

	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "credential file should be removed after cleanup, got err=%v", err)
}

// blockingCredentialProvider blocks GetTokenForURL until release is closed.
// entered is closed on the first call so tests can wait for the goroutine
// to be inside the provider before exercising cancellation.
type blockingCredentialProvider struct {
	token       string
	release     chan struct{}
	entered     chan struct{}
	enteredOnce sync.Once
}

func (p *blockingCredentialProvider) GetTokenForURL(_ context.Context, _ string) (string, error) {
	p.enteredOnce.Do(func() { close(p.entered) })
	<-p.release
	return p.token, nil
}

// TestWriteCredentialFile_Atomic guards against the git credential helper
// observing a half-written file while the refresh goroutine is rotating it.
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

func findCredentialHelperArg(args []string) string {
	for i, a := range args {
		if a == "-c" && i+1 < len(args) && strings.HasPrefix(args[i+1], "credential.helper=") {
			return strings.TrimPrefix(args[i+1], "credential.helper=")
		}
	}
	return ""
}

func credentialFilePathFromHelper(t *testing.T, helper string) string {
	t.Helper()
	const prefix = `!f() { test "$1" = get && cat '`
	const suffix = `'; }; f`
	assert.True(t, strings.HasPrefix(helper, prefix), "unexpected helper format: %q", helper)
	assert.True(t, strings.HasSuffix(helper, suffix), "unexpected helper format: %q", helper)
	path := strings.TrimSuffix(strings.TrimPrefix(helper, prefix), suffix)
	path = strings.ReplaceAll(path, `'\''`, `'`)
	return path
}
