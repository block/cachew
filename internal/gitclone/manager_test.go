package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
)

// createBareRepo creates a bare git repository at the given path, suitable for
// use as an upstream or as a mirror clone target.
func createBareRepo(t *testing.T, dir string) string {
	t.Helper()
	workPath := filepath.Join(dir, "work")
	barePath := filepath.Join(dir, "upstream.git")
	assert.NoError(t, os.MkdirAll(workPath, 0o755))

	for _, args := range [][]string{
		{"git", "-C", workPath, "init"},
		{"git", "-C", workPath, "config", "user.email", "test@example.com"},
		{"git", "-C", workPath, "config", "user.name", "Test"},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("x"), 0o644))
	for _, args := range [][]string{
		{"git", "-C", workPath, "add", "."},
		{"git", "-C", workPath, "commit", "-m", "init"},
		{"git", "clone", "--bare", workPath, barePath},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}
	return barePath
}

func TestNewManager(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()

	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)
	assert.NotZero(t, manager)
	assert.Equal(t, tmpDir, manager.config.MirrorRoot)
}

func TestNewManager_RequiresRootDir(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	config := Config{
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	_, err := NewManager(ctx, config, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mirror-root is required")
}

func TestManager_GetOrCreate(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)

	upstreamURL := "https://github.com/user/repo"
	repo, err := manager.GetOrCreate(context.Background(), upstreamURL)
	assert.NoError(t, err)
	assert.NotZero(t, repo)

	assert.Equal(t, upstreamURL, repo.UpstreamURL())
	assert.Equal(t, StateEmpty, repo.State())
	assert.Equal(t, filepath.Join(tmpDir, "github.com", "user", "repo"), repo.Path())

	repo2, err := manager.GetOrCreate(context.Background(), upstreamURL)
	assert.NoError(t, err)
	assert.True(t, repo == repo2, "expected same repository instance")
}

func TestManager_GetOrCreate_ExistingClone(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)

	repoPath := filepath.Join(tmpDir, "github.com", "user", "repo")
	assert.NoError(t, os.MkdirAll(repoPath, 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(repoPath, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))

	upstreamURL := "https://github.com/user/repo"
	repo, err := manager.GetOrCreate(context.Background(), upstreamURL)
	assert.NoError(t, err)
	assert.NotZero(t, repo)

	assert.Equal(t, StateReady, repo.State())
}

func TestManager_Get(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)

	upstreamURL := "https://github.com/user/repo"

	repo := manager.Get(upstreamURL)
	assert.Zero(t, repo)

	_, err = manager.GetOrCreate(context.Background(), upstreamURL)
	assert.NoError(t, err)

	repo = manager.Get(upstreamURL)
	assert.NotZero(t, repo)
	assert.Equal(t, upstreamURL, repo.UpstreamURL())
}

func TestManager_DiscoverExisting(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)

	// Create a real bare repo as a source, then clone it into the mirror paths.
	upstreamPath := createBareRepo(t, t.TempDir())

	repoPaths := []string{
		filepath.Join(tmpDir, "github.com", "user1", "repo1"),
		filepath.Join(tmpDir, "github.com", "user2", "repo2"),
		filepath.Join(tmpDir, "gitlab.com", "org", "project"),
	}
	for _, repoPath := range repoPaths {
		assert.NoError(t, os.MkdirAll(filepath.Dir(repoPath), 0o755))
		cmd := exec.Command("git", "clone", "--bare", upstreamPath, repoPath)
		assert.NoError(t, cmd.Run())
	}

	discovered, err := manager.DiscoverExisting(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, len(discovered))

	repo1 := manager.Get("https://github.com/user1/repo1")
	assert.NotZero(t, repo1)
	assert.Equal(t, StateReady, repo1.State())

	repo2 := manager.Get("https://github.com/user2/repo2")
	assert.NotZero(t, repo2)
	assert.Equal(t, StateReady, repo2.State())

	repo3 := manager.Get("https://gitlab.com/org/project")
	assert.NotZero(t, repo3)
	assert.Equal(t, StateReady, repo3.State())

	// Verify mirror config was applied to discovered repos.
	for _, repoPath := range repoPaths {
		for _, kv := range mirrorConfigSettings() {
			cmd := exec.Command("git", "-C", repoPath, "config", "--get", kv[0])
			output, err := cmd.Output()
			assert.NoError(t, err, "config key %s in %s", kv[0], repoPath)
			assert.Equal(t, kv[1], strings.TrimSpace(string(output)), "config key %s in %s", kv[0], repoPath)
		}
	}
}

func TestRepository_StateTransitions(t *testing.T) {
	repo := &Repository{
		state:       StateEmpty,
		path:        "/tmp/test",
		upstreamURL: "https://github.com/user/repo",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.Equal(t, StateEmpty, repo.State())

	repo.mu.Lock()
	repo.state = StateCloning
	repo.mu.Unlock()
	assert.Equal(t, StateCloning, repo.State())

	repo.mu.Lock()
	repo.state = StateReady
	repo.mu.Unlock()
	assert.Equal(t, StateReady, repo.State())
}

func TestRepository_NeedsFetch(t *testing.T) {
	repo := &Repository{
		state:     StateEmpty,
		lastFetch: time.Now().Add(-20 * time.Minute),
		fetchSem:  make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.True(t, repo.NeedsFetch(15*time.Minute))

	assert.False(t, repo.NeedsFetch(30*time.Minute))

	repo.mu.Lock()
	repo.lastFetch = time.Now()
	repo.mu.Unlock()

	assert.False(t, repo.NeedsFetch(15*time.Minute))
}

func TestParseGitRefs(t *testing.T) {
	output := []byte(`
abc123 refs/heads/main
def456 refs/heads/develop
789012 refs/tags/v1.0.0
	`)

	refs := ParseGitRefs(output)

	assert.Equal(t, "abc123", refs["refs/heads/main"])
	assert.Equal(t, "def456", refs["refs/heads/develop"])
	assert.Equal(t, "789012", refs["refs/tags/v1.0.0"])
}

func TestState_String(t *testing.T) {
	assert.Equal(t, "empty", StateEmpty.String())
	assert.Equal(t, "cloning", StateCloning.String())
	assert.Equal(t, "ready", StateReady.String())
}

func TestRepository_Clone_StateVisibleDuringClone(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "clone")
	repo := &Repository{
		state:       StateEmpty,
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	// Start clone in background
	cloneDone := make(chan error, 1)
	go func() {
		cloneDone <- repo.Clone(ctx)
	}()

	// Poll until we observe StateCloning (should not block)
	deadline := time.After(10 * time.Second)
	sawCloning := false
	for !sawCloning {
		select {
		case <-deadline:
			t.Fatal("timed out waiting to observe StateCloning — State() likely blocked on the clone lock")
		default:
			if repo.State() == StateCloning {
				sawCloning = true
			}
		}
	}

	assert.True(t, sawCloning)

	// Wait for clone to finish
	assert.NoError(t, <-cloneDone)
	assert.Equal(t, StateReady, repo.State())
}

func TestRepository_CloneSetsMirrorConfig(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "clone")
	repo := &Repository{
		state:       StateEmpty,
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.NoError(t, repo.Clone(ctx))
	assert.Equal(t, StateReady, repo.State())

	for _, kv := range mirrorConfigSettings() {
		cmd := exec.Command("git", "-C", clonePath, "config", "--get", kv[0])
		output, err := cmd.Output()
		assert.NoError(t, err, "config key %s", kv[0])
		assert.Equal(t, kv[1], strings.TrimSpace(string(output)), "config key %s", kv[0])
	}
}

func TestRepository_Repack(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "mirror")
	cmd := exec.Command("git", "clone", "--mirror", upstreamPath, clonePath)
	assert.NoError(t, cmd.Run())

	repo := &Repository{
		state:       StateReady,
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.NoError(t, repo.Repack(ctx))

	// Verify a pack file exists after repack.
	packs, err := filepath.Glob(filepath.Join(clonePath, "objects", "pack", "*.pack"))
	assert.NoError(t, err)
	assert.True(t, len(packs) > 0, "expected at least one pack file after repack")

	// Verify multi-pack-index was written.
	_, err = os.Stat(filepath.Join(clonePath, "objects", "pack", "multi-pack-index"))
	assert.NoError(t, err)
}

func TestRepository_HasCommit(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	repoPath := filepath.Join(tmpDir, "test-repo")

	assert.NoError(t, os.MkdirAll(repoPath, 0o755))

	cmd := exec.Command("git", "-C", repoPath, "init")
	assert.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", repoPath, "config", "user.email", "test@example.com")
	assert.NoError(t, cmd.Run())
	cmd = exec.Command("git", "-C", repoPath, "config", "user.name", "Test User")
	assert.NoError(t, cmd.Run())

	testFile := filepath.Join(repoPath, "test.txt")
	assert.NoError(t, os.WriteFile(testFile, []byte("test content"), 0o644))
	cmd = exec.Command("git", "-C", repoPath, "add", "test.txt")
	assert.NoError(t, cmd.Run())
	cmd = exec.Command("git", "-C", repoPath, "commit", "-m", "Initial commit")
	assert.NoError(t, cmd.Run())

	cmd = exec.Command("git", "-C", repoPath, "tag", "v1.0.0")
	assert.NoError(t, cmd.Run())

	repo := &Repository{
		state:       StateReady,
		path:        repoPath,
		upstreamURL: "https://example.com/test-repo",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.True(t, repo.HasCommit(ctx, "HEAD"))
	assert.True(t, repo.HasCommit(ctx, "v1.0.0"))

	assert.False(t, repo.HasCommit(ctx, "nonexistent"))
	assert.False(t, repo.HasCommit(ctx, "v9.9.9"))
}
