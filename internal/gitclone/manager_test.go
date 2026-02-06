package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()

	config := Config{
		RootDir:          tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
		GitConfig:        DefaultGitTuningConfig(),
	}

	manager, err := NewManager(context.Background(), config)
	assert.NoError(t, err)
	assert.NotZero(t, manager)
	assert.Equal(t, tmpDir, manager.config.RootDir)
}

func TestNewManager_RequiresRootDir(t *testing.T) {
	config := Config{
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	_, err := NewManager(context.Background(), config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "RootDir is required")
}

func TestManager_GetOrCreate(t *testing.T) {
	tmpDir := t.TempDir()
	config := Config{
		RootDir:          tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
		GitConfig:        DefaultGitTuningConfig(),
	}

	manager, err := NewManager(context.Background(), config)
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
	tmpDir := t.TempDir()
	config := Config{
		RootDir:          tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
		GitConfig:        DefaultGitTuningConfig(),
	}

	manager, err := NewManager(context.Background(), config)
	assert.NoError(t, err)

	repoPath := filepath.Join(tmpDir, "github.com", "user", "repo")
	gitDir := filepath.Join(repoPath, ".git")
	assert.NoError(t, os.MkdirAll(gitDir, 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))

	upstreamURL := "https://github.com/user/repo"
	repo, err := manager.GetOrCreate(context.Background(), upstreamURL)
	assert.NoError(t, err)
	assert.NotZero(t, repo)

	assert.Equal(t, StateReady, repo.State())
}

func TestManager_Get(t *testing.T) {
	tmpDir := t.TempDir()
	config := Config{
		RootDir:          tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
		GitConfig:        DefaultGitTuningConfig(),
	}

	manager, err := NewManager(context.Background(), config)
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
	tmpDir := t.TempDir()
	config := Config{
		RootDir:          tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
		GitConfig:        DefaultGitTuningConfig(),
	}

	manager, err := NewManager(context.Background(), config)
	assert.NoError(t, err)

	repos := []string{
		filepath.Join(tmpDir, "github.com", "user1", "repo1"),
		filepath.Join(tmpDir, "github.com", "user2", "repo2"),
		filepath.Join(tmpDir, "gitlab.com", "org", "project"),
	}

	for _, repoPath := range repos {
		gitDir := filepath.Join(repoPath, ".git")
		assert.NoError(t, os.MkdirAll(gitDir, 0o755))
		assert.NoError(t, os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))
	}

	err = manager.DiscoverExisting(context.Background())
	assert.NoError(t, err)

	repo1 := manager.Get("https://github.com/user1/repo1")
	assert.NotZero(t, repo1)
	assert.Equal(t, StateReady, repo1.State())

	repo2 := manager.Get("https://github.com/user2/repo2")
	assert.NotZero(t, repo2)
	assert.Equal(t, StateReady, repo2.State())

	repo3 := manager.Get("https://gitlab.com/org/project")
	assert.NotZero(t, repo3)
	assert.Equal(t, StateReady, repo3.State())
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
