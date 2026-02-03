package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"errors"
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
		CloneDepth:       0,
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

func TestCommitError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *CommitError
		expected string
	}{
		{
			name: "not found",
			err: &CommitError{
				SHA:      "abc123",
				NotFound: true,
			},
			expected: "commit abc123 not found",
		},
		{
			name: "not fetched",
			err: &CommitError{
				SHA:        "def456",
				NotFetched: true,
			},
			expected: "commit def456 exists upstream but not fetched locally",
		},
		{
			name: "with underlying error",
			err: &CommitError{
				SHA: "xyz789",
				Err: errors.New("network timeout"),
			},
			expected: "commit xyz789: network timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestRepository_HasCommit_NotReady(t *testing.T) {
	repo := &Repository{
		state:       StateEmpty,
		path:        "/tmp/test",
		upstreamURL: "https://github.com/user/repo",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	hasCommit, err := repo.HasCommit(context.Background(), "abc123")
	assert.Error(t, err)
	assert.False(t, hasCommit)
	assert.Contains(t, err.Error(), "repository not ready")
}

func TestRepository_HasCommit_MissingCommit(t *testing.T) {
	tmpDir := t.TempDir()
	repoPath := filepath.Join(tmpDir, "test-repo")

	// Initialize a real git repository
	assert.NoError(t, os.MkdirAll(repoPath, 0o755))

	// Run git init
	cmd := exec.Command("git", "init", repoPath)
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, "git init failed: %s", string(output))

	// Configure git for the test
	cmd = exec.Command("git", "-C", repoPath, "config", "user.email", "test@example.com")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, "git config user.email failed: %s", string(output))

	cmd = exec.Command("git", "-C", repoPath, "config", "user.name", "Test User")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, "git config user.name failed: %s", string(output))

	// Create a commit so the repo has some history
	testFile := filepath.Join(repoPath, "test.txt")
	assert.NoError(t, os.WriteFile(testFile, []byte("test content"), 0o644))

	cmd = exec.Command("git", "-C", repoPath, "add", "test.txt")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, "git add failed: %s", string(output))

	cmd = exec.Command("git", "-C", repoPath, "commit", "-m", "Initial commit")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, "git commit failed: %s", string(output))

	// Create a Repository instance in StateReady
	repo := &Repository{
		state:       StateReady,
		path:        repoPath,
		upstreamURL: "https://github.com/user/repo",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	// Test with a non-existent commit SHA
	nonExistentSHA := "0000000000000000000000000000000000000000"
	hasCommit, err := repo.HasCommit(context.Background(), nonExistentSHA)
	assert.NoError(t, err)
	assert.False(t, hasCommit)
}
