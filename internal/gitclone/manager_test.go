package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"fmt"
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

// TestMain drops the GIT_* variables git exports under hooks so test git
// commands target their temp dirs, not the ambient repository.
func TestMain(m *testing.M) {
	for _, v := range []string{
		"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
		"GIT_COMMON_DIR", "GIT_PREFIX", "GIT_NAMESPACE",
	} {
		_ = os.Unsetenv(v)
	}
	os.Exit(m.Run())
}

// testRepoConfig returns a Config with timeouts populated, suitable for
// constructing Repository values directly in tests that bypass NewManager.
func testRepoConfig() Config {
	return Config{
		CloneTimeout:    1 * time.Hour,
		FetchTimeout:    5 * time.Minute,
		LsRemoteTimeout: 60 * time.Second,
		RepackTimeout:   10 * time.Minute,
	}
}

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
		for _, kv := range mirrorConfigSettings(manager.Config().PackThreads) {
			cmd := exec.Command("git", "-C", repoPath, "config", "--get", kv[0])
			output, err := cmd.Output()
			assert.NoError(t, err, "config key %s in %s", kv[0], repoPath)
			assert.Equal(t, kv[1], strings.TrimSpace(string(output)), "config key %s in %s", kv[0], repoPath)
		}
	}
}

func TestManager_DiscoverExisting_SkipsAndRemovesLeftoverCloneTempDirs(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	config := Config{
		MirrorRoot:       tmpDir,
		FetchInterval:    15 * time.Minute,
		RefCheckInterval: 10 * time.Second,
	}

	manager, err := NewManager(ctx, config, nil)
	assert.NoError(t, err)

	upstreamPath := createBareRepo(t, t.TempDir())

	realRepo := filepath.Join(tmpDir, "github.com", "user1", "repo1")
	assert.NoError(t, os.MkdirAll(filepath.Dir(realRepo), 0o755))
	assert.NoError(t, exec.Command("git", "clone", "--bare", upstreamPath, realRepo).Run())

	// Simulate an interrupted clone: a leftover .clone-* temp dir containing a
	// mirror clone at its "repo" subdirectory, alongside the real repo.
	leftover := filepath.Join(tmpDir, "github.com", "user1", cloneTempPrefix+"123456")
	assert.NoError(t, os.MkdirAll(leftover, 0o755))
	assert.NoError(t, exec.Command("git", "clone", "--bare", upstreamPath, filepath.Join(leftover, "repo")).Run())

	discovered, err := manager.DiscoverExisting(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(discovered))
	assert.Equal(t, "https://github.com/user1/repo1", discovered[0].UpstreamURL())

	_, statErr := os.Stat(leftover)
	assert.True(t, os.IsNotExist(statErr), "leftover clone temp dir should be removed")
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

func TestRepository_FetchVerifiedDoesNotCoalesce(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "clone")
	repo := &Repository{
		state:       StateEmpty,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}
	assert.NoError(t, repo.Clone(ctx))

	// Advance upstream so the mirror is behind.
	workPath := filepath.Join(tmpDir, "work")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("y"), 0o644))
	for _, args := range [][]string{
		{"git", "-C", workPath, "commit", "-am", "update"},
		{"git", "-C", workPath, "push", upstreamPath, "HEAD"},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}
	newSHAOut, err := exec.Command("git", "-C", workPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	newSHA := strings.TrimSpace(string(newSHAOut))

	holdSem := func(t *testing.T, fetch func() error) error {
		t.Helper()
		release := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				<-release
				return nil
			})
		}()
		time.Sleep(20 * time.Millisecond)
		fetchDone := make(chan error, 1)
		go func() { fetchDone <- fetch() }()
		time.Sleep(50 * time.Millisecond)
		close(release)
		assert.NoError(t, <-holderDone)
		return <-fetchDone
	}

	// Fetch coalesces with the semaphore holder even though the holder was
	// not fetching, so the mirror stays behind.
	assert.NoError(t, holdSem(t, func() error { return repo.Fetch(ctx) }))
	assert.False(t, repo.HasCommit(ctx, newSHA))

	// FetchVerified waits for the holder and then runs its own fetch.
	assert.NoError(t, holdSem(t, func() error { return repo.FetchVerified(ctx) }))
	assert.True(t, repo.HasCommit(ctx, newSHA))
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
		config:      testRepoConfig(),
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
	cfg := testRepoConfig()
	cfg.PackThreads = 4
	repo := &Repository{
		state:       StateEmpty,
		config:      cfg,
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	assert.NoError(t, repo.Clone(ctx))
	assert.Equal(t, StateReady, repo.State())

	for _, kv := range mirrorConfigSettings(4) {
		cmd := exec.Command("git", "-C", clonePath, "config", "--get", kv[0])
		output, err := cmd.Output()
		assert.NoError(t, err, "config key %s", kv[0])
		assert.Equal(t, kv[1], strings.TrimSpace(string(output)), "config key %s", kv[0])
	}
}

func TestRepository_CloneFailedLeavesNoDebris(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	clonePath := filepath.Join(tmpDir, "mirrors", "github.com", "owner", "repo")
	repo := &Repository{
		state:       StateEmpty,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: "https://github.com/nonexistent-owner-abc123/nonexistent-repo-abc123",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	err := repo.Clone(ctx)
	assert.Error(t, err)
	assert.Equal(t, StateEmpty, repo.State())

	_, statErr := os.Stat(clonePath)
	assert.True(t, os.IsNotExist(statErr), "repo.Path() should not exist after failed clone")
}

func TestRepository_CloneDoesNotClobberSiblings(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	siblingPath := filepath.Join(mirrorRoot, "github.com", "owner", "sibling")
	assert.NoError(t, os.MkdirAll(siblingPath, 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(siblingPath, "HEAD"), []byte("ref: refs/heads/main\n"), 0o644))

	clonePath := filepath.Join(mirrorRoot, "github.com")
	repo := &Repository{
		state:       StateEmpty,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: "https://github.com/",
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	err := repo.Clone(ctx)
	assert.Error(t, err)

	_, statErr := os.Stat(siblingPath)
	assert.NoError(t, statErr, "sibling mirror should still exist after failed clone")
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
		config:      testRepoConfig(),
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

func TestRepository_RepackFull(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "mirror")
	cmd := exec.Command("git", "clone", "--mirror", upstreamPath, clonePath)
	assert.NoError(t, cmd.Run())

	repo := &Repository{
		state:       StateReady,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	// Unset full-repack-timeout exercises the fallback to repack-timeout.
	assert.NoError(t, repo.RepackFull(ctx))

	packs, err := filepath.Glob(filepath.Join(clonePath, "objects", "pack", "*.pack"))
	assert.NoError(t, err)
	assert.True(t, len(packs) > 0, "expected at least one pack file after full repack")

	_, err = os.Stat(filepath.Join(clonePath, "objects", "pack", "multi-pack-index"))
	assert.NoError(t, err)
}

func TestRepository_Repack_CleansUpStaleLockOnFailure(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "mirror")
	cmd := exec.Command("git", "clone", "--mirror", upstreamPath, clonePath)
	assert.NoError(t, cmd.Run())

	repo := &Repository{
		state:       StateReady,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	// Place a stale lock file simulating a killed repack.
	lockPath := filepath.Join(clonePath, "objects", "pack", "multi-pack-index.lock")
	assert.NoError(t, os.WriteFile(lockPath, []byte("stale"), 0o644))

	// Repack should fail because of the lock, but should clean it up.
	err := repo.Repack(ctx)
	assert.Error(t, err)

	// The stale lock should have been removed.
	_, statErr := os.Stat(lockPath)
	assert.True(t, os.IsNotExist(statErr), "expected multi-pack-index.lock to be removed after failed repack")
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

func TestRepository_EnsureRefs(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	upstreamPath := createBareRepo(t, tmpDir)

	clonePath := filepath.Join(tmpDir, "clone")
	repo := &Repository{
		state:       StateEmpty,
		config:      testRepoConfig(),
		path:        clonePath,
		upstreamURL: upstreamPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}
	assert.NoError(t, repo.Clone(ctx))

	local, err := repo.GetLocalRefs(ctx)
	assert.NoError(t, err)
	mainSHA, ok := local["refs/heads/main"]
	if !ok {
		mainSHA = local["refs/heads/master"]
	}
	assert.NotEqual(t, "", mainSHA)

	head := "refs/heads/main"
	if !ok {
		head = "refs/heads/master"
	}

	// Mirror already satisfies the request → no fetch.
	resolved, missing, fetched, err := repo.EnsureRefs(ctx, map[string]string{head: mainSHA}, nil)
	assert.NoError(t, err)
	assert.False(t, fetched)
	assert.Equal(t, 0, len(missing))
	assert.Equal(t, mainSHA, resolved[head])

	// Add a new commit to upstream so the mirror is now behind.
	workPath := filepath.Join(tmpDir, "work")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("y"), 0o644))
	for _, args := range [][]string{
		{"git", "-C", workPath, "commit", "-am", "update"},
		{"git", "-C", workPath, "push", upstreamPath, "HEAD:" + strings.TrimPrefix(head, "refs/heads/")},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}

	newSHAOut, err := exec.Command("git", "-C", workPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	newSHA := strings.TrimSpace(string(newSHAOut))
	assert.NotEqual(t, mainSHA, newSHA)

	// Asking for the new SHA triggers a fetch and the mirror catches up.
	resolved, missing, fetched, err = repo.EnsureRefs(ctx, map[string]string{head: newSHA}, nil)
	assert.NoError(t, err)
	assert.True(t, fetched)
	assert.Equal(t, 0, len(missing))
	assert.Equal(t, newSHA, resolved[head])

	// Empty SHA means "any": already satisfied without fetching.
	resolved, _, fetched, err = repo.EnsureRefs(ctx, map[string]string{head: ""}, nil)
	assert.NoError(t, err)
	assert.False(t, fetched)
	assert.Equal(t, newSHA, resolved[head])

	// Missing ref: fetch runs but ref remains missing → empty resolved SHA.
	resolved, _, fetched, err = repo.EnsureRefs(ctx, map[string]string{"refs/heads/does-not-exist": ""}, nil)
	assert.NoError(t, err)
	assert.True(t, fetched)
	assert.Equal(t, "", resolved["refs/heads/does-not-exist"])

	// Commit-only request that's already present → no fetch.
	resolved, missing, fetched, err = repo.EnsureRefs(ctx, nil, []string{newSHA})
	assert.NoError(t, err)
	assert.False(t, fetched)
	assert.Equal(t, 0, len(missing))
	assert.Equal(t, 0, len(resolved))

	// Commit-only request that's missing → fetch runs and commit is reported missing.
	resolved, missing, fetched, err = repo.EnsureRefs(ctx, nil,
		[]string{"0000000000000000000000000000000000000000"})
	assert.NoError(t, err)
	assert.True(t, fetched)
	assert.Equal(t, []string{"0000000000000000000000000000000000000000"}, missing)
	assert.Equal(t, 0, len(resolved))

	// Mixed request, both already satisfied.
	resolved, missing, fetched, err = repo.EnsureRefs(ctx,
		map[string]string{head: newSHA}, []string{newSHA})
	assert.NoError(t, err)
	assert.False(t, fetched)
	assert.Equal(t, 0, len(missing))
	assert.Equal(t, newSHA, resolved[head])
}

// TestMirrorConfigAllowsUnreachableSHA verifies that the mirror config lets
// git upload-pack serve objects that are present in the ODB but unreachable
// from any ref (e.g. after a force-push orphans a commit).
func TestMirrorConfigAllowsUnreachableSHA(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "work")
	upstreamDir := filepath.Join(tmpDir, "upstream.git")
	mirrorDir := filepath.Join(tmpDir, "mirror.git")

	// Create upstream repo with an initial commit.
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out)
		}
	}

	run("git", "init", "--bare", upstreamDir)
	run("git", "clone", upstreamDir, workDir)
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, "f.txt"), []byte("v1"), 0o644))
	run("git", "-C", workDir, "add", ".")
	run("git", "-C", workDir, "commit", "-m", "initial")
	run("git", "-C", workDir, "push", "origin", "HEAD:main")

	// Record the SHA that will become orphaned.
	out, err := exec.Command("git", "-C", workDir, "rev-parse", "HEAD").CombinedOutput()
	assert.NoError(t, err)
	orphanedSHA := strings.TrimSpace(string(out))

	// Mirror-clone and apply the cachew mirror config.
	run("git", "clone", "--mirror", upstreamDir, mirrorDir)
	assert.NoError(t, configureMirror(context.Background(), mirrorDir, 1))

	// Force-push a new root commit to upstream, then fetch into mirror.
	run("git", "-C", workDir, "checkout", "--orphan", "newroot")
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, "f.txt"), []byte("v2"), 0o644))
	run("git", "-C", workDir, "add", ".")
	run("git", "-C", workDir, "commit", "-m", "replacement")
	run("git", "-C", workDir, "push", "--force", "origin", "newroot:main")
	run("git", "-C", mirrorDir, "fetch", "--prune")

	// Sanity: orphaned SHA exists in ODB but is unreachable.
	assert.NoError(t, exec.Command("git", "-C", mirrorDir, "cat-file", "-e", orphanedSHA).Run())
	branchOut, _ := exec.Command("git", "-C", mirrorDir, "branch", "--contains", orphanedSHA).CombinedOutput()
	assert.Equal(t, "", strings.TrimSpace(string(branchOut)))

	// The key assertion: upload-pack should accept the unreachable SHA.
	// With allowReachableSHA1InWant this fails; with allowAnySHA1InWant it passes.
	cmd := exec.Command("git", "-C", mirrorDir, "upload-pack", "--strict", ".")
	cmd.Stdin = strings.NewReader(
		fmt.Sprintf("0032want %s\n00000009done\n", orphanedSHA),
	)
	uploadOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("upload-pack rejected unreachable SHA (mirror config should allow it):\n%s", uploadOut)
	}
}
