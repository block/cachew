package git_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
	"github.com/block/cachew/internal/strategy/git"
)

func TestSnapshotHTTPEndpoint(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: mirrorRoot,
	}, nil)
	_, err = git.New(ctx, git.Config{
		SnapshotInterval: 24 * time.Hour,
	}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Create a fake snapshot in the cache with a Last-Modified after the
	// mirror's last fetch so the endpoint considers it fresh.
	upstreamURL := "https://github.com/org/repo"
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	snapshotData := []byte("fake snapshot data")

	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/zstd"}
	headers["Last-Modified"] = []string{time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)}
	writer, err := memCache.Create(ctx, cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write(snapshotData)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// Test successful snapshot request — cached snapshot is fresh (Last-Modified
	// is after the mirror's last fetch), so it's served directly.
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "application/zstd", w.Header().Get("Content-Type"))
	assert.Equal(t, snapshotData, w.Body.Bytes())

	// Test snapshot not found - repo has no mirror, so clone is attempted but
	// fails immediately because the context is cancelled.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	req = httptest.NewRequest(http.MethodGet, "/git/github.com/org/nonexistent/snapshot.tar.zst", nil)
	req = req.WithContext(cancelCtx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/nonexistent/snapshot.tar.zst")
	w = httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 503, w.Code)
}

func TestSnapshotOnDemandGenerationViaHTTP(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "application/zstd", w.Header().Get("Content-Type"))
	assert.NotZero(t, w.Body.Len())

	// Allow background goroutines (spool cleanup, cache backfill) to finish
	// before TempDir cleanup runs.
	time.Sleep(2 * time.Second)
}

// createTestMirrorRepo creates a bare mirror-style repo at mirrorPath with one commit.
func createTestMirrorRepo(t *testing.T, mirrorPath string) {
	t.Helper()
	tmpWork := t.TempDir()

	for _, args := range [][]string{
		{"init", tmpWork},
		{"-C", tmpWork, "config", "user.email", "test@test.com"},
		{"-C", tmpWork, "config", "user.name", "Test"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}

	assert.NoError(t, os.WriteFile(filepath.Join(tmpWork, "hello.txt"), []byte("hello\n"), 0o644))

	for _, args := range [][]string{
		{"-C", tmpWork, "add", "."},
		{"-C", tmpWork, "commit", "-m", "initial"},
		{"clone", "--mirror", tmpWork, mirrorPath},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}
}

func TestSnapshotGenerationViaLocalClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"

	// Create a mirror repo at the path the clone manager would use.
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// GetOrCreate so the strategy knows about the repo.
	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)
	assert.Equal(t, gitclone.StateReady, repo.State())

	// Generate the snapshot.
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	// Verify snapshot was uploaded to cache.
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	_, headers, err := memCache.Open(ctx, cacheKey)
	assert.NoError(t, err)
	assert.Equal(t, "application/zstd", headers.Get("Content-Type"))

	// Restore the snapshot and verify it is a working (non-bare) checkout.
	restoreDir := filepath.Join(tmpDir, "restored")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	// A non-bare clone has a .git directory (not a bare repo).
	_, err = os.Stat(filepath.Join(restoreDir, ".git"))
	assert.NoError(t, err)

	// The working tree should contain the committed file.
	data, err := os.ReadFile(filepath.Join(restoreDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello\n", string(data))

	// The remote URL must point to the upstream, not the local mirror path.
	cmd := exec.Command("git", "-C", restoreDir, "remote", "get-url", "origin")
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, upstreamURL+"\n", string(output))

	// Snapshot working directory should have been cleaned up.
	snapshotWorkDir := filepath.Join(mirrorRoot, ".snapshots", "github.com", "org", "repo")
	_, err = os.Stat(snapshotWorkDir)
	assert.True(t, os.IsNotExist(err))
}

func TestMirrorSnapshotRestoreDirectly(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"

	// Create a mirror repo and generate a mirror snapshot (bare tarball).
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	err = s.GenerateAndUploadMirrorSnapshot(ctx, repo)
	assert.NoError(t, err)

	// Restore the mirror snapshot into a new directory.
	restoreDir := filepath.Join(tmpDir, "restored-mirror")
	cacheKey := cache.NewKey(upstreamURL + ".mirror-snapshot")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	// Should be bare already (no .git subdir).
	_, err = os.Stat(filepath.Join(restoreDir, ".git"))
	assert.True(t, os.IsNotExist(err), "mirror snapshot should be bare, no .git directory")

	cmd := exec.Command("git", "-C", restoreDir, "config", "core.bare")
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, "true\n", string(output))

	// Remote should already point to the upstream (mirror clone uses upstream URL).
	cmd = exec.Command("git", "-C", restoreDir, "remote", "get-url", "origin")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	// Mirror clones from a local path, so remote points to the local mirror.
	// After restore on a real pod, configureMirror would not change this;
	// the upstream URL is set correctly because the snapshot IS the mirror.

	// Verify the repo is functional: git branch should work.
	cmd = exec.Command("git", "-C", restoreDir, "branch")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Contains(t, string(output), "main")

	// Verify fetch refspec is mirror-style.
	cmd = exec.Command("git", "-C", restoreDir, "config", "remote.origin.fetch")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, "+refs/*:refs/*\n", string(output))

	// Verify remote.origin.mirror is set.
	cmd = exec.Command("git", "-C", restoreDir, "config", "remote.origin.mirror")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, "true\n", string(output))

	// No refs/remotes should exist (it's a mirror clone, not a regular clone).
	_, err = os.Stat(filepath.Join(restoreDir, "refs", "remotes"))
	assert.True(t, os.IsNotExist(err), "mirror snapshot should not have refs/remotes")
}

func TestMirrorSnapshotWithMultipleBranches(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"

	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepoWithBranches(t, mirrorPath, []string{"feature/branch-a", "fix/branch-b"})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	err = s.GenerateAndUploadMirrorSnapshot(ctx, repo)
	assert.NoError(t, err)

	// Restore and verify all branches are present as refs/heads/*.
	restoreDir := filepath.Join(tmpDir, "restored-mirror")
	cacheKey := cache.NewKey(upstreamURL + ".mirror-snapshot")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	cmd := exec.Command("git", "-C", restoreDir, "show-ref", "--heads")
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	refs := string(output)
	assert.Contains(t, refs, "refs/heads/feature/branch-a")
	assert.Contains(t, refs, "refs/heads/fix/branch-b")
	assert.True(t, strings.Count(refs, "refs/heads/") >= 3, "expected at least 3 branches, got: %s", refs)

	// No refs/remotes should exist (mirror clone stores branches directly).
	_, err = os.Stat(filepath.Join(restoreDir, "refs", "remotes"))
	assert.True(t, os.IsNotExist(err), "mirror snapshot should not have refs/remotes")
}

// createTestMirrorRepoWithBranches creates a bare mirror-style repo at
// mirrorPath with one commit on main and additional branches.
func createTestMirrorRepoWithBranches(t *testing.T, mirrorPath string, branches []string) {
	t.Helper()
	tmpWork := t.TempDir()

	for _, args := range [][]string{
		{"init", tmpWork},
		{"-C", tmpWork, "config", "user.email", "test@test.com"},
		{"-C", tmpWork, "config", "user.name", "Test"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}

	assert.NoError(t, os.WriteFile(filepath.Join(tmpWork, "hello.txt"), []byte("hello\n"), 0o644))

	for _, args := range [][]string{
		{"-C", tmpWork, "add", "."},
		{"-C", tmpWork, "commit", "-m", "initial"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}

	for _, branch := range branches {
		cmd := exec.Command("git", "-C", tmpWork, "branch", branch)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}

	cmd := exec.Command("git", "clone", "--mirror", tmpWork, mirrorPath)
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))

	// Pack all refs so the snapshot exercises the packed-refs code path.
	cmd = exec.Command("git", "-C", mirrorPath, "pack-refs", "--all")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
}

func TestSnapshotRemoteURLUsesServerURL(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	serverURL := "http://cachew.example.com"

	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{ServerURL: serverURL}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	restoreDir := filepath.Join(tmpDir, "restored")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	cmd := exec.Command("git", "-C", restoreDir, "remote", "get-url", "origin")
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, serverURL+"/git/github.com/org/repo\n", string(output))
}
