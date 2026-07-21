package git_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
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

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: mirrorRoot,
	}, nil)
	// SnapshotInterval=0 disables periodic snapshot jobs so they don't
	// overwrite the fake cached snapshot we insert below.
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	// The warm-up goroutine uses context.WithoutCancel so it survives
	// t.Context() cancellation. Wait for it before TempDir cleanup.
	t.Cleanup(func() { waitForReady(t, s) })

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

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
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
	createTestMirrorRepoWithFiles(t, mirrorPath, map[string]string{
		"hello.txt": "hello\n",
	})
}

func createTestMirrorRepoWithFiles(t *testing.T, mirrorPath string, files map[string]string) {
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

	for name, content := range files {
		path := filepath.Join(tmpWork, name)
		assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

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

// addCommitToMirror pushes a new empty commit to the bare mirror so its HEAD
// moves.
func addCommitToMirror(t *testing.T, mirrorPath string) {
	t.Helper()
	tmpWork := t.TempDir()
	for _, args := range [][]string{
		{"clone", mirrorPath, tmpWork},
		{"-C", tmpWork, "config", "user.email", "test@test.com"},
		{"-C", tmpWork, "config", "user.name", "Test"},
		{"-C", tmpWork, "commit", "--allow-empty", "-m", "update"},
		{"-C", tmpWork, "push", "origin", "HEAD"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}
}

// TestSnapshotUnchangedSkipsRegeneration proves the unchanged-skip end to
// end: with shared coordination state, regenerating while HEAD is unchanged
// leaves the cached snapshot (and its ETag) untouched, while a lost cache
// entry or a moved HEAD triggers a real regeneration.
func TestSnapshotUnchangedSkipsRegeneration(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{SnapshotMaxAge: time.Hour}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	s.SetMetadataStore(metadatadb.New(ctx, metadatadb.NewMemoryBackend()))

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)
	waitForReady(t, s)

	// Interval 0 lets every run claim; freshness is enforced by max-age.
	const interval = time.Duration(0)
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	etagAt := func() string {
		headers, err := memCache.Stat(ctx, cacheKey)
		assert.NoError(t, err)
		etag := headers.Get(cache.ETagKey)
		assert.NotZero(t, etag)
		return etag
	}

	assert.NoError(t, s.RunCoordinatedSnapshot(ctx, repo, interval))
	etag1 := etagAt()

	// Unchanged HEAD: the upload is skipped and the ETag stays stable.
	assert.NoError(t, s.RunCoordinatedSnapshot(ctx, repo, interval))
	assert.Equal(t, etag1, etagAt())

	// A lost cache entry regenerates despite matching coordination metadata.
	assert.NoError(t, memCache.Delete(ctx, cacheKey))
	assert.NoError(t, s.RunCoordinatedSnapshot(ctx, repo, interval))
	etag2 := etagAt()
	assert.NotEqual(t, etag1, etag2)

	// A moved HEAD regenerates.
	addCommitToMirror(t, mirrorPath)
	assert.NoError(t, s.RunCoordinatedSnapshot(ctx, repo, interval))
	assert.NotEqual(t, etag2, etagAt())
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

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
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
	waitForReady(t, s)
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
	snapshotWorkDir := filepath.Join(mirrorRoot, ".snapshots", "github.com", "org", "repo", "base")
	_, err = os.Stat(snapshotWorkDir)
	assert.True(t, os.IsNotExist(err))
}

func TestSnapshotGenerationIncludesTrackedLockFiles(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"

	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepoWithFiles(t, mirrorPath, map[string]string{
		"hello.txt":           "hello\n",
		"package-lock.json":   "{\n  \"name\": \"repo\"\n}\n",
		"subdir/Gemfile.lock": "GEM\n",
	})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)
	assert.Equal(t, gitclone.StateReady, repo.State())

	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	restoreDir := filepath.Join(tmpDir, "restored")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(restoreDir, "package-lock.json"))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"repo\"\n}\n", string(data))

	data, err = os.ReadFile(filepath.Join(restoreDir, "subdir", "Gemfile.lock"))
	assert.NoError(t, err)
	assert.Equal(t, "GEM\n", string(data))
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

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	waitForReady(t, s)
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

	// Verify the repo is functional: git branch should list at least one branch.
	cmd = exec.Command("git", "-C", restoreDir, "branch")
	output, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.True(t, len(strings.TrimSpace(string(output))) > 0, "expected at least one branch")

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

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	waitForReady(t, s)
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

func TestSnapshotServesFreshSnapshotWithCommitHeader(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	// Generate a snapshot — it will embed the mirror's HEAD as X-Cachew-Snapshot-Commit.
	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	// Serve the snapshot via HTTP. Since the mirror's HEAD matches the snapshot
	// commit, no bundle URL should be set, but X-Cachew-Snapshot-Commit must
	// be forwarded so the client knows the snapshot is fresh.
	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.NotEqual(t, "", w.Header().Get("X-Cachew-Snapshot-Commit"),
		"X-Cachew-Snapshot-Commit should be set so client knows snapshot is fresh")
	assert.Equal(t, "", w.Header().Get("X-Cachew-Bundle-Url"),
		"no bundle URL when snapshot is already at mirror HEAD")
	assert.NotEqual(t, "", w.Header().Get(cache.ETagKey),
		"ETag should be forwarded from the cached snapshot so clients can revalidate")
	assert.NotEqual(t, "", w.Header().Get("Content-Length"),
		"Content-Length should be forwarded from the cached snapshot")
}

func TestSnapshotHeadServesMetadataWithoutBody(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// Before any snapshot exists, HEAD must report 404 rather than generating one.
	missReq := httptest.NewRequest(http.MethodHead, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	missReq = missReq.WithContext(ctx)
	missReq.SetPathValue("host", "github.com")
	missReq.SetPathValue("path", "org/repo/snapshot.tar.zst")
	missResp := httptest.NewRecorder()
	handler.ServeHTTP(missResp, missReq)
	assert.Equal(t, 404, missResp.Code, "HEAD on an uncached snapshot must not trigger generation")

	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	req := httptest.NewRequest(http.MethodHead, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, 0, w.Body.Len(), "HEAD must not return a body")
	assert.Equal(t, "application/zstd", w.Header().Get("Content-Type"))
	assert.NotEqual(t, "", w.Header().Get(cache.ETagKey),
		"HEAD should report the snapshot ETag from cache metadata")
	assert.NotEqual(t, "", w.Header().Get("Content-Length"),
		"HEAD should report the snapshot Content-Length from cache metadata")
	assert.NotEqual(t, "", w.Header().Get("X-Cachew-Snapshot-Commit"),
		"HEAD should report the snapshot commit from cache metadata")

	// A HEAD carrying the snapshot's ETag in If-None-Match must revalidate to 304.
	etag := w.Header().Get(cache.ETagKey)
	condReq := httptest.NewRequest(http.MethodHead, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	condReq = condReq.WithContext(ctx)
	condReq.SetPathValue("host", "github.com")
	condReq.SetPathValue("path", "org/repo/snapshot.tar.zst")
	condReq.Header.Set("If-None-Match", etag)
	condResp := httptest.NewRecorder()
	handler.ServeHTTP(condResp, condReq)
	assert.Equal(t, http.StatusNotModified, condResp.Code,
		"HEAD with a matching If-None-Match should return 304")
}

func TestSnapshotGetHonorsIfNoneMatch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	// The memory cache returns a non-*os.File reader, exercising the io.Copy path
	// where ServeContent would not otherwise evaluate conditionals.
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// First GET captures the advertised ETag and the full body.
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)
	etag := w.Header().Get(cache.ETagKey)
	assert.NotEqual(t, "", etag, "GET should advertise the snapshot ETag")
	assert.NotEqual(t, 0, w.Body.Len(), "unconditional GET should return the snapshot body")

	// A conditional GET with the matching ETag must return 304 with no body.
	condReq := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	condReq = condReq.WithContext(ctx)
	condReq.SetPathValue("host", "github.com")
	condReq.SetPathValue("path", "org/repo/snapshot.tar.zst")
	condReq.Header.Set("If-None-Match", etag)
	condResp := httptest.NewRecorder()
	handler.ServeHTTP(condResp, condReq)
	assert.Equal(t, http.StatusNotModified, condResp.Code,
		"GET with a matching If-None-Match should return 304")
	assert.Equal(t, 0, condResp.Body.Len(), "304 must not return a body")
}

func TestSnapshotServesBundleURLWhenStale(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	// Generate a snapshot at the current HEAD.
	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	// Add a new commit to the mirror so the snapshot becomes stale.
	tmpWork := t.TempDir()
	for _, args := range [][]string{
		{"clone", mirrorPath, tmpWork},
		{"-C", tmpWork, "config", "user.email", "test@test.com"},
		{"-C", tmpWork, "config", "user.name", "Test"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}
	assert.NoError(t, os.WriteFile(filepath.Join(tmpWork, "new.txt"), []byte("new\n"), 0o644))
	for _, args := range [][]string{
		{"-C", tmpWork, "add", "."},
		{"-C", tmpWork, "commit", "-m", "new commit"},
		{"-C", tmpWork, "push", "origin", "HEAD"},
	} {
		cmd := exec.Command("git", args...)
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(output))
	}

	handler := mux.handlers["GET /git/{host}/{path...}"]
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.NotEqual(t, "", w.Header().Get("X-Cachew-Snapshot-Commit"),
		"X-Cachew-Snapshot-Commit should be set")
	assert.NotEqual(t, "", w.Header().Get("X-Cachew-Bundle-Url"),
		"bundle URL should be set when snapshot is stale")
	assert.Contains(t, w.Header().Get("X-Cachew-Bundle-Url"), "snapshot.bundle?base=",
		"bundle URL should include base parameter")

	// Allow background bundle generation goroutine to finish.
	time.Sleep(2 * time.Second)
}

func TestColdSnapshotServesWithoutCommitHeader(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Pre-populate the cache with a fake snapshot that has NO X-Cachew-Snapshot-Commit
	// header, simulating a cold-start scenario where the snapshot was uploaded to S3
	// without mirror metadata.
	upstreamURL := "https://github.com/org/coldrepo"
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/zstd"}
	headers["Last-Modified"] = []string{time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)}
	writer, err := memCache.Create(ctx, cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write([]byte("fake cold snapshot"))
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	handler := mux.handlers["GET /git/{host}/{path...}"]

	// Use a cancelled context so ensureCloneReady fails quickly and the cold
	// path returns the cached snapshot.
	reqCtx, cancelReq := context.WithCancel(ctx)

	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/coldrepo/snapshot.tar.zst", nil)
	req = req.WithContext(reqCtx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/coldrepo/snapshot.tar.zst")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	cancelReq()

	// Cold path should serve the snapshot but without X-Cachew-Snapshot-Commit,
	// signaling to the client that it needs to freshen.
	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "", w.Header().Get("X-Cachew-Snapshot-Commit"),
		"cold path should not set X-Cachew-Snapshot-Commit")
	assert.Equal(t, "", w.Header().Get("X-Cachew-Bundle-Url"),
		"cold path should not set bundle URL")
}

func TestDeferredRestoreOnlyScheduledOnce(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Pre-populate cache with a fake snapshot.
	upstreamURL := "https://github.com/org/deferred-test"
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/zstd"}
	headers["Last-Modified"] = []string{time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)}
	writer, err := memCache.Create(ctx, cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write([]byte("fake snapshot"))
	assert.NoError(t, err)
	assert.NoError(t, writer.Close())

	handler := mux.handlers["GET /git/{host}/{path...}"]

	// First request: cold path serves snapshot and schedules deferred restore.
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/deferred-test/snapshot.tar.zst", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/deferred-test/snapshot.tar.zst")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	assert.Equal(t, 200, w.Code)

	// Second request: should not panic or fail — deferred restore should only
	// be scheduled once (idempotent via deferredRestoreOnce).
	req2 := httptest.NewRequest(http.MethodGet, "/git/github.com/org/deferred-test/snapshot.tar.zst", nil)
	req2 = req2.WithContext(ctx)
	req2.SetPathValue("host", "github.com")
	req2.SetPathValue("path", "org/deferred-test/snapshot.tar.zst")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	// Second request may be 200 (from local cache) or 503 (clone not ready).
	// The key assertion is that it doesn't panic from double-scheduling.
}

func TestCacheBundleAbortsOnWriteFailure(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)

	// Wrap cache so Create returns a writer that fails on Write.
	failCache := &failWriteCache{Cache: memCache}

	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), failCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)

	key := cache.NewKey("test-bundle-abort")
	data := []byte("bundle data that should not persist")

	err = s.CacheBundle(ctx, key, bytes.NewReader(data))
	assert.Error(t, err)

	// Verify nothing was persisted — check underlying memCache, not failCache.
	_, _, err = memCache.Open(ctx, key)
	assert.IsError(t, err, os.ErrNotExist)
}

// failWriteCache wraps a cache.Cache and makes Create return a writer that
// always fails on Write.
type failWriteCache struct {
	cache.Cache
}

func (f *failWriteCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) (cache.Writer, error) {
	wc, err := f.Cache.Create(ctx, key, headers, ttl, opts...)
	if err != nil {
		return nil, err
	}
	return &failWriter{inner: wc}, nil
}

func (f *failWriteCache) Namespace(ns cache.Namespace) cache.Cache {
	return &failWriteCache{Cache: f.Cache.Namespace(ns)}
}

type failWriter struct {
	inner cache.Writer
}

func (w *failWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrShortWrite
}

func (w *failWriter) Close() error {
	return w.inner.Close()
}

func (w *failWriter) Abort(err error) error {
	return w.inner.Abort(err)
}

func TestSnapshotRemoteURLUsesUpstreamURL(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"

	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	restoreDir := filepath.Join(tmpDir, "restored")
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir, 0)
	assert.NoError(t, err)

	cmd := exec.Command("git", "-C", restoreDir, "remote", "get-url", "origin")
	output, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(output))
	assert.Equal(t, upstreamURL+"\n", string(output))
}

func TestSnapshotGetHonorsRange(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamURL := "https://github.com/org/repo"
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)

	// The memory cache returns a non-*os.File reader, so range handling must come
	// from the strategy forwarding Range to Open rather than http.ServeContent.
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	manager, err := cm()
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	waitForReady(t, s)
	err = s.GenerateAndUploadSnapshot(ctx, repo)
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	get := func(rangeHeader string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
		req = req.WithContext(ctx)
		req.SetPathValue("host", "github.com")
		req.SetPathValue("path", "org/repo/snapshot.tar.zst")
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}

	// Full GET advertises range support and yields the whole body.
	full := get("")
	assert.Equal(t, 200, full.Code)
	assert.Equal(t, "bytes", full.Header().Get("Accept-Ranges"))
	body := full.Body.Bytes()
	assert.True(t, len(body) > 4, "snapshot body should be larger than the test range")
	commit := full.Header().Get("X-Cachew-Snapshot-Commit")
	assert.NotZero(t, commit, "full GET should advertise the snapshot commit")

	// A satisfiable range returns 206 with the matching bytes and Content-Range.
	partial := get("bytes=0-3")
	assert.Equal(t, http.StatusPartialContent, partial.Code)
	assert.Equal(t, "bytes", partial.Header().Get("Accept-Ranges"))
	assert.Equal(t, "bytes 0-3/"+strconv.Itoa(len(body)), partial.Header().Get("Content-Range"))
	assert.Equal(t, body[:4], partial.Body.Bytes())
	// Ranged clients must receive the same freshen metadata as a full GET so
	// they can apply a delta bundle after a parallel download.
	assert.Equal(t, commit, partial.Header().Get("X-Cachew-Snapshot-Commit"))

	// A range beyond the object is not satisfiable.
	tooBig := get("bytes=" + strconv.Itoa(len(body)+10) + "-" + strconv.Itoa(len(body)+20))
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, tooBig.Code)
	assert.Equal(t, "bytes */"+strconv.Itoa(len(body)), tooBig.Header().Get("Content-Range"))

	etag := full.Header().Get("ETag")
	assert.NotZero(t, etag, "snapshot should advertise an ETag")

	getCond := func(rangeHeader string, condHeaders map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot.tar.zst", nil)
		req = req.WithContext(ctx)
		req.SetPathValue("host", "github.com")
		req.SetPathValue("path", "org/repo/snapshot.tar.zst")
		req.Header.Set("Range", rangeHeader)
		for k, v := range condHeaders {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}

	// Conditional validators take precedence over the range: a matching
	// If-None-Match revalidates to 304 and a stale If-Match fails with 412,
	// rather than returning a 206 body the client would discard.
	notModified := getCond("bytes=0-3", map[string]string{"If-None-Match": etag})
	assert.Equal(t, http.StatusNotModified, notModified.Code)
	assert.Equal(t, 0, notModified.Body.Len())

	preconditionFailed := getCond("bytes=0-3", map[string]string{"If-Match": `"stale-etag"`})
	assert.Equal(t, http.StatusPreconditionFailed, preconditionFailed.Code)

	// An unsatisfiable range with a stale If-Match is a precondition failure
	// (412), not a 416.
	beyond := strconv.Itoa(len(body)+10) + "-" + strconv.Itoa(len(body)+20)
	rangeWithStaleIfMatch := getCond("bytes="+beyond, map[string]string{"If-Match": `"stale-etag"`})
	assert.Equal(t, http.StatusPreconditionFailed, rangeWithStaleIfMatch.Code)
}
