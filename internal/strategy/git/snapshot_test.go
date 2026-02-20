package git_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
	"github.com/block/cachew/internal/strategy/git"
)

func TestSnapshotHTTPEndpoint(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: tmpDir,
	}, nil)
	_, err = git.New(ctx, git.Config{
		SnapshotInterval: 24 * time.Hour,
	}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Create a fake snapshot in the cache
	upstreamURL := "https://github.com/org/repo"
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	snapshotData := []byte("fake snapshot data")

	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/zstd"}
	writer, err := memCache.Create(ctx, cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write(snapshotData)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// Test successful snapshot request
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "application/zstd", w.Header().Get("Content-Type"))
	assert.Equal(t, snapshotData, w.Body.Bytes())

	// Test snapshot not found
	req = httptest.NewRequest(http.MethodGet, "/git/github.com/org/nonexistent/snapshot", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/nonexistent/snapshot")
	w = httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
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
	s, err := git.New(ctx, git.Config{}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
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
	err = snapshot.Restore(ctx, memCache, cacheKey, restoreDir)
	assert.NoError(t, err)

	// A non-bare clone has a .git directory (not a bare repo).
	_, err = os.Stat(filepath.Join(restoreDir, ".git"))
	assert.NoError(t, err)

	// The working tree should contain the committed file.
	data, err := os.ReadFile(filepath.Join(restoreDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello\n", string(data))

	// Snapshot working directory should have been cleaned up.
	snapshotWorkDir := filepath.Join(mirrorRoot, ".snapshots", "github.com", "org", "repo")
	_, err = os.Stat(snapshotWorkDir)
	assert.True(t, os.IsNotExist(err))
}
