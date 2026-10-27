package git_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

// pushBranchCommit commits file on branch (creating the branch from HEAD if
// needed) and pushes it to the bare mirror and the mirror's own upstream, so a
// pruning mirror fetch keeps it. It returns the new tip.
func pushBranchCommit(t *testing.T, mirrorPath, branch, file string) string {
	t.Helper()
	work := t.TempDir()
	run := func(args ...string) string {
		output, err := exec.Command("git", args...).CombinedOutput()
		assert.NoError(t, err, string(output))
		return strings.TrimSpace(string(output))
	}
	run("clone", mirrorPath, work)
	run("-C", work, "config", "user.email", "test@test.com")
	run("-C", work, "config", "user.name", "Test")
	if run("-C", work, "ls-remote", "--heads", "origin", branch) == "" {
		run("-C", work, "checkout", "-b", branch)
	} else {
		run("-C", work, "checkout", branch)
	}
	assert.NoError(t, os.WriteFile(filepath.Join(work, file), []byte(file+"\n"), 0o644))
	run("-C", work, "add", ".")
	run("-C", work, "commit", "-m", "update "+branch)
	run("-C", work, "push", "origin", branch)
	run("-C", work, "push", run("-C", mirrorPath, "config", "remote.origin.url"), branch)
	return run("-C", work, "rev-parse", "HEAD")
}

func newBranchSnapshotStrategy(t *testing.T, mirrorRoot string, config git.Config) (context.Context, *testMux, cache.Cache) {
	t.Helper()
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, config, newTestScheduler(ctx, t), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)
	return ctx, mux, memCache
}

func getGitPath(ctx context.Context, mux *testMux, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", strings.TrimPrefix(req.URL.Path, "/git/github.com/"))
	w := httptest.NewRecorder()
	mux.handlers["GET /git/{host}/{path...}"].ServeHTTP(w, req)
	return w
}

func TestBranchSnapshotRequestValidation(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	createTestMirrorRepo(t, filepath.Join(mirrorRoot, "github.com", "org", "repo"))

	ctx, mux, _ := newBranchSnapshotStrategy(t, mirrorRoot, git.Config{})
	w := getGitPath(ctx, mux, "/git/github.com/org/repo/snapshot.tar.zst?ref=release/1.0")
	assert.Equal(t, http.StatusNotFound, w.Code, "branch snapshots are off by default")

	ctx, mux, _ = newBranchSnapshotStrategy(t, mirrorRoot, git.Config{SnapshotInterval: time.Hour, BranchSnapshotMaxRefs: 2})
	for _, ref := range []string{"", "-x", "../main", "a..b", "a/", "a.lock", "a b", "a@{1}"} {
		resp := getGitPath(ctx, mux, "/git/github.com/org/repo/snapshot.tar.zst?ref="+url.QueryEscape(ref))
		assert.Equal(t, http.StatusBadRequest, resp.Code, "ref %q", ref)
	}
	w = getGitPath(ctx, mux, "/git/github.com/org/repo/snapshot.bundle?base="+strings.Repeat("a", 40)+"&ref=..")
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBranchSnapshotBuiltOnMissAndFreshenedByBundle(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	createTestMirrorRepo(t, mirrorPath)
	branchTip := pushBranchCommit(t, mirrorPath, "release/1.0", "release.txt")

	ctx, mux, memCache := newBranchSnapshotStrategy(t, mirrorRoot, git.Config{
		SnapshotInterval:          time.Hour,
		BranchSnapshotMaxRefs:     2,
		BranchSnapshotIdleTimeout: time.Hour,
	})
	const target = "/git/github.com/org/repo/snapshot.tar.zst?ref=release%2F1.0"

	w := getGitPath(ctx, mux, target)
	assert.Equal(t, http.StatusNotFound, w.Code, "the first request misses and schedules a build")

	deadline := time.Now().Add(30 * time.Second)
	for w.Code != http.StatusOK && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		w = getGitPath(ctx, mux, target)
	}
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, branchTip, w.Header().Get("X-Cachew-Snapshot-Commit"))
	assert.Equal(t, "", w.Header().Get("X-Cachew-Bundle-Url"), "a snapshot at the branch tip needs no bundle")

	headers, err := memCache.Stat(ctx, cache.NewKey("https://github.com/org/repo.snapshot@release/1.0"))
	assert.NoError(t, err)
	assert.Equal(t, branchTip, headers.Get("X-Cachew-Snapshot-Commit"))

	newTip := pushBranchCommit(t, mirrorPath, "release/1.0", "release2.txt")
	w = getGitPath(ctx, mux, target)
	assert.Equal(t, http.StatusOK, w.Code)
	bundleURL := w.Header().Get("X-Cachew-Bundle-Url")
	assert.Equal(t, "/git/github.com/org/repo/snapshot.bundle?base="+branchTip+"&ref=release%2F1.0", bundleURL)

	w = getGitPath(ctx, mux, bundleURL)
	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	bundlePath := filepath.Join(t.TempDir(), "delta.bundle")
	assert.NoError(t, os.WriteFile(bundlePath, w.Body.Bytes(), 0o600))
	heads, err := exec.Command("git", "bundle", "list-heads", bundlePath).CombinedOutput()
	assert.NoError(t, err, string(heads))
	assert.Equal(t, newTip+" refs/heads/release/1.0", strings.TrimSpace(string(heads)))
}

func TestBranchSnapshotUnknownBranchStaysMissing(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	createTestMirrorRepo(t, filepath.Join(mirrorRoot, "github.com", "org", "repo"))

	ctx, mux, memCache := newBranchSnapshotStrategy(t, mirrorRoot, git.Config{SnapshotInterval: time.Hour, BranchSnapshotMaxRefs: 2})
	w := getGitPath(ctx, mux, "/git/github.com/org/repo/snapshot.tar.zst?ref=does-not-exist")
	assert.Equal(t, http.StatusNotFound, w.Code)

	time.Sleep(2 * time.Second)
	_, err := memCache.Stat(ctx, cache.NewKey("https://github.com/org/repo.snapshot@does-not-exist"))
	assert.IsError(t, err, os.ErrNotExist)
}

func TestValidBranchName(t *testing.T) {
	for _, ref := range []string{"main", "release/1.2", "feature/x_y-z", "v1.2.3"} {
		assert.True(t, git.ValidBranchName(ref), ref)
	}
	for _, ref := range []string{"", "-main", "/main", "a//b", "a/.b", ".hidden", "a..b", "a/", "a.", "x.lock", "a b", "a~1", "a^", "a:b", "a@{1}", strings.Repeat("a", 256)} {
		assert.False(t, git.ValidBranchName(ref), ref)
	}
}

func TestBranchSnapshotTrackingEvictsLeastRecentlyRequested(t *testing.T) {
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{BranchSnapshotMaxRefs: 2, BranchSnapshotIdleTimeout: time.Hour}, newTestScheduler(ctx, t), memCache, newTestMux(), cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	const upstream = "https://github.com/org/repo"
	s.TrackRef(upstream, "a")
	time.Sleep(time.Millisecond)
	s.TrackRef(upstream, "b")
	time.Sleep(time.Millisecond)
	s.TrackRef(upstream, "a")
	time.Sleep(time.Millisecond)
	s.TrackRef(upstream, "c")

	active := s.ActiveRefs(upstream)
	slices.Sort(active)
	assert.Equal(t, []string{"a", "c"}, active)
}
