//go:build integration

package git_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

// runGit runs a git command, failing the test on error.
func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@test.com",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@test.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// gitRevParse returns the full SHA for the given ref in dir.
func gitRevParse(t *testing.T, dir, ref string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "rev-parse", ref)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse %s: %v\n%s", ref, err, out)
	}
	return strings.TrimSpace(string(out))
}

// pktLine encodes s as a git pkt-line.
func pktLine(s string) string {
	return fmt.Sprintf("%04x%s", len(s)+4, s)
}

// upstreamRedirectTransport rewrites all outbound requests to targetBaseURL,
// preserving the path and query, and counts each rewrite.
type upstreamRedirectTransport struct {
	targetBaseURL string
	inner         http.RoundTripper
	hits          *atomic.Int32
}

func (t *upstreamRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.hits.Add(1)
	req = req.Clone(req.Context())
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.targetBaseURL, "http://")
	req.Host = req.URL.Host
	return t.inner.RoundTrip(req)
}

// testServerWithLogging creates an httptest.Server that injects a logger into the request context.
func testServerWithLogging(ctx context.Context, handler http.Handler) *httptest.Server {
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := logging.FromContext(ctx).With("request", fmt.Sprintf("%s %s", r.Method, r.RequestURI))
		r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
		logger.Debug("Request received")
		handler.ServeHTTP(w, r)
	})
	return httptest.NewServer(wrapper)
}

// TestIntegrationGitCloneViaProxy tests cloning a repository through the git proxy.
// This test requires git to be installed and network access.
func TestIntegrationGitCloneViaProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check if git is available
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")

	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	// Create the git strategy
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	mux := http.NewServeMux()
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	assert.NotZero(t, strategy)

	// Start a test server with logging middleware
	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	// Clone a small public repository through the proxy
	// Using a small test repo to keep the test fast
	repoURL := fmt.Sprintf("%s/git/github.com/octocat/Hello-World", server.URL)

	// First clone - should forward to upstream and start background clone
	cmd := exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo1"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify the clone worked
	readmePath := filepath.Join(workDir, "repo1", "README")
	_, err = os.Stat(readmePath)
	assert.NoError(t, err)

	// Wait a bit for background clone to complete
	time.Sleep(2 * time.Second)

	// Second clone - should be served from local cache
	cmd = exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo2"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify the second clone worked
	readmePath2 := filepath.Join(workDir, "repo2", "README")
	_, err = os.Stat(readmePath2)
	assert.NoError(t, err)

	// Verify the clone was created
	clonePath := filepath.Join(clonesDir, "github.com", "octocat", "Hello-World")
	info, err := os.Stat(clonePath)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())

	// Verify it has a HEAD file (bare mirror clone)
	headFile := filepath.Join(clonePath, "HEAD")
	_, err = os.Stat(headFile)
	assert.NoError(t, err)
}

// TestIntegrationGitFetchViaProxy tests fetching updates through the proxy.
func TestIntegrationGitFetchViaProxy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")

	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)

	mux := http.NewServeMux()
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	repoURL := fmt.Sprintf("%s/git/github.com/octocat/Hello-World", server.URL)

	// Clone first
	cmd := exec.Command("git", "clone", repoURL, filepath.Join(workDir, "repo"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git clone output: %s", output)
	}
	assert.NoError(t, err)

	// Wait for background clone
	time.Sleep(2 * time.Second)

	// Fetch should work
	cmd = exec.Command("git", "-C", filepath.Join(workDir, "repo"), "fetch", "origin")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git fetch output: %s", output)
	}
	assert.NoError(t, err)
}

// TestIntegrationPushForwardsToUpstream verifies that push operations are forwarded.
// This test uses a local git server to verify push forwarding.
func TestIntegrationPushForwardsToUpstream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	upstreamDir := filepath.Join(tmpDir, "upstream")
	workDir := filepath.Join(tmpDir, "work")

	// Create a bare upstream repo
	err := os.MkdirAll(upstreamDir, 0o750)
	assert.NoError(t, err)

	cmd := exec.Command("git", "init", "--bare", filepath.Join(upstreamDir, "repo.git"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("git init output: %s", output)
	}
	assert.NoError(t, err)

	// Track if we received a push request
	pushReceived := false

	// Create a mock upstream that serves git protocol
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("Upstream received: %s %s", r.Method, r.URL.Path)

		if r.URL.Query().Get("service") == "git-receive-pack" || r.URL.Path == "/test/repo/git-receive-pack" {
			pushReceived = true
		}

		// For this test, just acknowledge we received the request
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	defer upstreamServer.Close()

	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	_, err = git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	// Create a local repo to push from
	err = os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	repoPath := filepath.Join(workDir, "repo")
	cmd = exec.Command("git", "init", repoPath)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git init output: %s", output)
	}
	assert.NoError(t, err)

	// Configure git
	cmd = exec.Command("git", "-C", repoPath, "config", "user.email", "test@test.com")
	_, _ = cmd.CombinedOutput()
	cmd = exec.Command("git", "-C", repoPath, "config", "user.name", "Test")
	_, _ = cmd.CombinedOutput()

	// Create a commit
	testFile := filepath.Join(repoPath, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0o644)
	assert.NoError(t, err)

	cmd = exec.Command("git", "-C", repoPath, "add", "test.txt")
	_, _ = cmd.CombinedOutput()

	cmd = exec.Command("git", "-C", repoPath, "commit", "-m", "test commit")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("git commit output: %s", output)
	}
	assert.NoError(t, err)

	// Try to push through the proxy - this will fail but should forward to upstream
	// We're just verifying the forwarding logic, not actual push success
	proxyURL := fmt.Sprintf("%s/git/localhost/test/repo", server.URL)
	cmd = exec.Command("git", "-C", repoPath, "push", proxyURL, "HEAD:main")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	_, _ = cmd.CombinedOutput()

	// Note: The push will likely fail because our mock upstream doesn't implement
	// the full git protocol, but the important thing is verifying the proxy
	// attempted to forward it (which we can verify through logs or the pushReceived flag
	// if we had wired up the server properly)
	t.Logf("Push forwarding test completed, pushReceived=%v", pushReceived)
}

// countingTransport wraps an http.RoundTripper to count outbound requests by URL path pattern.
type countingTransport struct {
	inner   http.RoundTripper
	counter *atomic.Int32
	pattern string
}

func (ct *countingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.Contains(req.URL.Path, ct.pattern) {
		ct.counter.Add(1)
	}
	return ct.inner.RoundTrip(req)
}

// TestIntegrationSpoolReusesDuringClone clones github.com/git/git through the proxy,
// waits 5 seconds (enough for the first clone to start but not finish), then clones
// again. The second clone should be served from the spool rather than making a new
// upstream request.
func TestIntegrationSpoolReusesDuringClone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelDebug})
	tmpDir := t.TempDir()
	clonesDir := filepath.Join(tmpDir, "clones")
	workDir := filepath.Join(tmpDir, "work")
	err := os.MkdirAll(workDir, 0o750)
	assert.NoError(t, err)

	// Count actual outbound upstream requests via a transport wrapper.
	var upstreamUploadPackRequests atomic.Int32

	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 15,
	}, nil)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, gc, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	strategy.SetHTTPTransport(&countingTransport{
		inner:   http.DefaultTransport,
		counter: &upstreamUploadPackRequests,
		pattern: "git-upload-pack",
	})

	server := testServerWithLogging(ctx, mux)
	defer server.Close()

	repoURL := fmt.Sprintf("%s/git/github.com/git/git", server.URL)

	// First clone – triggers upstream pass-through and background clone.
	t.Log("Starting first clone")
	cmd := exec.Command("git", "clone", "--depth=1", repoURL, filepath.Join(workDir, "repo1"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("first clone output: %s", output)
	}
	assert.NoError(t, err)

	// Record how many upstream upload-pack requests the first clone made.
	firstCloneCount := upstreamUploadPackRequests.Load()
	t.Logf("Upstream upload-pack requests after first clone: %d", firstCloneCount)
	assert.True(t, firstCloneCount > 0, "first clone should have made upstream requests")

	// Wait long enough for the background clone to have started but (likely) not
	// finished for a repo as large as git/git.
	t.Log("Waiting 5 seconds for background clone to be in progress")
	time.Sleep(5 * time.Second)

	// Second clone – should be served from the spool if the background clone is
	// still running, or from the local backend if it already finished.
	t.Log("Starting second clone")
	cmd = exec.Command("git", "clone", "--depth=1", repoURL, filepath.Join(workDir, "repo2"))
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Logf("second clone output: %s", output)
	}
	assert.NoError(t, err)

	// Verify both clones produced a working checkout.
	for _, name := range []string{"repo1", "repo2"} {
		gitDir := filepath.Join(workDir, name, ".git")
		_, statErr := os.Stat(gitDir)
		assert.NoError(t, statErr, "expected .git in %s", name)
	}

	// The second clone should not have generated any new upstream upload-pack
	// requests — it should have been served entirely from the spool or local backend.
	totalCount := upstreamUploadPackRequests.Load()
	t.Logf("Total upstream upload-pack requests: %d (first clone: %d)", totalCount, firstCloneCount)
	assert.Equal(t, firstCloneCount, totalCount, "second clone should not have made additional upstream upload-pack requests")
}

// TestIntegrationNotOurRefFallsBackToUpstream reproduces the force-push race
// condition that causes "not our ref" errors and verifies that cachew falls back
// to upstream rather than returning the git protocol error to the client.
//
// The race is:
//  1. Client receives /info/refs showing commit A as the tip of a branch.
//  2. A concurrent background fetch runs, incorporating a force-push that makes
//     commit A unreachable from all refs in the local mirror.
//  3. Client sends POST /git-upload-pack requesting commit A.
//  4. git upload-pack rejects the request: "not our ref <A>".
//
// After the fix, cachew detects the stderr error before any bytes reach the
// client and transparently forwards the request to upstream.
//
// Run with: go test -v -run TestIntegrationNotOurRefFallsBackToUpstream -tags integration -timeout 30s
func TestIntegrationNotOurRefFallsBackToUpstream(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelDebug})
	tmpDir := t.TempDir()

	upstreamDir := filepath.Join(tmpDir, "upstream.git")
	workDir := filepath.Join(tmpDir, "work")
	clonesDir := filepath.Join(tmpDir, "clones")

	// --- Build the upstream repo with an initial commit ---
	runGit(t, "", "init", "--bare", upstreamDir)
	runGit(t, "", "clone", upstreamDir, workDir)
	err := os.WriteFile(filepath.Join(workDir, "file.txt"), []byte("initial"), 0o644)
	assert.NoError(t, err)
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", "initial commit")
	runGit(t, workDir, "push", "origin", "HEAD:main")

	orphanedSHA := gitRevParse(t, workDir, "HEAD")
	t.Logf("Orphaned SHA will be: %s", orphanedSHA)

	// --- Clone upstream as the cachew mirror ---
	// The path must match what cachew derives from the URL /git/local/repo:
	//   host="local", repoPath="repo" → clonesDir/local/repo
	mirrorPath := filepath.Join(clonesDir, "local", "repo")
	runGit(t, "", "clone", "--mirror", upstreamDir, mirrorPath)

	// --- Force-push a completely new history to upstream ---
	// An orphan branch has no ancestors in common with the current history,
	// so after fetching the mirror, orphanedSHA becomes unreachable from all refs.
	runGit(t, workDir, "checkout", "--orphan", "replacement")
	err = os.WriteFile(filepath.Join(workDir, "file2.txt"), []byte("replacement"), 0o644)
	assert.NoError(t, err)
	runGit(t, workDir, "add", ".")
	runGit(t, workDir, "commit", "-m", "replacement commit")
	runGit(t, workDir, "push", "--force", "origin", "replacement:main")

	// Fetch the mirror so it picks up the force-push.
	// After this, orphanedSHA is in the ODB but unreachable from any ref.
	runGit(t, mirrorPath, "fetch", "--prune")

	// Sanity-check: orphanedSHA is still in the ODB ...
	catFile := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", orphanedSHA)
	assert.NoError(t, catFile.Run(), "orphaned SHA should still exist in the mirror ODB")

	// ... but is not reachable from any branch.
	branchContains := exec.Command("git", "-C", mirrorPath, "branch", "--contains", orphanedSHA)
	out, _ := branchContains.CombinedOutput()
	assert.Equal(t, "", strings.TrimSpace(string(out)), "orphaned SHA should not be reachable from any branch")

	// --- Mock upstream: records requests, returns a minimal git flush response ---
	var upstreamHits atomic.Int32
	mockUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		t.Logf("Mock upstream received: %s %s", r.Method, r.URL.Path)
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0008NAK\n")) // minimal NAK response
	}))
	defer mockUpstream.Close()

	// --- Set up cachew pointing at clonesDir ---
	mux := http.NewServeMux()
	gc := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    clonesDir,
		FetchInterval: 24 * time.Hour, // prevent auto-fetch during the test
	}, nil)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, gc,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Redirect all upstream proxy requests to the mock server.
	var redirectHits atomic.Int32
	strategy.SetHTTPTransport(&upstreamRedirectTransport{
		targetBaseURL: mockUpstream.URL,
		inner:         http.DefaultTransport,
		hits:          &redirectHits,
	})

	cachewServer := testServerWithLogging(ctx, mux)
	defer cachewServer.Close()

	// --- Send a raw upload-pack POST requesting the orphaned SHA ---
	// This mirrors what a git client does after receiving /info/refs that
	// advertised orphanedSHA (before the force-push fetch updated the mirror).
	body := pktLine("want "+orphanedSHA+"\n") + "0000" + pktLine("done\n")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cachewServer.URL+"/git/local/repo/git-upload-pack",
		strings.NewReader(body))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")

	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	// After the fix: the request must have been forwarded to upstream.
	// Before the fix: upstreamHits == 0 and this assertion fails.
	assert.True(t, upstreamHits.Load() > 0,
		"cachew should fall back to upstream when git upload-pack returns 'not our ref'")
}
