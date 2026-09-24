package git_test

import (
	"bytes"
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

type infoRefsTransport struct {
	hits atomic.Int32
}

// RoundTrip records proxy requests and returns a response that identifies the upstream path.
func (t *infoRefsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.hits.Add(1)
	body := "proxied-info-refs"
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        http.StatusText(http.StatusOK),
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

type infoRefsFixture struct {
	ctx          context.Context
	handler      http.Handler
	transport    *infoRefsTransport
	logs         *bytes.Buffer
	upstreamPath string
	workPath     string
	mirrorPath   string
}

func newInfoRefsFixture(t *testing.T) *infoRefsFixture {
	t.Helper()

	tmpDir := t.TempDir()
	upstreamPath := filepath.Join(tmpDir, "upstream.git")
	workPath := filepath.Join(tmpDir, "work")
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "example.test", "org", "repo")

	runInfoRefsGit(t, "init", "--bare", upstreamPath)
	runInfoRefsGit(t, "clone", upstreamPath, workPath)
	runInfoRefsGit(t, "-C", workPath, "config", "user.email", "test@test.com")
	runInfoRefsGit(t, "-C", workPath, "config", "user.name", "Test")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file.txt"), []byte("initial"), 0o644))
	runInfoRefsGit(t, "-C", workPath, "add", "file.txt")
	runInfoRefsGit(t, "-C", workPath, "commit", "-m", "initial")
	runInfoRefsGit(t, "-C", workPath, "push", "origin", "HEAD:main")
	runInfoRefsGit(t, "-C", upstreamPath, "symbolic-ref", "HEAD", "refs/heads/main")
	runInfoRefsGit(t, "clone", "--mirror", upstreamPath, mirrorPath)

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	ctx := logging.ContextWithLogger(t.Context(), logger)
	mux := newTestMux()
	manager := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	strategy, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), nil, mux, manager,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, strategy)

	transport := &infoRefsTransport{}
	strategy.SetHTTPTransport(transport)
	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	return &infoRefsFixture{
		ctx:          ctx,
		handler:      handler,
		transport:    transport,
		logs:         logs,
		upstreamPath: upstreamPath,
		workPath:     workPath,
		mirrorPath:   mirrorPath,
	}
}

func runInfoRefsGit(t *testing.T, args ...string) string {
	t.Helper()
	output, err := exec.Command("git", args...).CombinedOutput()
	assert.NoError(t, err, string(output))
	return strings.TrimSpace(string(output))
}

func setInfoRefsLsRemote(t *testing.T, target string, fail bool) {
	t.Helper()
	realGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	wrapperDir := t.TempDir()
	wrapper := filepath.Join(wrapperDir, "git")
	lsRemote := fmt.Sprintf("exec %q ls-remote %q", realGit, target)
	if fail {
		lsRemote = `echo "ls-remote failed" >&2
  exit 1`
	}
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "ls-remote" ]; then
  %s
fi
exec %q "$@"
`, lsRemote, realGit)
	assert.NoError(t, os.WriteFile(wrapper, []byte(script), 0o755))
	t.Setenv("PATH", wrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func (f *infoRefsFixture) request(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(f.ctx, http.MethodGet,
		"/git/example.test/org/repo/info/refs?service=git-upload-pack", nil)
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/info/refs")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, req)
	return w
}

func (f *infoRefsFixture) commitUpstream(t *testing.T) string {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(f.workPath, "file.txt"), []byte("updated"), 0o644))
	runInfoRefsGit(t, "-C", f.workPath, "commit", "-am", "updated")
	runInfoRefsGit(t, "-C", f.workPath, "push", "origin", "HEAD:main")
	return runInfoRefsGit(t, "-C", f.workPath, "rev-parse", "HEAD")
}

func TestInfoRefsCheckErrorForwardsUpstream(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, "", true)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "proxied-info-refs", w.Body.String())
	assert.Equal(t, int32(1), fixture.transport.hits.Load())
	assert.Contains(t, fixture.logs.String(), "Failed to check refs freshness, forwarding to upstream")
	assert.Contains(t, fixture.logs.String(), "check upstream refs")
}

func TestInfoRefsStaleForwardsAndFetches(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	wantSHA := fixture.commitUpstream(t)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "proxied-info-refs", w.Body.String())
	assert.Equal(t, int32(1), fixture.transport.hits.Load())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main") == wantSHA {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mirror did not fetch %s", wantSHA)
}

func TestInfoRefsFreshServesMirrorAndCachesCheck(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)

	w := fixture.request(t)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-advertisement")
	assert.NotEqual(t, "proxied-info-refs", w.Body.String())

	assert.NoError(t, os.Rename(fixture.upstreamPath, fixture.upstreamPath+".unavailable"))
	w = fixture.request(t)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-advertisement")
}
