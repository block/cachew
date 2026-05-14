package git_test

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/block/cachew/internal/strategy/git"
)

func TestEnsureRefsHandler(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()
	mirrorRoot := filepath.Join(tmpDir, "mirrors")

	// Build a bare upstream repo and a mirror clone of it. The mirror path
	// must match what cachew derives from /git/local/repo.
	upstreamPath := filepath.Join(tmpDir, "upstream.git")
	workPath := filepath.Join(tmpDir, "work")
	for _, args := range [][]string{
		{"init", "--bare", upstreamPath},
		{"clone", upstreamPath, workPath},
		{"-C", workPath, "config", "user.email", "test@test.com"},
		{"-C", workPath, "config", "user.name", "Test"},
	} {
		assert.NoError(t, exec.Command("git", args...).Run())
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("v1"), 0o644))
	for _, args := range [][]string{
		{"-C", workPath, "add", "."},
		{"-C", workPath, "commit", "-m", "v1"},
		{"-C", workPath, "push", "origin", "HEAD:main"},
	} {
		assert.NoError(t, exec.Command("git", args...).Run())
	}

	mirrorPath := filepath.Join(mirrorRoot, "local", "repo")
	assert.NoError(t, exec.Command("git", "clone", "--mirror", upstreamPath, mirrorPath).Run())

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	s, err := git.New(ctx, git.Config{}, newTestScheduler(ctx, t), memCache, mux, cm,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)

	handler := mux.handlers["POST /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	doRequest := func(refs map[string]string) (*httptest.ResponseRecorder, git.EnsureRefsResponse) {
		body, err := json.Marshal(map[string]any{"refs": refs})
		assert.NoError(t, err)
		req := httptest.NewRequestWithContext(ctx, http.MethodPost,
			"/git/local/repo/ensure-refs", bytes.NewReader(body))
		req.SetPathValue("host", "local")
		req.SetPathValue("path", "repo/ensure-refs")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		var resp git.EnsureRefsResponse
		if w.Code == http.StatusOK {
			assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		}
		return w, resp
	}

	// Fresh mirror already has main → no fetch.
	w, resp := doRequest(map[string]string{"refs/heads/main": ""})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.False(t, resp.Fetched)
	assert.NotEqual(t, "", resp.Refs["refs/heads/main"])

	// Push a new commit upstream to make the mirror stale.
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("v2"), 0o644))
	for _, args := range [][]string{
		{"-C", workPath, "commit", "-am", "v2"},
		{"-C", workPath, "push", "origin", "HEAD:main"},
	} {
		assert.NoError(t, exec.Command("git", args...).Run())
	}
	newSHAOut, err := exec.Command("git", "-C", workPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	newSHA := strings.TrimSpace(string(newSHAOut))

	// Asking for the new SHA forces a fetch and returns the fresh value.
	w, resp = doRequest(map[string]string{"refs/heads/main": newSHA})
	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, resp.Fetched)
	assert.Equal(t, newSHA, resp.Refs["refs/heads/main"])

	// Empty refs map → 400.
	w, _ = doRequest(map[string]string{})
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
