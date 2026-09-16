package git_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

func TestCloneUploadPackRejectedBeforePackfile(t *testing.T) {
	t.Parallel()
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: filepath.Join(t.TempDir(), "clones"),
	}, nil)
	s, err := git.New(ctx, git.Config{
		UploadPackCloneConcurrency: 1,
		UploadPackCloneRetryAfter:  15 * time.Second,
	}, newTestScheduler(ctx, t), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)

	release, admitted := s.HoldUploadPackCloneSlotForTest(ctx, "https://github.com/org/repo")
	assert.True(t, admitted)
	defer release()

	handler := mux.handlers["POST /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	req := httptest.NewRequestWithContext(ctx, http.MethodPost,
		"/git/github.com/org/repo.git/git-upload-pack", http.NoBody)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo.git/git-upload-pack")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "15", w.Header().Get("Retry-After"))
	assert.Contains(t, w.Body.String(), "too many concurrent git clones")
}

func TestIncrementalUploadPackNotGated(t *testing.T) {
	t.Parallel()
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: filepath.Join(t.TempDir(), "clones"),
	}, nil)
	s, err := git.New(ctx, git.Config{
		UploadPackCloneConcurrency: 1,
		UploadPackCloneRetryAfter:  15 * time.Second,
	}, newTestScheduler(ctx, t), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	waitForReady(t, s)

	release, admitted := s.HoldUploadPackCloneSlotForTest(ctx, "https://127.0.0.1/org/other")
	assert.True(t, admitted)
	defer release()

	handler := mux.handlers["POST /git/{host}/{path...}"]
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req := httptest.NewRequestWithContext(reqCtx, http.MethodPost,
		"/git/127.0.0.1/org/repo.git/git-upload-pack",
		bytes.NewReader([]byte("have 0123456789abcdef0123456789abcdef01234567\n")))
	req.SetPathValue("host", "127.0.0.1")
	req.SetPathValue("path", "org/repo.git/git-upload-pack")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.NotEqual(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "", w.Header().Get("Retry-After"))
}
