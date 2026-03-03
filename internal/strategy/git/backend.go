package git

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cgi" //nolint:gosec // CVE-2016-5386 only affects Go < 1.6.3
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) serveFromBackend(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	gitPath, err := exec.LookPath("git")
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "git not found in PATH")
		return
	}

	absRoot, err := filepath.Abs(s.cloneManager.Config().MirrorRoot)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to get absolute path")
		return
	}

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	var gitOperation string
	var repoPathWithSuffix string

	for _, op := range []string{"/info/refs", "/git-upload-pack", "/git-receive-pack"} {
		if idx := strings.Index(pathValue, op); idx != -1 {
			repoPathWithSuffix = pathValue[:idx]
			gitOperation = pathValue[idx:]
			break
		}
	}

	repoPath := strings.TrimSuffix(repoPathWithSuffix, ".git")
	backendPath := "/" + host + "/" + repoPath + gitOperation

	logger.DebugContext(r.Context(), "Serving with git http-backend",
		slog.String("original_path", r.URL.Path),
		slog.String("backend_path", backendPath),
		slog.String("clone_path", repo.Path()))

	// No read lock needed: git http-backend (upload-pack) handles concurrent
	// access safely via git's own file-level locking. Holding a read lock here
	// blocks fetches for the entire duration of a clone stream (minutes for
	// large repos), which combined with RWMutex writer-priority semantics
	// causes all subsequent requests to hang.
	var stderrBuf bytes.Buffer

	handler := &cgi.Handler{
		Path:   gitPath,
		Args:   []string{"http-backend"},
		Stderr: &stderrBuf,
		Env: []string{
			"GIT_PROJECT_ROOT=" + absRoot,
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_HTTP_MAX_REQUEST_BUFFER=100M",
			"PATH=" + os.Getenv("PATH"),
		},
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = backendPath

	// Go's cgi.Handler rejects chunked request bodies with a 400.
	// Buffer the body so we can set ContentLength and clear TransferEncoding.
	if r2.ContentLength < 0 {
		body, err := io.ReadAll(r2.Body)
		if err != nil {
			httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to read request body")
			return
		}
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
		r2.TransferEncoding = nil
	}

	handler.ServeHTTP(w, r2)

	if stderrBuf.Len() > 0 {
		logger.ErrorContext(r.Context(), "git http-backend error",
			slog.String("stderr", stderrBuf.String()),
			slog.String("path", backendPath))
	}
}

func (s *Strategy) ensureRefsUpToDate(ctx context.Context, repo *gitclone.Repository) {
	logger := logging.FromContext(ctx)

	needsFetch, err := repo.EnsureRefsUpToDate(ctx)
	if err != nil {
		logger.WarnContext(ctx, "Failed to check upstream refs",
			slog.String("error", err.Error()))
		return
	}
	if needsFetch {
		logger.DebugContext(ctx, "Refs stale, scheduling background fetch",
			slog.String("upstream", repo.UpstreamURL()))
		s.scheduler.Submit(repo.UpstreamURL(), "fetch", func(ctx context.Context) error {
			s.backgroundFetch(ctx, repo)
			return nil
		})
	}
}
