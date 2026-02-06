package git

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/cgi" //nolint:gosec // CVE-2016-5386 only affects Go < 1.6.3
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/alecthomas/errors"

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

	absRoot, err := filepath.Abs(s.config.MirrorRoot)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to get absolute path")
		return
	}

	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// Insert /.git before the git protocol paths to match the filesystem layout
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
	backendPath := "/" + host + "/" + repoPath + "/.git" + gitOperation

	logger.DebugContext(r.Context(), "Serving with git http-backend",
		slog.String("original_path", r.URL.Path),
		slog.String("backend_path", backendPath),
		slog.String("clone_path", repo.Path()))

	repo.WithReadLock(func() error { //nolint:errcheck,gosec
		var stderrBuf bytes.Buffer

		handler := &cgi.Handler{
			Path:   gitPath,
			Args:   []string{"http-backend"},
			Stderr: &stderrBuf,
			Env: []string{
				"GIT_PROJECT_ROOT=" + absRoot,
				"GIT_HTTP_EXPORT_ALL=1",
				"PATH=" + os.Getenv("PATH"),
			},
		}

		r2 := r.Clone(r.Context())
		r2.URL.Path = backendPath

		handler.ServeHTTP(w, r2)

		if stderrBuf.Len() > 0 {
			logger.ErrorContext(r.Context(), "git http-backend error",
				slog.String("stderr", stderrBuf.String()),
				slog.String("path", backendPath))
		}

		return nil
	})
}

func (s *Strategy) ensureRefsUpToDate(ctx context.Context, repo *gitclone.Repository) error {
	gitcloneConfig := gitclone.Config{
		RootDir:          s.config.MirrorRoot,
		FetchInterval:    s.config.FetchInterval,
		RefCheckInterval: s.config.RefCheckInterval,
		GitConfig:        gitclone.DefaultGitTuningConfig(),
	}
	if err := repo.EnsureRefsUpToDate(ctx, gitcloneConfig); err != nil {
		return errors.Wrap(err, "ensure refs up to date")
	}
	return nil
}
