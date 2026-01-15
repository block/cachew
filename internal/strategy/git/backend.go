package git

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/cgi" //nolint:gosec // CVE-2016-5386 only affects Go < 1.6.3
	"os"
	"os/exec"
	"path/filepath"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// serveFromBackend serves a Git request using git http-backend.
func (s *Strategy) serveFromBackend(w http.ResponseWriter, r *http.Request, c *clone) {
	logger := logging.FromContext(r.Context())

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

	// Build the path that git http-backend expects
	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// git http-backend expects the path as-is: /host/repo.git/info/refs
	backendPath := "/" + host + "/" + pathValue

	logger.DebugContext(r.Context(), "Serving with git http-backend",
		slog.String("original_path", r.URL.Path),
		slog.String("backend_path", backendPath),
		slog.String("clone_path", c.path))

	handler := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + absRoot,
			"GIT_HTTP_EXPORT_ALL=1",
			"PATH=" + os.Getenv("PATH"),
		},
	}

	// Modify request for http-backend
	r2 := r.Clone(r.Context())
	r2.URL.Path = backendPath

	handler.ServeHTTP(w, r2)
}

// executeClone performs a git clone --bare --mirror operation.
func (s *Strategy) executeClone(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	if err := os.MkdirAll(filepath.Dir(c.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	// #nosec G204 - c.upstreamURL and c.path are controlled by us
	cmd := exec.CommandContext(ctx, "git", "clone", "--bare", "--mirror", c.upstreamURL, c.path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git clone failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "git clone")
	}

	logger.DebugContext(ctx, "git clone succeeded", slog.String("output", string(output)))
	return nil
}

// executeFetch performs a git fetch --all operation.
func (s *Strategy) executeFetch(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	// #nosec G204 - c.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", c.path, "fetch", "--all")
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git fetch failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "git fetch")
	}

	logger.DebugContext(ctx, "git fetch succeeded", slog.String("output", string(output)))
	return nil
}
