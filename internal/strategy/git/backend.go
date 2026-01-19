package git

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/cgi" //nolint:gosec // CVE-2016-5386 only affects Go < 1.6.3
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) serveFromBackend(w http.ResponseWriter, r *http.Request, c *clone) {
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
		slog.String("clone_path", c.path))

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
}

func (s *Strategy) executeClone(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	if err := os.MkdirAll(filepath.Dir(c.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	// Configure git for large repositories to avoid network buffer issues
	// #nosec G204 - c.upstreamURL and c.path are controlled by us
	args := []string{"clone"}
	if s.config.CloneDepth > 0 {
		args = append(args, "--depth", strconv.Itoa(s.config.CloneDepth))
	}
	args = append(args,
		"-c", "http.postBuffer=524288000", // 500MB buffer
		"-c", "http.lowSpeedLimit=1000", // 1KB/s minimum speed
		"-c", "http.lowSpeedTime=600", // 10 minute timeout at low speed
		c.upstreamURL, c.path)
	cmd, err := gitCommand(ctx, c.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git clone failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "git clone")
	}

	// git clone only sets up fetching for the default branch, change it to fetch all branches
	// #nosec G204 - c.path is controlled by us
	cmd = exec.CommandContext(ctx, "git", "-C", c.path, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*")
	output, err = cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git config failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "configure fetch refspec")
	}

	cmd, err = gitCommand(ctx, c.upstreamURL, "-C", c.path,
		"-c", "http.postBuffer=524288000",
		"-c", "http.lowSpeedLimit=1000",
		"-c", "http.lowSpeedTime=600",
		"fetch", "--all")
	if err != nil {
		return errors.Wrap(err, "create git command for fetch")
	}
	output, err = cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git fetch --all failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "fetch all branches")
	}

	return nil
}

func (s *Strategy) executeFetch(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	select {
	case <-c.fetchSem:
		defer func() {
			c.fetchSem <- struct{}{}
		}()
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
	default:
		logger.DebugContext(ctx, "Fetch already in progress, waiting")
		select {
		case <-c.fetchSem:
			c.fetchSem <- struct{}{}
			return nil
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for fetch")
		}
	}

	// Configure git for large repositories to avoid network buffer issues
	// #nosec G204 - c.path is controlled by us
	cmd, err := gitCommand(ctx, c.upstreamURL, "-C", c.path,
		"-c", "http.postBuffer=524288000", // 500MB buffer
		"-c", "http.lowSpeedLimit=1000", // 1KB/s minimum speed
		"-c", "http.lowSpeedTime=600", // 10 minute timeout at low speed
		"remote", "update", "--prune")
	if err != nil {
		logger.ErrorContext(ctx, "Failed to create git command",
			slog.String("upstream", c.upstreamURL),
			slog.String("error", err.Error()))
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.ErrorContext(ctx, "git remote update failed",
			slog.String("error", err.Error()),
			slog.String("output", string(output)))
		return errors.Wrap(err, "git remote update")
	}

	logger.DebugContext(ctx, "git remote update succeeded", slog.String("output", string(output)))
	return nil
}

// ensureRefsUpToDate checks if upstream has refs we don't have and fetches if needed.
// Short-lived cache avoids excessive ls-remote calls.
func (s *Strategy) ensureRefsUpToDate(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	c.mu.Lock()
	if c.refCheckValid && time.Since(c.lastRefCheck) < s.config.RefCheckInterval {
		c.mu.Unlock()
		logger.DebugContext(ctx, "Skipping ref check, recently checked",
			slog.Duration("since_last_check", time.Since(c.lastRefCheck)))
		return nil
	}
	c.lastRefCheck = time.Now()
	c.refCheckValid = true
	c.mu.Unlock()

	logger.DebugContext(ctx, "Checking upstream for new refs",
		slog.String("upstream", c.upstreamURL))

	localRefs, err := s.getLocalRefs(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get local refs")
	}

	upstreamRefs, err := s.getUpstreamRefs(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get upstream refs")
	}

	needsFetch := false
	for ref, upstreamSHA := range upstreamRefs {
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		// Only check refs/heads/* since GitHub exposes refs/pull/* and other refs we don't fetch
		if !strings.HasPrefix(ref, "refs/heads/") {
			continue
		}
		localRef := "refs/remotes/origin/" + strings.TrimPrefix(ref, "refs/heads/")
		localSHA, exists := localRefs[localRef]
		if !exists || localSHA != upstreamSHA {
			logger.DebugContext(ctx, "Upstream ref differs from local",
				slog.String("upstream_ref", ref),
				slog.String("local_ref", localRef),
				slog.String("upstream_sha", upstreamSHA),
				slog.String("local_sha", localSHA))
			needsFetch = true
			break
		}
	}

	if !needsFetch {
		c.mu.Lock()
		c.refCheckValid = true
		c.mu.Unlock()
		logger.DebugContext(ctx, "No upstream changes detected")
		return nil
	}

	logger.InfoContext(ctx, "Upstream has new or updated refs, fetching")
	err = s.executeFetch(ctx, c)
	if err != nil {
		c.mu.Lock()
		c.refCheckValid = false
		c.mu.Unlock()
	}
	return err
}

func (s *Strategy) getLocalRefs(ctx context.Context, c *clone) (map[string]string, error) {
	// #nosec G204 - c.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", c.path, "for-each-ref", "--format=%(objectname) %(refname)")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git for-each-ref")
	}

	return ParseGitRefs(output), nil
}

func (s *Strategy) getUpstreamRefs(ctx context.Context, c *clone) (map[string]string, error) {
	// #nosec G204 - c.upstreamURL is controlled by us
	cmd, err := gitCommand(ctx, c.upstreamURL, "ls-remote", c.upstreamURL)
	if err != nil {
		return nil, errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git ls-remote")
	}

	return ParseGitRefs(output), nil
}

// ParseGitRefs parses the output of git show-ref or git ls-remote (format: <SHA> <ref>).
func ParseGitRefs(output []byte) map[string]string {
	refs := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			sha := parts[0]
			ref := parts[1]
			refs[ref] = sha
		}
	}
	return refs
}
