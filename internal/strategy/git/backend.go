package git

import (
	"bufio"
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

// serveFromBackend serves a Git request using git http-backend.
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

// executeClone performs a git clone --bare operation.
func (s *Strategy) executeClone(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	if err := os.MkdirAll(filepath.Dir(c.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	// #nosec G204 - c.upstreamURL and c.path are controlled by us
	// Configure git for large repositories to avoid network buffer issues
	args := []string{"clone", "--bare"}
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

	logger.DebugContext(ctx, "git clone succeeded", slog.String("output", string(output)))
	return nil
}

// executeFetch performs a git fetch --all operation.
func (s *Strategy) executeFetch(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	// Try to acquire the semaphore
	select {
	case <-c.fetchSem:
		// We acquired the semaphore, perform the fetch
		defer func() {
			// Release the semaphore
			c.fetchSem <- struct{}{}
		}()
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
	default:
		// Semaphore is held by another goroutine, wait for it
		logger.DebugContext(ctx, "Fetch already in progress, waiting")
		select {
		case <-c.fetchSem:
			// Fetch completed by another goroutine, release and return
			c.fetchSem <- struct{}{}
			return nil
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for fetch")
		}
	}

	// #nosec G204 - c.path is controlled by us
	// Configure git for large repositories to avoid network buffer issues
	// Use 'remote update' to properly handle ref updates and pruning
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
// Uses a short-lived cache to avoid excessive ls-remote calls.
func (s *Strategy) ensureRefsUpToDate(ctx context.Context, c *clone) error {
	logger := logging.FromContext(ctx)

	c.mu.Lock()
	// Check if we've done a recent ref check
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

	// Get local refs
	localRefs, err := s.getLocalRefs(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get local refs")
	}

	// Get upstream refs
	upstreamRefs, err := s.getUpstreamRefs(ctx, c)
	if err != nil {
		return errors.Wrap(err, "get upstream refs")
	}

	// Check if upstream has any refs we don't have or refs that have been updated
	// Skip peeled refs (refs ending in ^{}) as they're not real refs
	needsFetch := false
	for ref, upstreamSHA := range upstreamRefs {
		// Skip peeled tag refs like refs/tags/v1.0.0^{}
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		localSHA, exists := localRefs[ref]
		if !exists || localSHA != upstreamSHA {
			logger.DebugContext(ctx, "Upstream ref differs from local",
				slog.String("ref", ref),
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

// getLocalRefs returns a map of ref names to SHAs for the local clone.
func (s *Strategy) getLocalRefs(ctx context.Context, c *clone) (map[string]string, error) {
	// #nosec G204 - c.path is controlled by us
	// Use --head to include HEAD symbolic ref
	cmd, err := gitCommand(ctx, "", "-C", c.path, "show-ref", "--head")
	if err != nil {
		return nil, errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git show-ref")
	}

	return ParseGitRefs(output), nil
}

// getUpstreamRefs returns a map of ref names to SHAs for the upstream repository.
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

// ParseGitRefs parses the output of git show-ref or git ls-remote.
// Format: <SHA> <ref>.
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
