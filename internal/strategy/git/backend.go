package git

import (
	"bytes"
	"context"
	"io"
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

// bufferedResponseWriter buffers a response until it exceeds a threshold, then streams it through.
//
// If the threshold is never exceeded, the caller must call commit() or the response is discarded,
// which allows a clean fallback to a different handler.
type bufferedResponseWriter struct {
	w         http.ResponseWriter
	headers   http.Header
	buf       bytes.Buffer
	code      int
	committed bool
	threshold int
}

func newBufferedResponseWriter(w http.ResponseWriter, threshold int) *bufferedResponseWriter {
	return &bufferedResponseWriter{
		w:         w,
		headers:   make(http.Header),
		code:      http.StatusOK,
		threshold: threshold,
	}
}

func (b *bufferedResponseWriter) Header() http.Header {
	if b.committed {
		return b.w.Header()
	}
	return b.headers
}

func (b *bufferedResponseWriter) WriteHeader(code int) {
	if !b.committed {
		b.code = code
	}
}

func (b *bufferedResponseWriter) Write(p []byte) (int, error) {
	if b.committed {
		return b.w.Write(p) //nolint:wrapcheck
	}
	if b.buf.Len()+len(p) >= b.threshold {
		b.commit()
		return b.w.Write(p) //nolint:wrapcheck
	}
	n, _ := b.buf.Write(p) // bytes.Buffer.Write never returns an error
	return n, nil
}

func (b *bufferedResponseWriter) commit() {
	if b.committed {
		return
	}
	b.committed = true
	for k, vs := range b.headers {
		for _, v := range vs {
			b.w.Header().Add(k, v)
		}
	}
	b.w.WriteHeader(b.code)
	_, _ = io.Copy(b.w, &b.buf) //nolint:errcheck
}

// serveFromBackend serves the request using git http-backend against the local mirror.
//
// It returns true when upload-pack rejects the request with "not our ref" and it should be retried upstream.
// This typically happens when a concurrent force-push fetch orphans a previously advertised commit.
func (s *Strategy) serveFromBackend(w http.ResponseWriter, r *http.Request, repo *gitclone.Repository) bool {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	gitPath, err := exec.LookPath("git")
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "git not found in PATH")
		return false
	}

	absRoot, err := filepath.Abs(s.cloneManager.Config().MirrorRoot)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to get absolute path")
		return false
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

	logger.DebugContext(r.Context(), "Serving with git http-backend", "original_path", r.URL.Path,
		"backend_path", backendPath, "clone_path", repo.Path())

	// No read lock: git http-backend handles concurrent access via git's own file-level locking.
	//
	// A read lock would block fetches for the whole clone stream (minutes for large repos), and
	// RWMutex writer priority would then hang all subsequent requests.

	// Buffer up to 1 MB so a "not our ref" error (always <1 KB) can be suppressed before reaching the client.
	//
	// This allows a clean fallback to upstream. Large successful pack responses stream through normally.
	const fallbackThreshold = 1 << 20 // 1 MB
	bw := newBufferedResponseWriter(w, fallbackThreshold)

	var stderrBuf bytes.Buffer

	// Host Git configuration is unrelated to serving mirrors and can add command-backed upload-pack behavior.
	handler := &cgi.Handler{
		Path:   gitPath,
		Args:   []string{"http-backend"},
		Stderr: &stderrBuf,
		Env: []string{
			"GIT_PROJECT_ROOT=" + absRoot,
			"GIT_HTTP_EXPORT_ALL=1",
			"GIT_HTTP_MAX_REQUEST_BUFFER=100M",
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_CONFIG_GLOBAL=" + os.DevNull,
			"PATH=" + os.Getenv("PATH"),
		},
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = backendPath

	// Go's cgi.Handler rejects chunked request bodies with a 400, so buffer the body to set ContentLength.
	if r2.ContentLength < 0 {
		body, err := io.ReadAll(r2.Body)
		if err != nil {
			httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to read request body")
			return false
		}
		r2.Body = io.NopCloser(bytes.NewReader(body))
		r2.ContentLength = int64(len(body))
		r2.TransferEncoding = nil
	}

	handler.ServeHTTP(bw, r2)

	if stderrBuf.Len() > 0 {
		stderr := stderrBuf.String()
		logger.ErrorContext(r.Context(), "git http-backend error", "stderr", stderr, "path", backendPath)
		if !bw.committed && strings.Contains(stderr, "not our ref") {
			return true
		}
	}

	bw.commit()
	return false
}

func (s *Strategy) checkRefsStale(ctx context.Context, repo *gitclone.Repository) (bool, error) {
	needsFetch, err := repo.EnsureRefsUpToDate(ctx)
	if err != nil {
		return false, errors.Wrap(err, "check upstream refs")
	}
	return needsFetch, nil
}
