package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/alecthomas/errors"
)

// SnapshotCommitHeader is the /snapshot.tar.zst response header holding the mirror's HEAD SHA at snapshot time.
//
// An empty value means the snapshot was served cold (no associated mirror) and the caller should freshen the
// working tree itself.
const SnapshotCommitHeader = "X-Cachew-Snapshot-Commit"

// BundleURLHeader is the /snapshot.tar.zst response header pointing at a delta bundle up to the mirror's HEAD.
const BundleURLHeader = "X-Cachew-Bundle-Url"

// EnsureGitRefsRequest specifies what must be present on the server's mirror; Refs or Commits must be non-empty.
//
// Refs maps each ref (e.g. "refs/heads/main") to its expected SHA, where an empty SHA accepts any SHA. Commits
// lists commit SHAs that must exist in the mirror's object database regardless of which ref points at them.
type EnsureGitRefsRequest struct {
	Refs    map[string]string `json:"refs,omitempty"`
	Commits []string          `json:"commits,omitempty"`
}

// EnsureGitRefsResponse is the response returned by EnsureGitRefs.
//
// Refs holds each requested ref's resolved SHA, empty if still missing after the fetch. MissingCommits lists
// requested commits still absent on the server. Fetched reports whether the server fetched from upstream.
type EnsureGitRefsResponse struct {
	Refs           map[string]string `json:"refs,omitempty"`
	MissingCommits []string          `json:"missing_commits,omitempty"`
	Fetched        bool              `json:"fetched"`
}

// EnsureGitRefs makes the server's mirror of repoURL satisfy request, fetching upstream synchronously if needed.
//
// Call it before a git fetch/clone against cachew when fresh refs or specific commits are required, since the
// default ref-check rate-limit window could otherwise serve stale data.
func (c *Client) EnsureGitRefs(ctx context.Context, repoURL string, request EnsureGitRefsRequest) (EnsureGitRefsResponse, error) {
	endpoint, err := gitEndpointURL(c.baseURL, repoURL, "ensure-refs")
	if err != nil {
		return EnsureGitRefsResponse{}, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return EnsureGitRefsResponse{}, errors.Wrap(err, "encode request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return EnsureGitRefsResponse{}, errors.Wrap(err, "create request")
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return EnsureGitRefsResponse{}, errors.Wrap(err, "execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body) //nolint:errcheck
		return EnsureGitRefsResponse{}, errors.Errorf("ensure refs: status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var out EnsureGitRefsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return EnsureGitRefsResponse{}, errors.Wrap(err, "decode response")
	}
	return out, nil
}

// GitSnapshot is a streaming response from OpenGitSnapshot; the caller must Close it.
//
// Body is the zstd-compressed tarball. Commit is the mirror's HEAD SHA at snapshot time (empty for cold serves).
// A non-empty BundleURL names a delta bundle up to the mirror's current HEAD, which OpenGitBundle can fetch.
type GitSnapshot struct {
	Body      io.ReadCloser
	Headers   http.Header
	Commit    string
	BundleURL string
}

// Close releases the underlying response body.
func (s *GitSnapshot) Close() error { return errors.WithStack(s.Body.Close()) }

// OpenGitSnapshot fetches a working-tree snapshot for repoURL from cachew.
//
// The caller extracts the zstd-compressed tarball (e.g. via the snapshot package). Returns os.ErrNotExist when
// the server has no snapshot available.
func (c *Client) OpenGitSnapshot(ctx context.Context, repoURL string) (*GitSnapshot, error) {
	return c.openGitArtifact(ctx, repoURL, "snapshot.tar.zst")
}

// OpenGitLFSSnapshot fetches the LFS-object snapshot for repoURL, or returns os.ErrNotExist if none is cached.
func (c *Client) OpenGitLFSSnapshot(ctx context.Context, repoURL string) (*GitSnapshot, error) {
	return c.openGitArtifact(ctx, repoURL, "lfs-snapshot.tar.zst")
}

// ErrUpToDate is returned by OpenGitBundle when the snapshot already matches upstream HEAD, so no freshen is needed.
var ErrUpToDate = errors.New("snapshot already up to date")

// OpenGitBundle fetches the bundle at a BundleURL from a previous GitSnapshot response.
//
// The caller writes the body to a file and applies it via `git pull`/`git fetch`. Returns ErrUpToDate when the
// snapshot already matches upstream HEAD.
func (c *Client) OpenGitBundle(ctx context.Context, bundleURL string) (io.ReadCloser, error) {
	resolved, err := resolveBundleURL(c.baseURL, bundleURL)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolved, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create bundle request")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "execute bundle request")
	}
	if resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, errors.Join(ErrUpToDate, resp.Body.Close())
	}
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, errors.Join(errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode}), resp.Body.Close())
	}
	return resp.Body, nil
}

func (c *Client) openGitArtifact(ctx context.Context, repoURL, suffix string) (*GitSnapshot, error) {
	endpoint, err := gitEndpointURL(c.baseURL, repoURL, suffix)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "execute request")
	}
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, errors.Join(errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode}), resp.Body.Close())
	}
	return &GitSnapshot{
		Body:      resp.Body,
		Headers:   filterHeaders(resp.Header, transportHeaders...),
		Commit:    resp.Header.Get(SnapshotCommitHeader),
		BundleURL: resp.Header.Get(BundleURLHeader),
	}, nil
}

// OpenGitSnapshotParallel is OpenGitSnapshot using up to concurrency range requests of chunkSize bytes each.
//
// It returns once Commit and BundleURL are known and downloads in the background so extraction overlaps the
// transfer. Close cancels the download. Concurrency 1, or a server without range support, uses one full request.
func (c *Client) OpenGitSnapshotParallel(ctx context.Context, repoURL string, chunkSize int64, concurrency int) (*GitSnapshot, error) {
	if concurrency <= 1 {
		return c.OpenGitSnapshot(ctx, repoURL)
	}
	if err := validateParallelParams(chunkSize, concurrency); err != nil {
		return nil, err
	}
	endpoint, err := gitEndpointURL(c.baseURL, repoURL, "snapshot.tar.zst")
	if err != nil {
		return nil, err
	}
	reader := &gitArtifactRangeReader{client: c, endpoint: endpoint, discovered: make(chan struct{})}

	ctx, cancel := context.WithCancel(ctx)

	buf := newStreamBuffer(chunkSize, concurrency)
	done := make(chan error, 1)
	go func() {
		err := ParallelGet(ctx, reader, NewKey(repoURL), buf, chunkSize, concurrency)
		buf.closeWrite(err)
		done <- errors.Wrap(err, "download snapshot")
	}()

	// A small object can finish before discovered is observed, so select may pick either channel.
	//
	// Treat done as "no snapshot" only if discovery never happened; otherwise Body drains the buffer or the error.
	select {
	case <-reader.discovered:
	case err := <-done:
		if !reader.didDiscover() {
			cancel()
			_ = buf.Close() //nolint:errcheck
			if err == nil {
				return nil, errors.WithStack(os.ErrNotExist)
			}
			return nil, err
		}
	}
	headers := reader.discoveryHeaders()
	return &GitSnapshot{
		Body:      &cancelReadCloser{ReadCloser: buf, cancel: cancel},
		Headers:   headers,
		Commit:    headers.Get(SnapshotCommitHeader),
		BundleURL: headers.Get(BundleURLHeader),
	}, nil
}

// cancelReadCloser also cancels its context on Close, so closing a streaming download stops its goroutine promptly.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return errors.WithStack(c.ReadCloser.Close())
}

// gitArtifactRangeReader adapts a git artifact endpoint to RangeReader, ignoring Key since the URL is the identity.
//
// ParallelGet reports the discovery response's headers via observeDiscovery, because they carry the snapshot's
// freshen metadata that ParallelGet does not surface itself.
type gitArtifactRangeReader struct {
	client   *Client
	endpoint string

	// discovered is closed once the discovery response's headers are recorded.
	discovered chan struct{}

	mu        sync.Mutex
	discovery http.Header
}

func (g *gitArtifactRangeReader) Open(ctx context.Context, _ Key, opts ...RequestOption) (io.ReadCloser, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.endpoint, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create request")
	}
	NewRequestOptions(opts...).applyToRequest(req)

	resp, err := g.client.http.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "execute request")
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, resp.Header, nil
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	case http.StatusRequestedRangeNotSatisfiable:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, resp.Header, errors.Join(ErrRangeNotSatisfiable, resp.Body.Close())
	default:
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(errors.WithStack(&HTTPStatusError{StatusCode: resp.StatusCode}), resp.Body.Close())
	}
}

func (g *gitArtifactRangeReader) didDiscover() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.discovery != nil
}

// observeDiscovery stores the discovery response's headers so their freshen metadata outlives the bodies.
//
// Only the first observation is kept: a later etag-less fallback read must not clobber the
// headers a caller may already hold.
func (g *gitArtifactRangeReader) observeDiscovery(h http.Header) { //nolint:privatedoc // explains why
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.discovery == nil {
		g.discovery = h.Clone()
		close(g.discovered)
	}
}

// discoveryHeaders returns the first response's headers with transport-layer headers stripped.
//
// That response is a 206 for the first chunk only, so Content-Range and Content-Length are
// rewritten to describe the full object streamed on GitSnapshot.Body.
func (g *gitArtifactRangeReader) discoveryHeaders() http.Header { //nolint:privatedoc // documents a non-obvious contract
	g.mu.Lock()
	defer g.mu.Unlock()
	headers := filterHeaders(g.discovery, transportHeaders...)
	if total, ok := parseContentRangeTotal(headers.Get("Content-Range")); ok {
		headers.Del("Content-Range")
		headers.Set("Content-Length", strconv.FormatInt(total, 10))
	}
	return headers
}

func gitEndpointURL(baseURL, repoURL, suffix string) (string, error) {
	parsed, err := url.Parse(repoURL)
	if err != nil {
		return "", errors.Wrap(err, "parse repo URL")
	}
	if parsed.Host == "" {
		return "", errors.Errorf("repo URL %q is missing a host", repoURL)
	}
	repoPath := strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), ".git")
	if repoPath == "" {
		return "", errors.Errorf("repo URL %q is missing a path", repoURL)
	}
	return fmt.Sprintf("%s/git/%s/%s/%s", baseURL, parsed.Host, repoPath, suffix), nil
}

func resolveBundleURL(baseURL, bundleURL string) (string, error) {
	parsed, err := url.Parse(bundleURL)
	if err != nil {
		return "", errors.Wrap(err, "parse bundle URL")
	}
	if parsed.IsAbs() {
		return bundleURL, nil
	}
	if !strings.HasPrefix(bundleURL, "/") {
		bundleURL = "/" + bundleURL
	}
	return baseURL + bundleURL, nil
}
