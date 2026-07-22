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

// SnapshotCommitHeader is the response header on /snapshot.tar.zst that
// contains the mirror's HEAD SHA at the time the snapshot was generated. An
// empty value means the snapshot was served cold (no associated mirror) and
// the caller should freshen the working tree itself.
const SnapshotCommitHeader = "X-Cachew-Snapshot-Commit"

// BundleURLHeader is the response header on /snapshot.tar.zst that, when
// present, points at a delta bundle that brings the snapshot up to the
// mirror's current HEAD.
const BundleURLHeader = "X-Cachew-Bundle-Url"

// EnsureGitRefsRequest specifies what the caller wants present on the
// server's mirror. At least one of Refs or Commits must be non-empty.
//
// Refs maps each required ref (e.g. "refs/heads/main") to the expected SHA;
// an empty SHA means "require the ref to exist at any SHA". Commits lists
// individual commit SHAs that must exist in the mirror's object database,
// regardless of which ref points at them.
type EnsureGitRefsRequest struct {
	Refs    map[string]string `json:"refs,omitempty"`
	Commits []string          `json:"commits,omitempty"`
}

// EnsureGitRefsResponse is the response returned by EnsureGitRefs.
//
// Refs contains the resolved local SHA for each requested ref (empty if the
// ref is still missing on the server after the fetch). MissingCommits lists
// the requested commits that are still absent from the server's object
// database. Fetched reports whether the server performed an upstream fetch.
type EnsureGitRefsResponse struct {
	Refs           map[string]string `json:"refs,omitempty"`
	MissingCommits []string          `json:"missing_commits,omitempty"`
	Fetched        bool              `json:"fetched"`
}

// EnsureGitRefs asks the cachew server to ensure its local mirror of repoURL
// satisfies the request before the caller fetches. The server synchronously
// fetches from upstream if any requested ref is missing/stale or any
// requested commit is absent from its object database.
//
// Use this before issuing a git fetch/clone against cachew when fresh refs
// or specific commits are required and the default ref-check rate-limit
// window would otherwise allow stale data to be served.
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

// GitSnapshot is a streaming response from OpenGitSnapshot.
//
// Body is the zstd-compressed tarball; the caller must Close it. Commit holds
// the mirror's HEAD SHA at snapshot time (empty for cold serves). BundleURL,
// if non-empty, identifies a delta bundle that brings the snapshot up to the
// mirror's current HEAD; it can be passed to OpenGitBundle.
type GitSnapshot struct {
	Body      io.ReadCloser
	Headers   http.Header
	Commit    string
	BundleURL string
}

// Close releases the underlying response body.
func (s *GitSnapshot) Close() error { return errors.WithStack(s.Body.Close()) }

// OpenGitSnapshot fetches a working-tree snapshot for repoURL from cachew.
// The caller is responsible for extracting the returned zstd-compressed
// tarball (e.g. via the snapshot package). Returns os.ErrNotExist when the
// server has no snapshot available.
func (c *Client) OpenGitSnapshot(ctx context.Context, repoURL string) (*GitSnapshot, error) {
	return c.openGitArtifact(ctx, repoURL, "snapshot.tar.zst")
}

// OpenGitLFSSnapshot fetches the LFS-object snapshot for repoURL. Returns
// os.ErrNotExist when the server has no LFS snapshot cached.
func (c *Client) OpenGitLFSSnapshot(ctx context.Context, repoURL string) (*GitSnapshot, error) {
	return c.openGitArtifact(ctx, repoURL, "lfs-snapshot.tar.zst")
}

// ErrUpToDate is returned by OpenGitBundle when the server reports the
// snapshot already matches upstream HEAD, so there is nothing to fetch and no
// freshen is needed.
var ErrUpToDate = errors.New("snapshot already up to date")

// OpenGitBundle fetches the bundle pointed at by a BundleURL returned in a
// previous GitSnapshot response. The caller is responsible for writing the
// body to a file and applying it via `git pull`/`git fetch`. Returns
// ErrUpToDate when the snapshot already matches upstream HEAD.
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

// OpenGitSnapshotParallel downloads the working-tree snapshot for repoURL with
// up to concurrency concurrent range requests of chunkSize bytes each. It
// returns as soon as the freshen metadata (Commit, BundleURL) is available,
// with the download continuing in the background as the caller reads Body, so
// extraction overlaps the transfer.
//
// The caller must Close the returned GitSnapshot, which cancels any in-flight
// download. A concurrency of 1 streams a single plain request with no
// buffering; a server without range support falls back to a single full
// download. Returns os.ErrNotExist when the server has no snapshot.
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

	// A small object can finish before discovered is observed, leaving both
	// channels ready and select picking at random; treat done as "no snapshot"
	// only when discovery never happened, otherwise let Body drain the buffered
	// bytes or surface the download error.
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

// cancelReadCloser cancels the supplied context when Closed, in addition to
// closing the wrapped reader, so closing a streaming download stops its
// background goroutine promptly.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return errors.WithStack(c.ReadCloser.Close())
}

// gitArtifactRangeReader adapts a git artifact endpoint to the RangeReader
// interface. The object's identity is the endpoint URL, so the Key argument is
// ignored. ParallelGet reports the accepted discovery response's headers via
// observeDiscovery, which carry the snapshot's freshen metadata it does not
// surface itself.
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

// didDiscover reports whether the first response's headers have been recorded.
func (g *gitArtifactRangeReader) didDiscover() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.discovery != nil
}

// observeDiscovery stores the accepted discovery response's headers so the
// freshen metadata they carry survives after the bodies are consumed. Only the
// first observation is kept: a later etag-less fallback read must not clobber
// the headers a caller may already hold.
func (g *gitArtifactRangeReader) observeDiscovery(h http.Header) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.discovery == nil {
		g.discovery = h.Clone()
		close(g.discovered)
	}
}

// discoveryHeaders returns the first response's headers with transport-layer
// headers stripped. The discovery response is a 206 for only the first chunk,
// so Content-Range/Content-Length are rewritten to describe the full
// reassembled object streamed on GitSnapshot.Body.
func (g *gitArtifactRangeReader) discoveryHeaders() http.Header {
	g.mu.Lock()
	defer g.mu.Unlock()
	headers := filterHeaders(g.discovery, transportHeaders...)
	if total, ok := parseContentRangeTotal(headers.Get("Content-Range")); ok {
		headers.Del("Content-Range")
		headers.Set("Content-Length", strconv.FormatInt(total, 10))
	}
	return headers
}

// gitEndpointURL builds a /git/{host}/{repoPath}/{suffix} URL from a cachew
// base URL and an upstream repository URL (e.g. https://github.com/org/repo).
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

// resolveBundleURL joins an X-Cachew-Bundle-Url header value (which may be
// absolute or root-relative) onto the client's base URL.
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
