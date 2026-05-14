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
	"strings"

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

// EnsureGitRefsResponse is the response returned by EnsureGitRefs.
//
// Refs contains the resolved local SHA for each requested ref (empty if the
// ref is still missing on the server after the fetch). Fetched reports
// whether the server performed an upstream fetch to satisfy the request.
type EnsureGitRefsResponse struct {
	Refs    map[string]string `json:"refs"`
	Fetched bool              `json:"fetched"`
}

// EnsureGitRefs asks the cachew server to ensure its local mirror of repoURL
// contains the listed refs at the given SHAs before the caller fetches. An
// empty SHA means "require the ref to exist, at any SHA". The server will
// synchronously fetch from upstream if any requested ref is missing or stale.
//
// Use this before issuing a git fetch/clone against cachew when fresh refs
// are required and the default ref-check rate-limit window would otherwise
// allow stale refs to be served.
func (c *Client) EnsureGitRefs(ctx context.Context, repoURL string, refs map[string]string) (EnsureGitRefsResponse, error) {
	endpoint, err := gitEndpointURL(c.baseURL, repoURL, "ensure-refs")
	if err != nil {
		return EnsureGitRefsResponse{}, err
	}

	body, err := json.Marshal(struct {
		Refs map[string]string `json:"refs"`
	}{Refs: refs})
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

// OpenGitBundle fetches the bundle pointed at by a BundleURL returned in a
// previous GitSnapshot response. The caller is responsible for writing the
// body to a file and applying it via `git pull`/`git fetch`.
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
