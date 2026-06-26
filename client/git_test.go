package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

func TestEnsureGitRefs(t *testing.T) {
	var receivedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/git/github.com/org/repo/ensure-refs", r.URL.Path)
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&receivedBody))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"refs":            map[string]string{"refs/heads/main": "abc123"},
			"missing_commits": []string{"deadbeef"},
			"fetched":         true,
		}))
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	resp, err := api.EnsureGitRefs(context.Background(),
		"https://github.com/org/repo",
		client.EnsureGitRefsRequest{
			Refs:    map[string]string{"refs/heads/main": ""},
			Commits: []string{"abc", "deadbeef"},
		})
	assert.NoError(t, err)
	assert.True(t, resp.Fetched)
	assert.Equal(t, "abc123", resp.Refs["refs/heads/main"])
	assert.Equal(t, []string{"deadbeef"}, resp.MissingCommits)

	refs, ok := receivedBody["refs"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "", refs["refs/heads/main"])

	commits, ok := receivedBody["commits"].([]any)
	assert.True(t, ok)
	assert.Equal(t, 2, len(commits))
}

func TestEnsureGitRefsCommitsOnly(t *testing.T) {
	var receivedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&receivedBody))
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"fetched": false,
		}))
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	resp, err := api.EnsureGitRefs(context.Background(),
		"https://github.com/org/repo",
		client.EnsureGitRefsRequest{Commits: []string{"abc"}})
	assert.NoError(t, err)
	assert.False(t, resp.Fetched)
	assert.Equal(t, 0, len(resp.MissingCommits))

	_, ok := receivedBody["refs"]
	assert.False(t, ok, "refs field should be omitted when empty")
}

func TestEnsureGitRefsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	_, err := api.EnsureGitRefs(context.Background(),
		"https://github.com/org/repo",
		client.EnsureGitRefsRequest{Refs: map[string]string{"refs/heads/main": ""}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 400")
}

func TestEnsureGitRefsInvalidRepoURL(t *testing.T) {
	api := client.New("http://example.com", nil)

	_, err := api.EnsureGitRefs(context.Background(), "not-a-url", client.EnsureGitRefsRequest{})
	assert.Error(t, err)

	_, err = api.EnsureGitRefs(context.Background(), "https://github.com/", client.EnsureGitRefsRequest{})
	assert.Error(t, err)
}

func TestOpenGitSnapshot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			assert.Equal(t, "/git/github.com/org/repo/snapshot.tar.zst", r.URL.Path)
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set(client.SnapshotCommitHeader, "deadbeef")
			w.Header().Set(client.BundleURLHeader, "/git/github.com/org/repo/snapshot.bundle?base=deadbeef")
			_, _ = w.Write([]byte("snapshot-bytes")) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/lfs-snapshot.tar.zst"):
			http.NotFound(w, r)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())

	snap, err := api.OpenGitSnapshot(context.Background(), "https://github.com/org/repo")
	assert.NoError(t, err)
	defer snap.Close()

	assert.Equal(t, "deadbeef", snap.Commit)
	assert.Equal(t, "/git/github.com/org/repo/snapshot.bundle?base=deadbeef", snap.BundleURL)
	body, err := io.ReadAll(snap.Body)
	assert.NoError(t, err)
	assert.Equal(t, "snapshot-bytes", string(body))

	_, err = api.OpenGitLFSSnapshot(context.Background(), "https://github.com/org/repo")
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestOpenGitBundle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/git/github.com/org/repo/snapshot.bundle", r.URL.Path)
		assert.Equal(t, "abc", r.URL.Query().Get("base"))
		w.Header().Set("Content-Type", "application/x-git-bundle")
		_, _ = w.Write([]byte("bundle-bytes")) //nolint:errcheck
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())

	// Root-relative URL (as returned in X-Cachew-Bundle-Url).
	body, err := api.OpenGitBundle(context.Background(),
		"/git/github.com/org/repo/snapshot.bundle?base=abc")
	assert.NoError(t, err)
	data, err := io.ReadAll(body)
	assert.NoError(t, err)
	assert.NoError(t, body.Close())
	assert.Equal(t, "bundle-bytes", string(data))

	// Absolute URL.
	body, err = api.OpenGitBundle(context.Background(),
		srv.URL+"/git/github.com/org/repo/snapshot.bundle?base=abc")
	assert.NoError(t, err)
	assert.NoError(t, body.Close())
}

func TestOpenGitBundleNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	_, err := api.OpenGitBundle(context.Background(), "/git/x/y/snapshot.bundle")
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestDownloadGitSnapshotParallel(t *testing.T) {
	body := make([]byte, 1000)
	for i := range body {
		body[i] = byte(i % 251)
	}
	const etag = `"snap-v1"`

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/git/github.com/org/repo/snapshot.tar.zst", r.URL.Path)
		requests.Add(1)
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("ETag", etag)
		w.Header().Set(client.SnapshotCommitHeader, "deadbeef")
		w.Header().Set(client.BundleURLHeader, "/git/github.com/org/repo/snapshot.bundle?base=deadbeef")
		// ServeContent honours Range/If-Range against the ETag set above, so it
		// returns 206 + Content-Range for the chunked requests ParallelGet makes.
		http.ServeContent(w, r, "snapshot.tar.zst", time.Time{}, bytes.NewReader(body))
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	var dst bufferAt
	// A 128-byte chunk over a 1000-byte body forces multiple chunks, exercising
	// concurrent range reassembly.
	meta, err := api.DownloadGitSnapshot(context.Background(), "https://github.com/org/repo", &dst, 128, 4)
	assert.NoError(t, err)
	assert.Equal(t, body, dst.buf)
	assert.Equal(t, "deadbeef", meta.Commit)
	assert.Equal(t, "/git/github.com/org/repo/snapshot.bundle?base=deadbeef", meta.BundleURL)
	assert.True(t, requests.Load() > 1, "expected multiple range requests, got %d", requests.Load())
}

func TestDownloadGitSnapshotFallsBackWithoutRange(t *testing.T) {
	body := []byte("full body, server ignores ranges")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// No ETag and no range handling: always answer the full object with 200,
		// mimicking an older server. ParallelGet must fall back to a single read.
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set(client.SnapshotCommitHeader, "cafe")
		_, _ = w.Write(body) //nolint:errcheck
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	var dst bufferAt
	meta, err := api.DownloadGitSnapshot(context.Background(), "https://github.com/org/repo", &dst, 8, 4)
	assert.NoError(t, err)
	assert.Equal(t, body, dst.buf)
	assert.Equal(t, "cafe", meta.Commit)
}

func TestDownloadGitSnapshotNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	var dst bufferAt
	_, err := api.DownloadGitSnapshot(context.Background(), "https://github.com/org/repo", &dst, 8, 4)
	assert.True(t, errors.Is(err, os.ErrNotExist))
}
