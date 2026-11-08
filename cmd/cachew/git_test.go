package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	gitstrategy "github.com/block/cachew/internal/strategy/git"
)

// TestMain drops the GIT_* variables git exports under hooks so test git
// commands target their temp dirs, not the ambient repository.
func TestMain(m *testing.M) {
	for _, v := range []string{
		"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
		"GIT_COMMON_DIR", "GIT_PREFIX", "GIT_NAMESPACE",
	} {
		_ = os.Unsetenv(v)
	}
	os.Exit(m.Run())
}

func initGitRepo(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...) //nolint:gosec
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(out))
	}

	run("init", "-b", "main")
	for name, content := range files {
		path := filepath.Join(dir, name)
		assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		assert.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	run("add", "-A")
	run("commit", "-m", "initial")
}

func createTarZst(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tarCmd := exec.Command("tar", "-cpf", "-", "-C", dir, ".")
	zstdCmd := exec.Command("zstd", "-c")

	tarOut, err := tarCmd.StdoutPipe()
	assert.NoError(t, err)
	zstdCmd.Stdin = tarOut
	zstdCmd.Stdout = &buf

	assert.NoError(t, tarCmd.Start())
	assert.NoError(t, zstdCmd.Start())
	assert.NoError(t, tarCmd.Wait())
	assert.NoError(t, zstdCmd.Wait())
	return buf.Bytes()
}

func gitRevParse(t *testing.T, dir, ref string) string { //nolint:unparam // helper accepts any ref
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "rev-parse", ref).Output() //nolint:gosec
	assert.NoError(t, err)
	return strings.TrimSpace(string(out))
}

func createBundle(t *testing.T, dir, baseCommit string) []byte {
	t.Helper()
	bundlePath := filepath.Join(t.TempDir(), "delta.bundle")
	// Use refs/heads/main (not HEAD) to match server behaviour.
	cmd := exec.Command("git", "-C", dir, "bundle", "create", bundlePath, "refs/heads/main", "^"+baseCommit) //nolint:gosec
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(out))

	data, err := os.ReadFile(bundlePath)
	assert.NoError(t, err)
	return data
}

func TestGitRestoreSnapshot(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{
		"hello.txt":         "hello world",
		"subdir/nested.txt": "nested content",
	})

	snapshotData := createTarZst(t, srcDir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst") {
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	cmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err := cmd.Run(context.Background(), api)
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(content))

	content, err = os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "nested content", string(content))
}

func TestGitRestoreSnapshotParallel(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{
		"hello.txt":         "hello world",
		"subdir/nested.txt": "nested content",
	})
	snapshotData := createTarZst(t, srcDir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst") {
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("ETag", `"snap-v1"`)
			// ServeContent honours Range/If-Range against the ETag, so ParallelGet
			// fetches the snapshot in concurrent chunks.
			http.ServeContent(w, r, "snapshot.tar.zst", time.Time{}, bytes.NewReader(snapshotData))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	// A nested, not-yet-existing target exercises temp-dir creation on the
	// target filesystem.
	dstDir := filepath.Join(t.TempDir(), "nested", "restored")
	cmd := &GitRestoreCmd{
		RepoURL:             "https://github.com/test/repo",
		Directory:           dstDir,
		DownloadConcurrency: 4,
		DownloadChunkSizeMB: 8,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	assert.NoError(t, cmd.Run(context.Background(), api))

	content, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(content))
	content, err = os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "nested content", string(content))

	// The temp snapshot is staged on the target filesystem and cleaned up.
	entries, err := os.ReadDir(filepath.Dir(dstDir))
	assert.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), ".cachew-snapshot-"), "temp snapshot left behind: %s", e.Name())
	}
}

func TestGitRestoreIntoExistingDirectory(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"hello.txt": "hello world"})
	snapshotData := createTarZst(t, srcDir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst") {
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	assert.NoError(t, os.MkdirAll(dstDir, 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(dstDir, "existing.txt"), []byte("keep me"), 0o600))

	cmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	assert.NoError(t, cmd.Run(context.Background(), api))

	content, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(content))
	content, err = os.ReadFile(filepath.Join(dstDir, "existing.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "keep me", string(content))
}

func TestGitRestoreExtractStagesTrailingSlashTarget(t *testing.T) {
	dstDir := filepath.Join(t.TempDir(), "restored")
	cmd := &GitRestoreCmd{Directory: dstDir + string(filepath.Separator)}
	err := cmd.extract(context.Background(), strings.NewReader("not a zstd stream"))
	assert.Error(t, err)
	_, err = os.Stat(dstDir)
	assert.True(t, os.IsNotExist(err), "failed restore must not create the target directory")
}

func TestGitRestoreWithBundle(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	baseCommit := gitRevParse(t, srcDir, "HEAD")

	snapshotData := createTarZst(t, srcDir)

	// Add a second commit for the bundle.
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("v2"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(srcDir, "new.txt"), []byte("new"), 0o644))
	cmd := exec.Command("git", "-C", srcDir, "add", "-A")
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test.com")
	out, err := cmd.CombinedOutput()
	assert.NoError(t, err, string(out))

	cmd = exec.Command("git", "-C", srcDir, "commit", "-m", "update")
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test.com")
	out, err = cmd.CombinedOutput()
	assert.NoError(t, err, string(out))

	bundleData := createBundle(t, srcDir, baseCommit)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("X-Cachew-Bundle-Url", "/git/github.com/test/repo/snapshot.bundle?base="+baseCommit)
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/snapshot.bundle"):
			assert.Equal(t, baseCommit, r.URL.Query().Get("base"))
			w.Header().Set("Content-Type", "application/x-git-bundle")
			w.Write(bundleData) //nolint:errcheck

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err = restoreCmd.Run(context.Background(), api)
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(content))

	content, err = os.ReadFile(filepath.Join(dstDir, "new.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "new", string(content))
}

func TestGitRestoreNoBundle(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})

	snapshotData := createTarZst(t, srcDir)

	bundleRequested := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("X-Cachew-Bundle-Url", "/git/github.com/test/repo/snapshot.bundle?base=abc")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/snapshot.bundle"):
			bundleRequested = true
			http.NotFound(w, r)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err := restoreCmd.Run(context.Background(), api)
	assert.NoError(t, err)
	assert.False(t, bundleRequested)

	content, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(content))
}

func TestGitRestoreSnapshotNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err := restoreCmd.Run(context.Background(), api)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no snapshot available")
}

func TestGitRestoreWithRef(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	snapshotData := createTarZst(t, srcDir)

	var ensureCalled atomic.Bool
	var snapshotServedAt atomic.Int64
	var ensurePayload map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			snapshotServedAt.Store(time.Now().UnixNano())
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			ensureCalled.Store(true)
			if snapshotServedAt.Load() == 0 {
				t.Error("ensure-refs must run after snapshot")
			}
			if r.Method != http.MethodPost {
				t.Errorf("ensure-refs: want POST, got %s", r.Method)
			}
			if err := json.NewDecoder(r.Body).Decode(&ensurePayload); err != nil {
				t.Errorf("decode ensure-refs body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"refs":    map[string]string{"refs/heads/main": "abc123"},
				"fetched": true,
			})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Ref:       map[string]string{"refs/heads/main": "abc123"},
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	// The restored snapshot has no origin remote, so the post-pull will fail.
	// That's expected; we just want to verify ensure-refs ran after snapshot.
	err := restoreCmd.Run(context.Background(), api)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "git pull")
	assert.True(t, ensureCalled.Load())

	refs, ok := ensurePayload["refs"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "abc123", refs["refs/heads/main"])
}

func TestGitRestoreSkipsEnsureRefsWhenLocalHasSHA(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	localSHA := gitRevParse(t, srcDir, "HEAD")

	snapshotData := createTarZst(t, srcDir)

	var ensureCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			ensureCalled.Store(true)
			http.Error(w, "should not be called", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Ref:       map[string]string{"refs/heads/main": localSHA},
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	assert.NoError(t, restoreCmd.Run(context.Background(), api))
	assert.False(t, ensureCalled.Load(), "ensure-refs should be skipped when local clone has the requested SHA")
}

func TestGitRestoreSkipsEnsureRefsWhenLocalHasCommit(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	localSHA := gitRevParse(t, srcDir, "HEAD")

	snapshotData := createTarZst(t, srcDir)

	var ensureCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			ensureCalled.Store(true)
			http.Error(w, "should not be called", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Commit:    []string{localSHA},
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	assert.NoError(t, restoreCmd.Run(context.Background(), api))
	assert.False(t, ensureCalled.Load(), "ensure-refs should be skipped when local clone has the requested commit")
}

func TestGitRestoreCommitMissingAfterFetchIsFatal(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	snapshotData := createTarZst(t, srcDir)
	missingSHA := "0000000000000000000000000000000000000000"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"missing_commits": []string{missingSHA},
				"fetched":         true,
			})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Commit:    []string{missingSHA},
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err := restoreCmd.Run(context.Background(), api)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server is missing")
}

func TestGitRestoreSkipsPullWhenLocalHasResolvedSHA(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})
	localSHA := gitRevParse(t, srcDir, "HEAD")

	snapshotData := createTarZst(t, srcDir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			// Resolve the unpinned ref to the SHA the local clone already has.
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"refs":    map[string]string{"refs/heads/main": localSHA},
				"fetched": false,
			})

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Ref:       map[string]string{"refs/heads/main": ""}, // unpinned
		NoBundle:  true,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	// The snapshot has no origin remote, so a real pull would fail. The test
	// passes only if pull is skipped because the server's resolved SHA is
	// already in the local clone.
	assert.NoError(t, restoreCmd.Run(context.Background(), api))
}

func TestGitRestorePullsFromOrigin(t *testing.T) {
	// Build an "upstream" repo whose snapshot we serve, plus a bare clone of
	// it that the working tree's origin will point at after restore. The
	// upstream then gets an extra commit so we can verify that the post-
	// restore `git pull` picks it up.
	tmpDir := t.TempDir()
	workDir := filepath.Join(tmpDir, "work")
	assert.NoError(t, os.MkdirAll(workDir, 0o755))
	initGitRepo(t, workDir, map[string]string{"file.txt": "v1"})

	bareDir := filepath.Join(tmpDir, "origin.git")
	runGit := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	runGit("clone", "--bare", workDir, bareDir)

	// Add an origin pointing at the bare clone so the post-restore pull has
	// somewhere to fetch from.
	runGit("-C", workDir, "remote", "add", "origin", bareDir)
	// Track origin/main so `git pull` knows what to merge.
	runGit("-C", workDir, "fetch", "origin")
	runGit("-C", workDir, "branch", "--set-upstream-to=origin/main", "main")

	snapshotData := createTarZst(t, workDir)

	// Add a commit upstream that the snapshot doesn't have.
	workDir2 := filepath.Join(tmpDir, "work2")
	runGit("clone", bareDir, workDir2)
	assert.NoError(t, os.WriteFile(filepath.Join(workDir2, "new.txt"), []byte("added"), 0o644))
	runGit("-C", workDir2, "add", "-A")
	runGit("-C", workDir2, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "add")
	runGit("-C", workDir2, "push", "origin", "main")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Write(snapshotData) //nolint:errcheck
		case strings.HasSuffix(r.URL.Path, "/ensure-refs"):
			w.Header().Set("Content-Type", "application/json")
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"refs": map[string]string{}, "fetched": false,
			}))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
		Ref:       map[string]string{"refs/heads/main": ""},
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	assert.NoError(t, restoreCmd.Run(context.Background(), api))

	// The post-restore pull from origin (= the bare repo) should have brought
	// in the new.txt commit.
	content, err := os.ReadFile(filepath.Join(dstDir, "new.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "added", string(content))
}

func TestGitRestoreBundleFailureNonFatal(t *testing.T) {
	srcDir := t.TempDir()
	initGitRepo(t, srcDir, map[string]string{"file.txt": "v1"})

	snapshotData := createTarZst(t, srcDir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/snapshot.tar.zst"):
			w.Header().Set("Content-Type", "application/zstd")
			w.Header().Set("X-Cachew-Bundle-Url", "/git/github.com/test/repo/snapshot.bundle?base=abc")
			w.Write(snapshotData) //nolint:errcheck

		case strings.HasSuffix(r.URL.Path, "/snapshot.bundle"):
			http.Error(w, "internal error", http.StatusInternalServerError)

		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dstDir := filepath.Join(t.TempDir(), "restored")
	restoreCmd := &GitRestoreCmd{
		RepoURL:   "https://github.com/test/repo",
		Directory: dstDir,
	}
	api := client.NewWithHTTPClient(srv.URL, srv.Client())
	err := restoreCmd.Run(context.Background(), api)
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(content))
}

func TestGitCommitSnapshotOfflineFanout(t *testing.T) {
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_ALLOW_PROTOCOL", "file")
	t.Setenv("GIT_TERMINAL_PROMPT", "0")
	t.Setenv("GIT_AUTHOR_NAME", "test")
	t.Setenv("GIT_AUTHOR_EMAIL", "test@test.com")
	t.Setenv("GIT_COMMITTER_NAME", "test")
	t.Setenv("GIT_COMMITTER_EMAIL", "test@test.com")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, ctx = logging.Configure(ctx, logging.Config{})
	gitRun := func(dir string, args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...).CombinedOutput()
		assert.NoError(t, err, string(out))
		return strings.TrimSpace(string(out))
	}
	lfsData := make([]byte, 2<<20)
	_, err := rand.Read(lfsData)
	assert.NoError(t, err)
	lfsContent := string(lfsData)
	oid := fmt.Sprintf("%x", sha256.Sum256([]byte(lfsContent)))
	var downloads atomic.Int32
	var available atomic.Bool
	lfs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/objects/"+oid {
			downloads.Add(1)
			fmt.Fprint(w, lfsContent)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
		fmt.Fprintf(w, `{"transfer":"basic","objects":[{"oid":%q,"size":%d,"actions":{"download":{"href":%q}}}]}`,
			oid, len(lfsContent), "http://"+r.Host+"/objects/"+oid)
	}))
	defer lfs.Close()
	src := t.TempDir()
	initGitRepo(t, src, map[string]string{"file.txt": "historical blob reused by target\n"})
	first := gitRevParse(t, src, "HEAD")
	assert.NoError(t, os.WriteFile(filepath.Join(src, "file.txt"), []byte("default branch content\n"), 0o644))
	gitRun(src, "commit", "-am", "main advances")
	gitRun(src, "checkout", "-b", "target", first)
	assert.NoError(t, os.WriteFile(filepath.Join(src, ".gitattributes"), []byte("*.bin filter=lfs diff=lfs merge=lfs -text\n"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(src, ".lfsconfig"), []byte("[lfs]\nurl = "+lfs.URL+"/lfs\n"), 0o644))
	pointer := fmt.Sprintf("version https://git-lfs.github.com/spec/v1\noid sha256:%s\nsize %d\n", oid, len(lfsContent))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "asset.bin"), []byte(pointer), 0o644))
	gitRun(src, "add", ".")
	gitRun(src, "commit", "-m", "target with LFS")
	sha := gitRevParse(t, src, "HEAD")
	gitRun(src, "checkout", "main")

	root := t.TempDir()
	mirror := filepath.Join(root, "github.com", "org", "repo")
	gitRun(src, "clone", "--mirror", src, mirror)
	store, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour, LimitMB: 64})
	assert.NoError(t, err)
	scheduler := jobscheduler.NewProvider(ctx, jobscheduler.Config{Concurrency: 1})
	mux := http.NewServeMux()
	s, err := gitstrategy.New(ctx, gitstrategy.Config{
		ZstdThreads: 1, SnapshotFilters: map[string]string{"github.com/org/repo": "blob:none"},
	}, scheduler, store, mux, gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: root}, nil),
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	deadline := time.Now().Add(10 * time.Second)
	for !s.Ready() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, s.Ready())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(logging.ContextWithLogger(r.Context(), logging.FromContext(ctx))))
	}))
	defer server.Close()
	api := client.NewWithHTTPClient(server.URL, server.Client())
	prepare := GitPrepareCmd{RepoURL: "https://github.com/org/repo", SHA: sha}
	assert.Error(t, prepare.Run(ctx, api))
	stats, err := store.Stats(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), stats.Objects, "failed LFS preparation must not publish")
	available.Store(true)

	results := make(chan error, 12)
	for range 12 {
		go func() { results <- prepare.Run(ctx, api) }()
	}
	for range 12 {
		assert.NoError(t, <-results)
	}
	assert.Equal(t, int32(1), downloads.Load(), "fanout must share preparation")

	plainDest := filepath.Join(t.TempDir(), "plain")
	plain := GitRestoreCmd{RepoURL: prepare.RepoURL, SHA: first, Directory: plainDest, ZstdThreads: 1}
	assert.NoError(t, plain.Run(ctx, api), "GET miss should prepare a non-LFS commit")
	assert.Equal(t, first, gitRevParse(t, plainDest, "HEAD"))

	assert.NoError(t, os.RemoveAll(mirror))
	assert.NoError(t, os.RemoveAll(src))
	lfs.Close()
	assert.NoError(t, prepare.Run(ctx, api), "preparation hit must also work offline")
	for _, concurrency := range []int{1, 4} {
		dest := filepath.Join(t.TempDir(), "restored")
		restore := GitRestoreCmd{RepoURL: prepare.RepoURL, SHA: sha, Directory: dest,
			DownloadConcurrency: concurrency, DownloadChunkSizeMB: 1, ZstdThreads: 1}
		assert.NoError(t, restore.Run(ctx, api))
		assert.Equal(t, sha, gitRevParse(t, dest, "HEAD"))
		assert.Equal(t, "true", gitRun(dest, "rev-parse", "--is-shallow-repository"))
		assert.Equal(t, "", gitRun(dest, "status", "--porcelain"))
		assert.Equal(t, prepare.RepoURL, gitRun(dest, "remote", "get-url", "origin"))
		for name, expected := range map[string]string{"file.txt": "historical blob reused by target\n", "asset.bin": lfsContent} {
			data, err := os.ReadFile(filepath.Join(dest, name))
			assert.NoError(t, err)
			assert.Equal(t, expected, string(data))
		}
		gitRun(dest, "fsck", "--full")
		gitRun(dest, "lfs", "fsck", "--objects", "HEAD")
	}
	assert.Equal(t, int32(1), downloads.Load())
	prepare.SHA = strings.Repeat("0", 40)
	assert.Error(t, prepare.Run(ctx, api), "an unavailable SHA must not fall back to a different snapshot")
}

func TestGitCommitRestoreRejectsMismatch(t *testing.T) {
	src := t.TempDir()
	initGitRepo(t, src, map[string]string{"file.txt": "wrong revision"})
	archive := createTarZst(t, src)
	want := strings.Repeat("a", 40)
	for _, header := range []string{gitRevParse(t, src, "HEAD"), want} {
		t.Run(header, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(client.SnapshotCommitHeader, header)
				w.Write(archive)
			}))
			defer server.Close()
			dest := filepath.Join(t.TempDir(), "restore")
			restore := GitRestoreCmd{RepoURL: "https://github.com/org/repo", SHA: want, Directory: dest}
			assert.Error(t, restore.Run(t.Context(), client.NewWithHTTPClient(server.URL, server.Client())))
			_, err := os.Stat(dest)
			assert.True(t, os.IsNotExist(err), "invalid snapshot must not be published")
		})
	}
}
