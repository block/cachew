package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

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

func gitRevParse(t *testing.T, dir, ref string) string {
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
	cli := &CLI{URL: srv.URL}
	err := cmd.Run(context.Background(), cli, srv.Client())
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(content))

	content, err = os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "nested content", string(content))
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
	cli := &CLI{URL: srv.URL}
	err = restoreCmd.Run(context.Background(), cli, srv.Client())
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
	cli := &CLI{URL: srv.URL}
	err := restoreCmd.Run(context.Background(), cli, srv.Client())
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
	cli := &CLI{URL: srv.URL}
	err := restoreCmd.Run(context.Background(), cli, srv.Client())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
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
	cli := &CLI{URL: srv.URL}
	err := restoreCmd.Run(context.Background(), cli, srv.Client())
	assert.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(content))
}
