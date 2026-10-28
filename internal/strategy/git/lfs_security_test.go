package git //nolint:testpackage // white-box testing is required for process-boundary hardening

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func TestGitLFSVersionSupported(t *testing.T) {
	assert.False(t, gitLFSVersionSupported([]byte("git-lfs/3.7.1 (GitHub; linux amd64; go 1.24.4)")))
	assert.False(t, gitLFSVersionSupported([]byte("unexpected output")))
	assert.True(t, gitLFSVersionSupported([]byte("git-lfs/3.8.1 (GitHub; linux amd64; go 1.27.3)")))
	assert.True(t, gitLFSVersionSupported([]byte("git-lfs/4.0.0 (GitHub; linux amd64; go 1.28)")))
}

func TestLFSEndpointForUpstream(t *testing.T) {
	tests := []struct {
		upstream string
		expected string
	}{
		{"https://github.com/org/repo", "https://github.com/org/repo.git/info/lfs"},
		{"https://github.com/org/repo.git", "https://github.com/org/repo.git/info/lfs"},
	}
	for _, test := range tests {
		actual, err := lfsEndpointForUpstream(test.upstream)
		assert.NoError(t, err)
		assert.Equal(t, test.expected, actual)
	}
}

func TestLFSEndpointForUpstreamRejectsUnsafeURL(t *testing.T) {
	for _, upstream := range []string{
		"http://127.0.0.1/org/repo",
		"https://user@example.com/org/repo",
		"https://example.com/org/repo?endpoint=http://127.0.0.1",
		"https://example.com/org/repo#x",
	} {
		_, err := lfsEndpointForUpstream(upstream)
		assert.Error(t, err)
	}
}

func TestReplaceRepositoryLFSConfigDoesNotFollowSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink setup requires privileges on Windows")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	assert.NoError(t, os.WriteFile(outside, []byte("preserve"), 0o600))
	workDir := t.TempDir()
	assert.NoError(t, os.Symlink(outside, filepath.Join(workDir, ".lfsconfig")))

	assert.NoError(t, replaceRepositoryLFSConfig(workDir))
	contents, err := os.ReadFile(outside)
	assert.NoError(t, err)
	assert.Equal(t, "preserve", string(contents))
	info, err := os.Lstat(filepath.Join(workDir, ".lfsconfig"))
	assert.NoError(t, err)
	assert.False(t, info.Mode()&os.ModeSymlink != 0)
}

func TestRestoreMirrorSnapshotSanitizesCachedCode(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	upstream := "https://github.com/org/repo"
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstream)
	assert.NoError(t, err)

	archiveDir := filepath.Join(t.TempDir(), "repo")
	assert.NoError(t, exec.Command("git", "init", "--bare", archiveDir).Run())
	proofPath := filepath.Join(t.TempDir(), "proof")
	assert.NoError(t, os.WriteFile(filepath.Join(archiveDir, "hooks", "reference-transaction"),
		[]byte("#!/bin/sh\ntouch \""+proofPath+"\"\n"), 0o755))
	assert.NoError(t, exec.Command("git", "-C", archiveDir, "config", "credential.helper", "!touch "+proofPath).Run())

	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	assert.NoError(t, snapshot.Create(ctx, mem, mirrorSnapshotCacheKey(upstream), archiveDir, time.Hour, nil, 1))
	strategy := &Strategy{cache: mem, cloneManager: manager, config: Config{ZstdThreads: 1}}
	assert.NoError(t, strategy.tryRestoreSnapshot(ctx, repo))

	_, err = os.Lstat(filepath.Join(repo.Path(), "hooks", "reference-transaction"))
	assert.True(t, os.IsNotExist(err))
	cmd := exec.Command("git", "-C", repo.Path(), "config", "--get", "credential.helper")
	assert.Error(t, cmd.Run())
	_, err = os.Lstat(proofPath)
	assert.True(t, os.IsNotExist(err))
}

func TestCloneForSnapshotIgnoresGlobalFilters(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{})
	workDir := t.TempDir()
	for _, args := range [][]string{
		{"git", "-C", workDir, "init"},
		{"git", "-C", workDir, "config", "user.email", "test@example.com"},
		{"git", "-C", workDir, "config", "user.name", "Test"},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, ".gitattributes"), []byte("* filter=poc\n"), 0o600))
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, "data"), []byte("cached code\n"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", workDir, "add", "."},
		{"git", "-C", workDir, "commit", "-m", "initial"},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}

	mirrorRoot := t.TempDir()
	mirrorPath := filepath.Join(mirrorRoot, "github.com", "org", "repo")
	assert.NoError(t, exec.Command("git", "clone", "--mirror", workDir, mirrorPath).Run())
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, "https://github.com/org/repo")
	assert.NoError(t, err)

	proofPath := filepath.Join(t.TempDir(), "proof")
	filterPath := filepath.Join(t.TempDir(), "filter")
	assert.NoError(t, os.WriteFile(filterPath, []byte("#!/bin/sh\ntouch \""+proofPath+"\"\nexit 1\n"), 0o700))
	configPath := filepath.Join(t.TempDir(), "gitconfig")
	assert.NoError(t, os.WriteFile(configPath, []byte("[filter \"poc\"]\n\tprocess = "+filterPath+"\n\trequired = true\n"), 0o600))
	t.Setenv("GIT_CONFIG_GLOBAL", configPath)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	strategy := &Strategy{cloneManager: manager}
	assert.NoError(t, strategy.cloneForSnapshot(ctx, repo, filepath.Join(t.TempDir(), "snapshot"), ""))
	_, err = os.Lstat(proofPath)
	assert.True(t, os.IsNotExist(err))
}
