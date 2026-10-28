package git //nolint:testpackage // white-box testing is required for process-boundary hardening

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/alecthomas/assert/v2"
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
