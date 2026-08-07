package gitclone //nolint:testpackage // white-box testing required for unexported fields

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

// newTestRepo creates a bare mirror-style git repository with one commit and
// returns a Repository pointing at it, along with the HEAD commit SHA.
func newTestRepo(t *testing.T) (*Repository, string) {
	t.Helper()
	tmpDir := t.TempDir()
	workPath := filepath.Join(tmpDir, "work")
	repoPath := filepath.Join(tmpDir, "mirror.git")

	assert.NoError(t, os.MkdirAll(workPath, 0o755))

	for _, args := range [][]string{
		{"git", "-c", "init.defaultBranch=main", "-C", workPath, "init"},
		{"git", "-C", workPath, "config", "user.email", "test@example.com"},
		{"git", "-C", workPath, "config", "user.name", "Test"},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}

	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "f.txt"), []byte("hello"), 0o600))

	for _, args := range [][]string{
		{"git", "-C", workPath, "add", "."},
		{"git", "-C", workPath, "commit", "-m", "init"},
		{"git", "clone", "--mirror", workPath, repoPath},
	} {
		assert.NoError(t, exec.Command(args[0], args[1:]...).Run())
	}

	// #nosec G204 - workPath is a t.TempDir() path created by this test
	out, err := exec.Command("git", "-C", workPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	headSHA := strings.TrimSpace(string(out))

	repo := &Repository{
		state:       StateReady,
		config:      testRepoConfig(),
		path:        repoPath,
		upstreamURL: workPath,
		fetchSem:    make(chan struct{}, 1),
	}
	repo.fetchSem <- struct{}{}

	return repo, headSHA
}

func TestMissingObjects(t *testing.T) {
	// A fabricated valid-hex SHA that does not exist in any real repo.
	const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	tests := []struct {
		name        string
		inputFn     func(presentSHA string) []string
		wantMissing func(presentSHA string) []string
	}{
		{
			name:        "EmptyInput",
			inputFn:     func(_ string) []string { return nil },
			wantMissing: func(_ string) []string { return nil },
		},
		{
			name:        "AllPresent",
			inputFn:     func(sha string) []string { return []string{sha} },
			wantMissing: func(_ string) []string { return nil },
		},
		{
			name:        "AllMissing",
			inputFn:     func(_ string) []string { return []string{absentSHA} },
			wantMissing: func(_ string) []string { return []string{absentSHA} },
		},
		{
			name:        "SomeMissing",
			inputFn:     func(sha string) []string { return []string{sha, absentSHA} },
			wantMissing: func(_ string) []string { return []string{absentSHA} },
		},
		{
			name:        "DuplicatesDeduped",
			inputFn:     func(_ string) []string { return []string{absentSHA, absentSHA} },
			wantMissing: func(_ string) []string { return []string{absentSHA} },
		},
		{
			name:        "OrderPreserved",
			inputFn:     func(_ string) []string { return []string{absentSHA, "deadbeef00deadbeef00deadbeef00deadbeef00"} },
			wantMissing: func(_ string) []string { return []string{absentSHA, "deadbeef00deadbeef00deadbeef00deadbeef00"} },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, headSHA := newTestRepo(t)
			input := tc.inputFn(headSHA)
			got, err := repo.MissingObjects(context.Background(), input)
			assert.NoError(t, err)
			assert.Equal(t, tc.wantMissing(headSHA), got)
		})
	}
}

func TestMissingObjects_InvalidOID(t *testing.T) {
	tests := []struct {
		name string
		oid  string
	}{
		{
			name: "TooShort",
			oid:  "deadbeef",
		},
		{
			name: "TooLong",
			oid:  strings.Repeat("a", 41),
		},
		{
			name: "UpperCaseHex",
			oid:  "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF",
		},
		{
			name: "NonHexChars",
			oid:  "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := newTestRepo(t)
			_, err := repo.MissingObjects(context.Background(), []string{tc.oid})
			assert.Error(t, err)
		})
	}
}
