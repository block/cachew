package git_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

type testMux struct {
	handlers map[string]http.Handler
}

func newTestMux() *testMux {
	return &testMux{handlers: make(map[string]http.Handler)}
}

func (m *testMux) Handle(pattern string, handler http.Handler) {
	m.handlers[pattern] = handler
}

func (m *testMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	m.handlers[pattern] = http.HandlerFunc(handler)
}

func TestNew(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		config    gitclone.Config
		wantError string
	}{
		{
			name: "ValidConfig",
			config: gitclone.Config{
				MirrorRoot:    filepath.Join(tmpDir, "clones"),
				FetchInterval: 15,
			},
		},
		{
			name: "MissingClonesRoot",
			config: gitclone.Config{
				FetchInterval: 15,
			},
			wantError: "mirror-root is required",
		},
		{
			name: "DefaultFetchInterval",
			config: gitclone.Config{
				MirrorRoot: filepath.Join(tmpDir, "clones2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newTestMux()
			cm := gitclone.NewManagerProvider(ctx, tt.config)
			s, err := git.New(ctx, git.Config{}, jobscheduler.New(ctx, jobscheduler.Config{}), nil, mux, cm)
			if tt.wantError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			assert.NoError(t, err)
			assert.NotZero(t, s)
			assert.Equal(t, "git", s.String())

			// Verify handlers were registered
			assert.NotZero(t, mux.handlers["GET /git/{host}/{path...}"])
			assert.NotZero(t, mux.handlers["POST /git/{host}/{path...}"])
		})
	}
}

func TestExtractRepoPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "InfoRefs",
			input:    "org/repo/info/refs",
			expected: "org/repo",
		},
		{
			name:     "GitUploadPack",
			input:    "org/repo/git-upload-pack",
			expected: "org/repo",
		},
		{
			name:     "GitReceivePack",
			input:    "org/repo/git-receive-pack",
			expected: "org/repo",
		},
		{
			name:     "WithGitSuffix",
			input:    "org/repo.git/info/refs",
			expected: "org/repo",
		},
		{
			name:     "NestedPath",
			input:    "org/group/subgroup/repo/info/refs",
			expected: "org/group/subgroup/repo",
		},
		{
			name:     "PlainPath",
			input:    "org/repo",
			expected: "org/repo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := git.ExtractRepoPath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewWithExistingCloneOnDisk(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	// Create a fake clone directory on disk before initializing strategy
	// For regular clones, we need a .git subdirectory with HEAD file
	clonePath := filepath.Join(tmpDir, "github.com", "org", "repo")
	gitDir := filepath.Join(clonePath, ".git")
	err := os.MkdirAll(gitDir, 0o750)
	assert.NoError(t, err)

	// Create HEAD file to make it look like a valid git repo
	headPath := filepath.Join(gitDir, "HEAD")
	err = os.WriteFile(headPath, []byte("ref: refs/heads/main\n"), 0o640)
	assert.NoError(t, err)

	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    tmpDir,
		FetchInterval: 15,
	})
	s, err := git.New(ctx, git.Config{}, jobscheduler.New(ctx, jobscheduler.Config{}), nil, mux, cm)
	assert.NoError(t, err)
	assert.NotZero(t, s)
}

func TestIntegrationWithMockUpstream(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})

	// Create a mock upstream server
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("mock git response"))
	}))
	defer upstream.Close()

	tmpDir := t.TempDir()

	// Create strategy - it will register handlers
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:    tmpDir,
		FetchInterval: 15,
	})
	_, err := git.New(ctx, git.Config{}, jobscheduler.New(ctx, jobscheduler.Config{}), nil, mux, cm)
	assert.NoError(t, err)

	// Verify handlers exist
	assert.NotZero(t, mux.handlers["GET /git/{host}/{path...}"])
	assert.NotZero(t, mux.handlers["POST /git/{host}/{path...}"])
}

func TestParseGitRefs(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	_ = ctx

	tests := []struct {
		name     string
		output   string
		expected map[string]string
	}{
		{
			name: "MultipleRefs",
			output: `abc123def456 refs/heads/main
789ghi012jkl refs/heads/develop
mno345pqr678 refs/tags/v1.0.0`,
			expected: map[string]string{
				"refs/heads/main":    "abc123def456",
				"refs/heads/develop": "789ghi012jkl",
				"refs/tags/v1.0.0":   "mno345pqr678",
			},
		},
		{
			name:     "EmptyOutput",
			output:   "",
			expected: map[string]string{},
		},
		{
			name: "SingleRef",
			output: `abc123def456 refs/heads/main
`,
			expected: map[string]string{
				"refs/heads/main": "abc123def456",
			},
		},
		{
			name: "WithPeeledRefs",
			output: `e93f19bd6cab17c507792599b8a22f7b567ef516 refs/tags/v1.2.1
babfaf8dee0baa09c56d1a2ec5623b60d900518b refs/tags/v1.2.1^{}
abc123def456 refs/heads/main`,
			expected: map[string]string{
				"refs/tags/v1.2.1":    "e93f19bd6cab17c507792599b8a22f7b567ef516",
				"refs/tags/v1.2.1^{}": "babfaf8dee0baa09c56d1a2ec5623b60d900518b",
				"refs/heads/main":     "abc123def456",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gitclone.ParseGitRefs([]byte(tt.output))
			assert.Equal(t, tt.expected, result)
		})
	}
}
