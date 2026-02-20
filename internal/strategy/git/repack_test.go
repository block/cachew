package git_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

func TestRepackInterval(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		repackInterval time.Duration
	}{
		{
			name:           "Enabled",
			repackInterval: 24 * time.Hour,
		},
		{
			name:           "Disabled",
			repackInterval: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := newTestMux()
			cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
				MirrorRoot: filepath.Join(tmpDir, tt.name),
			}, nil)
			s, err := git.New(ctx, git.Config{
				RepackInterval: tt.repackInterval,
			}, jobscheduler.New(ctx, jobscheduler.Config{}), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
			assert.NoError(t, err)
			assert.NotZero(t, s)
		})
	}
}

func TestRepackScheduledForExistingRepos(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	// Create a fake bare clone directory on disk before initializing strategy.
	clonePath := filepath.Join(tmpDir, "github.com", "org", "repo")
	err := os.MkdirAll(clonePath, 0o750)
	assert.NoError(t, err)
	err = os.WriteFile(filepath.Join(clonePath, "HEAD"), []byte("ref: refs/heads/main\n"), 0o640)
	assert.NoError(t, err)

	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: tmpDir,
	}, nil)
	s, err := git.New(ctx, git.Config{
		RepackInterval: 24 * time.Hour,
	}, jobscheduler.New(ctx, jobscheduler.Config{}), nil, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	assert.NotZero(t, s)
}
