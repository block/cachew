package git //nolint:testpackage // This test needs access to the spool mutex.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

func TestCloneFailurePreservesSuccessorOwnership(t *testing.T) {
	root := t.TempDir()
	started := filepath.Join(root, "started")
	assert.NoError(t, os.WriteFile(filepath.Join(root, "git"), []byte("#!/bin/sh\ntouch \"$CLONE_STARTED\"\nexit 42\n"), 0o750))
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CLONE_STARTED", started)
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: filepath.Join(root, "mirrors")}, nil)
	assert.NoError(t, err)
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, "https://example.test/example/repo")
	assert.NoError(t, err)
	s := &Strategy{cache: memCache, cloneManager: manager, metrics: newGitMetrics()}

	s.spoolsMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			s.spoolsMu.Unlock()
		}
	})
	done := make(chan error, 1)
	go func() { done <- s.startClone(ctx, repo) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(started)
		if err == nil && repo.State() == gitclone.StateEmpty {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("clone did not fail before spool cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	assert.True(t, repo.TryStartCloning())
	s.spoolsMu.Unlock()
	locked = false
	assert.Error(t, <-done)
	assert.Equal(t, gitclone.StateCloning, repo.State())
}
