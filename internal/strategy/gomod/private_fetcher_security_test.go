package gomod_test

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/gomod"
)

func TestGenerateZipIgnoresHostTemplates(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	workDir := t.TempDir()
	for _, args := range [][]string{
		{"-C", workDir, "init"},
		{"-C", workDir, "config", "user.email", "test@example.com"},
		{"-C", workDir, "config", "user.name", "Test"},
	} {
		assert.NoError(t, exec.Command("git", args...).Run())
	}
	assert.NoError(t, os.WriteFile(filepath.Join(workDir, "go.mod"), []byte("module example.com/mod\n"), 0o600))
	assert.NoError(t, exec.Command("git", "-C", workDir, "add", "go.mod").Run())
	assert.NoError(t, exec.Command("git", "-C", workDir, "commit", "-m", "initial").Run())
	assert.NoError(t, exec.Command("git", "-C", workDir, "tag", "v1.0.0").Run())

	upstream := "https://example.com/mod"
	mirrorRoot := t.TempDir()
	mirrorPath := filepath.Join(mirrorRoot, "example.com", "mod")
	assert.NoError(t, exec.Command("git", "clone", "--mirror", workDir, mirrorPath).Run())
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, upstream)
	assert.NoError(t, err)

	templateDir := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(templateDir, "hooks"), 0o700))
	proofPath := filepath.Join(t.TempDir(), "proof")
	assert.NoError(t, os.WriteFile(filepath.Join(templateDir, "hooks", "post-checkout"),
		[]byte("#!/bin/sh\ntouch \""+proofPath+"\"\n"), 0o700))
	t.Setenv("GIT_TEMPLATE_DIR", templateDir)

	content, err := gomod.GenerateZipForTest(ctx, repo, "example.com/mod", "v1.0.0")
	assert.NoError(t, err)
	assert.NoError(t, content.Close())
	_, err = os.Lstat(proofPath)
	assert.True(t, os.IsNotExist(err))
}
