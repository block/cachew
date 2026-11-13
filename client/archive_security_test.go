package client_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestArchiveRejectsIncludePathsOutsideBase(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	assert.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))

	var out bytes.Buffer
	assert.Error(t, client.Archive(t.Context(), &out, base, []string{"../secret.txt"}, nil, 0))
	assert.Error(t, client.Archive(t.Context(), &out, base, []string{outside}, nil, 0))
}
