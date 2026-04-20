package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestResolveKey(t *testing.T) {
	dir := t.TempDir()
	sumPath := filepath.Join(dir, "go.sum")
	assert.NoError(t, os.WriteFile(sumPath, []byte("v1"), 0o644))

	hashKey, err := client.HashFiles(sumPath)
	assert.NoError(t, err)

	platform := runtime.GOOS + "-" + runtime.GOARCH + "-"
	today := time.Now().Format("2006-01-02-")

	tests := []struct {
		name        string
		cli         CLI
		key         string
		hashFiles   []string
		wantDisplay string
	}{
		{name: "LiteralKey", key: "foo", wantDisplay: "foo"},
		{name: "HashFiles", hashFiles: []string{sumPath}, wantDisplay: hashKey.String()},
		{name: "LiteralKeyPlatformPrefix", cli: CLI{Platform: true}, key: "foo", wantDisplay: platform + "foo"},
		{name: "HashFilesDailyPrefix", cli: CLI{Daily: true}, hashFiles: []string{sumPath}, wantDisplay: today + hashKey.String()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, display, err := resolveKey(&tt.cli, tt.key, tt.hashFiles)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantDisplay, display)

			var want client.Key
			assert.NoError(t, want.UnmarshalText([]byte(tt.wantDisplay)))
			assert.Equal(t, want, key)
		})
	}
}

func TestResolveKeyHashFilesNoMatch(t *testing.T) {
	_, _, err := resolveKey(&CLI{}, "", []string{filepath.Join(t.TempDir(), "missing-*")})
	assert.Error(t, err)
}
