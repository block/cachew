//go:build linux

package client

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestOwnerLookupUsesNSS(t *testing.T) {
	stubGetent(t, `case "$1 $2" in
"passwd directory-user") echo "directory-user:*:4242:4242::/home/directory-user:/bin/sh" ;;
"group directory-group") echo "directory-group:*:4343:" ;;
"passwd 0") echo "root:x:0:0:root:/root:/bin/sh" ;;
*) exit 2 ;;
esac`)

	uid, err := userID(t.Context(), "directory-user")
	assert.NoError(t, err)
	assert.Equal(t, 4242, uid)
	gid, err := groupID(t.Context(), "directory-group")
	assert.NoError(t, err)
	assert.Equal(t, 4343, gid)
	_, err = userID(t.Context(), "0")
	assert.Error(t, err)
}

func TestOwnerLookupStopsWhenCancelled(t *testing.T) {
	stubGetent(t, "exec sleep 30")
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := userID(ctx, "directory-user")
	assert.Error(t, err)
	assert.True(t, time.Since(start) < 10*time.Second)
}

func stubGetent(t *testing.T, script string) {
	t.Helper()
	bin := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(bin, "getent"), []byte("#!/bin/sh\n"+script+"\n"), 0o755))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}
