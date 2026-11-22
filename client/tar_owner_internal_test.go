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
"passwd AliasUser") echo "canonical-user:*:4444:4444::/home/canonical-user:/bin/sh" ;;
"group AliasGroup") echo "canonical-group:*:4545:" ;;
"passwd 0") echo "root:x:0:0:root:/root:/bin/sh" ;;
"passwd 1234") echo "1234:*:4646:4646::/home/1234:/bin/sh" ;;
*) exit 2 ;;
esac`)

	uid, err := userID(t.Context(), "directory-user")
	assert.NoError(t, err)
	assert.Equal(t, 4242, uid)
	gid, err := groupID(t.Context(), "directory-group")
	assert.NoError(t, err)
	assert.Equal(t, 4343, gid)
	// NSS can resolve an alias to its canonical name, which getpwnam accepts too.
	uid, err = userID(t.Context(), "AliasUser")
	assert.NoError(t, err)
	assert.Equal(t, 4444, uid)
	gid, err = groupID(t.Context(), "AliasGroup")
	assert.NoError(t, err)
	assert.Equal(t, 4545, gid)
	// getent looks numeric keys up by ID, so only a record with that exact name matches what getpwnam would find.
	uid, err = userID(t.Context(), "1234")
	assert.NoError(t, err)
	assert.Equal(t, 4646, uid)
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
