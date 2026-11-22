//go:build linux

package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestOwnerLookupUsesNSS(t *testing.T) {
	bin := t.TempDir()
	getent := `#!/bin/sh
case "$1 $2" in
"passwd directory-user") echo "directory-user:*:4242:4242::/home/directory-user:/bin/sh" ;;
"group directory-group") echo "directory-group:*:4343:" ;;
"passwd 0") echo "root:x:0:0:root:/root:/bin/sh" ;;
*) exit 2 ;;
esac
`
	assert.NoError(t, os.WriteFile(filepath.Join(bin, "getent"), []byte(getent), 0o755))
	t.Setenv("PATH", bin)

	uid, err := userID("directory-user")
	assert.NoError(t, err)
	assert.Equal(t, 4242, uid)
	gid, err := groupID("directory-group")
	assert.NoError(t, err)
	assert.Equal(t, 4343, gid)
	_, err = userID("0")
	assert.Error(t, err)
}
