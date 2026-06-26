package git_test

import (
	"os"
	"testing"
)

// TestMain drops the GIT_* variables git exports under hooks so test git
// commands target their temp dirs, not the ambient repository.
func TestMain(m *testing.M) {
	for _, v := range []string{
		"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
		"GIT_COMMON_DIR", "GIT_PREFIX", "GIT_NAMESPACE",
	} {
		_ = os.Unsetenv(v)
	}
	os.Exit(m.Run())
}
