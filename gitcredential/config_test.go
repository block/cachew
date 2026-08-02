package gitcredential_test

import (
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/hcl/v2"

	"github.com/block/cachew/gitcredential"
)

func TestCommandConfigHCL(t *testing.T) {
	ast, err := hcl.Parse(strings.NewReader(`
git-credential-command "one" {
  command = ["/bin/one"]
  remotes = ["https://example.com/org/one"]
}
git-credential-command "two" {
  command = ["/bin/two", "--audience", "git"]
  remotes = ["https://example.com/org/two"]
  timeout = "10s"
  refresh-before = "1m"
}
`))
	assert.NoError(t, err)
	var cfg struct {
		Commands []gitcredential.CommandConfig `hcl:"git-credential-command,block,optional"`
	}
	assert.NoError(t, hcl.UnmarshalAST(ast, &cfg))
	assert.Equal(t, 2, len(cfg.Commands))
	assert.Equal(t, "one", cfg.Commands[0].Name)
	assert.Equal(t, 5*time.Second, cfg.Commands[0].Timeout)
	assert.Equal(t, 5*time.Minute, cfg.Commands[0].RefreshBefore)
	assert.Equal(t, []string{"/bin/two", "--audience", "git"}, cfg.Commands[1].Command)
	assert.Equal(t, 10*time.Second, cfg.Commands[1].Timeout)
	assert.Equal(t, time.Minute, cfg.Commands[1].RefreshBefore)
}
