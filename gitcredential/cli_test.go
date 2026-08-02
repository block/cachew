package gitcredential_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/gitcredential"
)

type providerOptions struct {
	Audience string `name:"audience" required:""`
}

func TestRunCommandCLIHelp(t *testing.T) {
	stdout := &bytes.Buffer{}
	called := false
	err := gitcredential.RunCommandCLI(
		t.Context(),
		[]string{"--help"},
		bytes.NewReader(nil),
		stdout,
		&bytes.Buffer{},
		&providerOptions{},
		func(_ context.Context, _ *providerOptions) (gitcredential.CommandHandler, error) {
			called = true
			return gitcredential.CommandHandlerFunc(func(context.Context, string) (gitcredential.CommandResult, error) {
				return gitcredential.CommandResult{}, nil
			}), nil
		},
	)
	assert.NoError(t, err)
	assert.False(t, called)
	assert.Contains(t, stdout.String(), "--audience")
}

func TestRunCommandCLI(t *testing.T) {
	stdin := bytes.NewBufferString("{\"version\":1,\"remote_url\":\"https://example.com/org/repo\"}\n")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	options := &providerOptions{}
	err := gitcredential.RunCommandCLI(
		t.Context(),
		[]string{"--audience", "example"},
		stdin,
		stdout,
		stderr,
		options,
		func(_ context.Context, parsed *providerOptions) (gitcredential.CommandHandler, error) {
			return gitcredential.CommandHandlerFunc(func(_ context.Context, remoteURL string) (gitcredential.CommandResult, error) {
				assert.Equal(t, "example", parsed.Audience)
				assert.Equal(t, "https://example.com/org/repo", remoteURL)
				return gitcredential.CommandResult{
					Authorization: "Bearer token",
					ExpiresAt:     time.Now().Add(time.Hour),
				}, nil
			}), nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, "", stderr.String())
	var response gitcredential.Response
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &response))
	assert.Equal(t, gitcredential.ProtocolVersion, response.Version)
	assert.Equal(t, "Bearer token", response.Authorization)
}
