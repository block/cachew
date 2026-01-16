package git //nolint:testpackage // Internal functions need to be tested

import (
	"context"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestGetInsteadOfDisableArgsForURL(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		targetURL string
		// We can't easily test the actual git config reading in a unit test,
		// but we can test the logic would work correctly
		skipTest bool
	}{
		{
			name:      "EmptyURL",
			targetURL: "",
			skipTest:  false,
		},
		{
			name:      "GitHubURL",
			targetURL: "https://github.com/user/repo",
			skipTest:  true, // Skip actual git config test
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipTest {
				t.Skip("Requires git config setup")
			}

			args, err := getInsteadOfDisableArgsForURL(ctx, tt.targetURL)
			assert.NoError(t, err)
			if tt.targetURL == "" {
				assert.Equal(t, 0, len(args))
			}
		})
	}
}

func TestGitCommand(t *testing.T) {
	ctx := context.Background()

	// Test that gitCommand creates a valid command
	cmd, err := gitCommand(ctx, "https://github.com/user/repo", "version")
	assert.NoError(t, err)

	assert.NotZero(t, cmd)
	// Should have at least "git" and "version" in args
	assert.True(t, len(cmd.Args) >= 2)
	// First arg should be git binary path
	assert.Equal(t, "git", cmd.Args[0])
	// Last arg should be "version"
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}

func TestGitCommandWithEmptyURL(t *testing.T) {
	ctx := context.Background()

	// Test with empty URL (for commands that don't need URL filtering)
	cmd, err := gitCommand(ctx, "", "version")
	assert.NoError(t, err)

	assert.NotZero(t, cmd)
	assert.Equal(t, "git", cmd.Args[0])
	assert.Equal(t, "version", cmd.Args[len(cmd.Args)-1])
}
