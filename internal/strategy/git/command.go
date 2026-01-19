package git

import (
	"bufio"
	"context"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

// gitCommand creates a git command with insteadOf URL rewriting disabled for the given URL
// to prevent infinite loops where git config rules rewrite URLs to point back through the proxy.
func gitCommand(ctx context.Context, url string, args ...string) (*exec.Cmd, error) {
	configArgs, err := getInsteadOfDisableArgsForURL(ctx, url)
	if err != nil {
		return nil, errors.Wrap(err, "get insteadOf disable args")
	}

	var allArgs []string
	if len(configArgs) > 0 {
		allArgs = append(allArgs, configArgs...)
	}
	allArgs = append(allArgs, args...)

	cmd := exec.CommandContext(ctx, "git", allArgs...)
	return cmd, nil
}

// getInsteadOfDisableArgsForURL returns arguments to disable insteadOf rules that would affect the given URL.
func getInsteadOfDisableArgsForURL(ctx context.Context, targetURL string) ([]string, error) {
	if targetURL == "" {
		return nil, nil
	}

	cmd := exec.CommandContext(ctx, "git", "config", "--get-regexp", "^url\\..*\\.(insteadof|pushinsteadof)$")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Exit code 1 when no insteadOf rules exist is expected, not an error
		return []string{}, nil //nolint:nilerr
	}

	var args []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			configKey := parts[0]
			pattern := parts[1]

			if strings.HasPrefix(targetURL, pattern) {
				args = append(args, "-c", configKey+"=")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scan insteadOf output")
	}

	return args, nil
}
