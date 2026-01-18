package git

import (
	"bufio"
	"context"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

// gitCommand creates a git command with insteadOf URL rewriting disabled for the given URL.
// This prevents git config rules like "url.X.insteadOf=Y" from rewriting the specific URL
// to point back through the proxy, which would cause infinite loops.
// Other insteadOf rules and all auth configuration are preserved.
func gitCommand(ctx context.Context, url string, args ...string) (*exec.Cmd, error) {
	// Query for insteadOf rules that would affect this URL and build -c flags to disable them
	configArgs, err := getInsteadOfDisableArgsForURL(ctx, url)
	if err != nil {
		return nil, errors.Wrap(err, "get insteadOf disable args")
	}

	// Prepend disable args to the git command arguments
	var allArgs []string
	if len(configArgs) > 0 {
		allArgs = append(allArgs, configArgs...)
	}
	allArgs = append(allArgs, args...)

	cmd := exec.CommandContext(ctx, "git", allArgs...)
	return cmd, nil
}

// getInsteadOfDisableArgsForURL queries git config for insteadOf rules that would affect
// the given URL and returns arguments to disable only those specific rules.
func getInsteadOfDisableArgsForURL(ctx context.Context, targetURL string) ([]string, error) {
	if targetURL == "" {
		return nil, nil
	}

	// Query git config for all url.*.insteadOf and url.*.pushInsteadOf settings
	cmd := exec.CommandContext(ctx, "git", "config", "--get-regexp", "^url\\..*\\.(insteadof|pushinsteadof)$")
	output, err := cmd.CombinedOutput()
	if err != nil {
		// No insteadOf rules found (exit code 1) is expected and not an error
		// Return empty args to continue without disabling any rules
		return []string{}, nil //nolint:nilerr // Exit code 1 is expected when no rules exist
	}

	// Parse output and check which rules would match our URL
	// Output format: url.<base>.insteadof <pattern> or url.<base>.pushinsteadof <pattern>
	var args []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		// Split into config key and value
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			configKey := parts[0]
			pattern := parts[1]

			// Check if our target URL would match this insteadOf pattern
			if strings.HasPrefix(targetURL, pattern) {
				// This rule would affect our URL, so disable it
				args = append(args, "-c", configKey+"=")
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "scan insteadOf output")
	}

	return args, nil
}
