// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	neturl "net/url"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

func gitCommand(ctx context.Context, repoURL string, credentialProvider CredentialProvider, args ...string) (*exec.Cmd, error) {
	modifiedURL := repoURL
	if credentialProvider != nil && strings.Contains(repoURL, "github.com") {
		token, err := credentialProvider.GetTokenForURL(ctx, repoURL)
		if err == nil && token != "" {
			modifiedURL = injectTokenIntoURL(repoURL, token)
		}
		// If error getting token, fall back to original URL (system credentials)
	}

	configArgs, err := getInsteadOfDisableArgsForURL(ctx, repoURL)
	if err != nil {
		return nil, errors.Wrap(err, "get insteadOf disable args")
	}

	var allArgs []string
	if len(configArgs) > 0 {
		allArgs = append(allArgs, configArgs...)
	}
	allArgs = append(allArgs, args...)

	// Replace URL in args if it was modified for authentication
	if modifiedURL != repoURL {
		for i, arg := range allArgs {
			if arg == repoURL {
				allArgs[i] = modifiedURL
			}
		}
	}

	return exec.CommandContext(ctx, "git", allArgs...), nil
}

// Converts https://github.com/org/repo to https://x-access-token:TOKEN@github.com/org/repo
func injectTokenIntoURL(rawURL, token string) string {
	if token == "" {
		return rawURL
	}

	u, err := neturl.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	// Only inject token for GitHub URLs
	if !strings.Contains(u.Host, "github.com") {
		return rawURL
	}

	// Upgrade http to https for security
	if u.Scheme == "http" {
		u.Scheme = "https"
	}

	u.User = neturl.UserPassword("x-access-token", token)
	return u.String()
}

func getInsteadOfDisableArgsForURL(ctx context.Context, targetURL string) ([]string, error) {
	if targetURL == "" {
		return nil, nil
	}

	cmd := exec.CommandContext(ctx, "git", "config", "--get-regexp", "^url\\..*\\.(insteadof|pushinsteadof)$")
	output, err := cmd.CombinedOutput()
	if err != nil {
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

func ParseGitRefs(output []byte) map[string]string {
	refs := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			sha := parts[0]
			ref := parts[1]
			refs[ref] = sha
		}
	}
	return refs
}
