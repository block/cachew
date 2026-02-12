// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

func (r *Repository) gitCommand(ctx context.Context, args ...string) (*exec.Cmd, error) {
	repoURL := r.upstreamURL
	var token string
	if r.credentialProvider != nil && strings.Contains(repoURL, "github.com") {
		var err error
		token, err = r.credentialProvider.GetTokenForURL(ctx, repoURL)
		// If error getting token, fall back to original URL (system credentials)
		if err != nil {
			token = ""
		}
	}

	configArgs, err := getInsteadOfDisableArgsForURL(ctx, repoURL)
	if err != nil {
		return nil, errors.Wrap(err, "get insteadOf disable args")
	}

	var allArgs []string
	if len(configArgs) > 0 {
		allArgs = append(allArgs, configArgs...)
	}

	// Add credential helper configuration if we have a token
	// This ensures git uses the GitHub App token for authentication
	// for all operations (clone, fetch, remote update, etc.)
	if token != "" {
		escapedToken := strings.ReplaceAll(token, "'", "'\\''")
		credHelper := "!f() { test \"$1\" = get && echo username=x-access-token && printf 'password=%s\\n' '" + escapedToken + "'; }; f"
		allArgs = append(allArgs, "-c", "credential.helper="+credHelper)
	}

	allArgs = append(allArgs, args...)

	return exec.CommandContext(ctx, "git", allArgs...), nil
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
