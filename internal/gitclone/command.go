// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
)

// GitCommand returns a git subprocess configured with repository-scoped
// authentication and any per-URL git config overrides disabled.
func (r *Repository) GitCommand(ctx context.Context, args ...string) (*exec.Cmd, error) {
	repoURL := r.upstreamURL
	var authorization string
	var credentialScope string
	if r.credentialProvider != nil {
		credential, matched, err := r.credentialProvider.Credential(ctx, repoURL)
		if err != nil {
			return nil, errors.Wrap(err, "get git credential")
		}
		if matched {
			authorization = credential.Authorization
			credentialScope = credential.URLScope
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

	if authorization != "" {
		if strings.TrimSpace(authorization) != authorization || strings.ContainsAny(authorization, "\r\n\x00") {
			return nil, errors.New("credential provider returned an invalid authorization value")
		}
		expectedScope, err := gitcredential.NormalizeRepositoryURLScope(repoURL)
		if err != nil {
			return nil, errors.Wrap(err, "normalize credential URL scope")
		}
		if credentialScope != expectedScope {
			return nil, errors.New("credential provider returned an invalid URL scope")
		}
		allArgs = append(allArgs, "-c", "http."+credentialScope+".extraHeader=Authorization: "+authorization)
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
