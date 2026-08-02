// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	"math"
	"os"
	"os/exec"
	"strconv"
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

	var credentialConfigKey string
	var credentialConfigValue string
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
		credentialConfigKey = "http." + credentialScope + ".extraHeader"
		credentialConfigValue = "Authorization: " + authorization
	}

	allArgs = append(allArgs, args...)

	cmd := exec.CommandContext(ctx, "git", allArgs...)
	if credentialConfigKey != "" {
		cmd.Env, err = appendGitConfigEnv(os.Environ(), credentialConfigKey, credentialConfigValue)
		if err != nil {
			return nil, errors.Wrap(err, "configure Git credential environment")
		}
	}
	return cmd, nil
}

func appendGitConfigEnv(env []string, key, value string) ([]string, error) {
	count := 0
	if rawCount, ok := envValue(env, "GIT_CONFIG_COUNT"); ok {
		parsed, err := strconv.Atoi(rawCount)
		if err != nil || parsed < 0 || parsed == math.MaxInt {
			return nil, errors.Errorf("invalid GIT_CONFIG_COUNT %q", rawCount)
		}
		count = parsed
	}
	env = setEnv(env, "GIT_CONFIG_COUNT", strconv.Itoa(count+1))
	env = setEnv(env, "GIT_CONFIG_KEY_"+strconv.Itoa(count), key)
	env = setEnv(env, "GIT_CONFIG_VALUE_"+strconv.Itoa(count), value)
	return env, nil
}

func envValue(env []string, key string) (string, bool) {
	for i := len(env) - 1; i >= 0; i-- {
		if envKey, value, ok := strings.Cut(env[i], "="); ok && envKey == key {
			return value, true
		}
	}
	return "", false
}

func setEnv(env []string, key, value string) []string {
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if envKey, _, ok := strings.Cut(entry, "="); ok && envKey == key {
			continue
		}
		result = append(result, entry)
	}
	return append(result, key+"="+value)
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
