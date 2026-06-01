// Package gitclone provides reusable git clone management with lifecycle control,
// concurrency management, and large repository optimizations.
package gitclone

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// credentialFileRefreshInterval is short enough that any rotation by
// TokenManager (which refreshes ~5 min before the 1 h token expiry) is
// reflected on disk before git-lfs exhausts its retry budget on a stale
// token.
const credentialFileRefreshInterval = 30 * time.Second

// GitCommand returns a git subprocess configured with repository-scoped
// authentication and any per-URL git config overrides disabled.
//
// Callers MUST invoke the returned cleanup (typically via defer) once the
// command has finished. cleanup is always non-nil and safe to call multiple
// times, including when GitCommand returns an error.
func (r *Repository) GitCommand(ctx context.Context, args ...string) (*exec.Cmd, func(), error) {
	cleanup := func() {}

	configArgs, err := getInsteadOfDisableArgsForURL(ctx, r.upstreamURL)
	if err != nil {
		return nil, cleanup, errors.Wrap(err, "get insteadOf disable args")
	}

	var allArgs []string
	allArgs = append(allArgs, configArgs...)

	if r.credentialProvider != nil && strings.Contains(r.upstreamURL, "github.com") {
		token, err := r.credentialProvider.GetTokenForURL(ctx, r.upstreamURL)
		if err == nil && token != "" {
			credFile, fileCleanup, err := r.startTokenCredentialFile(ctx, token)
			if err != nil {
				return nil, cleanup, errors.Wrap(err, "start token credential file")
			}
			cleanup = fileCleanup
			// `!cmd` runs cmd via the shell on every credential query, so a
			// rewrite of credFile by the refresh goroutine is picked up on
			// the next git-lfs retry — which is the whole point: long-running
			// subprocesses can't otherwise observe a token rotation.
			//
			// Git appends the operation (`get`/`store`/`erase`) as a positional
			// argument to the helper command. The function form here both gates
			// on `get` and absorbs the argument, so a bare `cat <credfile>`
			// can't be tricked into also reading a file named `get`/`store`/
			// `erase` from the worktree.
			allArgs = append(allArgs, "-c",
				"credential.helper=!f() { test \"$1\" = get && cat "+shellSingleQuote(credFile)+"; }; f")
		}
	}

	allArgs = append(allArgs, args...)

	return exec.CommandContext(ctx, "git", allArgs...), cleanup, nil
}

// startTokenCredentialFile creates a 0600 temp file containing a git
// credential helper response for the given initial token and spawns a
// goroutine that rewrites it whenever the token rotates, until cleanup is
// called or ctx is cancelled.
func (r *Repository) startTokenCredentialFile(ctx context.Context, initialToken string) (string, func(), error) {
	f, err := os.CreateTemp("", "cachew-git-cred-*")
	if err != nil {
		return "", func() {}, errors.Wrap(err, "create credential file")
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(path) //nolint:gosec // path is from os.CreateTemp
		return "", func() {}, errors.Wrap(err, "close credential file")
	}
	if err := os.Chmod(path, 0o600); err != nil { //nolint:gosec // path is from os.CreateTemp
		_ = os.Remove(path) //nolint:gosec // path is from os.CreateTemp
		return "", func() {}, errors.Wrap(err, "chmod credential file")
	}
	if err := writeCredentialFile(path, initialToken); err != nil {
		_ = os.Remove(path) //nolint:gosec // path is from os.CreateTemp
		return "", func() {}, err
	}

	refreshCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			cancel()
			_ = os.Remove(path) //nolint:gosec // path is from os.CreateTemp
		})
	}

	go r.refreshCredentialFile(refreshCtx, path, initialToken)

	return path, cleanup, nil
}

func (r *Repository) refreshCredentialFile(ctx context.Context, path, current string) {
	logger := logging.FromContext(ctx).With("upstream", r.upstreamURL, "cred_file", path)
	ticker := time.NewTicker(credentialFileRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next, changed, err := r.refreshCredentialFileOnce(ctx, path, current)
			switch {
			case err != nil:
				logger.WarnContext(ctx, "Failed to refresh git credential file", "error", err)
			case changed:
				logger.DebugContext(ctx, "Git credential file refreshed with rotated token")
				current = next
			}
		}
	}
}

func (r *Repository) refreshCredentialFileOnce(ctx context.Context, path, current string) (string, bool, error) {
	token, err := r.credentialProvider.GetTokenForURL(ctx, r.upstreamURL)
	if err != nil {
		return current, false, errors.Wrap(err, "fetch token")
	}
	if token == "" || token == current {
		return current, false, nil
	}
	if err := writeCredentialFile(path, token); err != nil {
		return current, false, err
	}
	return token, true, nil
}

// writeCredentialFile atomically writes the git credential helper response
// for token to path. The on-disk format matches the helper protocol output
// so that `credential.helper=!cat <path>` satisfies a `get` query without
// any shell-level templating of the token value.
func writeCredentialFile(path, token string) error {
	body := "username=x-access-token\npassword=" + token + "\n"
	tmp := path + ".new"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil { //nolint:gosec // path derives from os.CreateTemp
		return errors.Wrap(err, "write temp credential file")
	}
	if err := os.Rename(tmp, path); err != nil { //nolint:gosec // path derives from os.CreateTemp
		_ = os.Remove(tmp) //nolint:gosec // path derives from os.CreateTemp
		return errors.Wrap(err, "rename credential file")
	}
	return nil
}

func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
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
