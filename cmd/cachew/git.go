package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/snapshot"
)

// GitCmd groups git-aware subcommands that talk directly to cachew's
// /git/ strategy endpoints (not the generic object-store API).
type GitCmd struct {
	Restore GitRestoreCmd `cmd:"" help:"Restore a repository from a cachew git snapshot."`
}

// GitRestoreCmd fetches a git snapshot, extracts it, and optionally applies
// a delta bundle. If --ref is set it then asks the server to ensure those
// refs are fresh and runs `git pull --ff-only` so the working tree catches
// up to upstream.
type GitRestoreCmd struct {
	RepoURL     string            `arg:"" help:"Repository URL (e.g. https://github.com/org/repo)."`
	Directory   string            `arg:"" help:"Target directory for the clone." type:"path"`
	Ref         map[string]string `help:"Required refs to freshen on the server before pulling, in the form 'name=sha' (e.g. 'refs/heads/main=abc123'). An empty SHA means any SHA is acceptable. Setting this also runs a final 'git pull' from origin so the working tree is brought up to date."`
	NoBundle    bool              `help:"Skip applying delta bundle."`
	ZstdThreads int               `help:"Threads for zstd decompression (0 = all CPU cores)." default:"0"`
}

func (c *GitRestoreCmd) Run(ctx context.Context, api *client.Client) error {
	fmt.Fprintf(os.Stderr, "Fetching snapshot for %s\n", c.RepoURL) //nolint:forbidigo

	snap, err := api.OpenGitSnapshot(ctx, c.RepoURL)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.Errorf("no snapshot available for %s", c.RepoURL)
		}
		return errors.Wrap(err, "fetch snapshot")
	}
	defer snap.Close()

	fmt.Fprintf(os.Stderr, "Extracting to %s...\n", c.Directory) //nolint:forbidigo
	if err := snapshot.Extract(ctx, snap.Body, c.Directory, c.ZstdThreads); err != nil {
		return errors.Wrap(err, "extract snapshot")
	}
	fmt.Fprintf(os.Stderr, "Snapshot restored to %s\n", c.Directory) //nolint:forbidigo

	if snap.BundleURL != "" && !c.NoBundle {
		fmt.Fprintf(os.Stderr, "Applying delta bundle...\n") //nolint:forbidigo
		if err := applyBundle(ctx, api, snap.BundleURL, c.Directory); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to apply delta bundle: %v\n", err) //nolint:forbidigo
		} else {
			fmt.Fprintf(os.Stderr, "Delta bundle applied\n") //nolint:forbidigo
		}
	}

	// Snapshot + bundle leave the working tree at whatever the mirror had
	// when the bundle was last generated, which may be arbitrarily old. If
	// the caller asked for specific refs to be fresh, freshen the mirror
	// and then pull from origin to catch the working tree up.
	if len(c.Ref) > 0 {
		fmt.Fprintf(os.Stderr, "Ensuring %d ref(s) are fresh for %s\n", len(c.Ref), c.RepoURL) //nolint:forbidigo
		resp, err := api.EnsureGitRefs(ctx, c.RepoURL, c.Ref)
		if err != nil {
			return errors.Wrap(err, "ensure refs")
		}
		if resp.Fetched {
			fmt.Fprintf(os.Stderr, "Server fetched fresh refs from upstream\n") //nolint:forbidigo
		}

		fmt.Fprintf(os.Stderr, "Pulling from origin...\n") //nolint:forbidigo
		if err := gitPullOrigin(ctx, c.Directory); err != nil {
			return errors.Wrap(err, "git pull from origin")
		}
	}

	return nil
}

func applyBundle(ctx context.Context, api *client.Client, bundleURL, directory string) error {
	body, err := api.OpenGitBundle(ctx, bundleURL)
	if err != nil {
		return errors.Wrap(err, "fetch bundle")
	}
	defer body.Close()

	tmpFile, err := os.CreateTemp("", "cachew-bundle-*.bundle")
	if err != nil {
		return errors.Wrap(err, "create temp bundle file")
	}
	defer os.Remove(tmpFile.Name()) //nolint:errcheck

	if _, err := io.Copy(tmpFile, body); err != nil {
		_ = tmpFile.Close()
		return errors.Wrap(err, "download bundle")
	}
	if err := tmpFile.Close(); err != nil {
		return errors.Wrap(err, "close temp bundle file")
	}

	// Determine the current branch so we can pull from the bundle.
	branchCmd := exec.CommandContext(ctx, "git", "-C", directory, "symbolic-ref", "--short", "HEAD") //nolint:gosec
	branchOut, err := branchCmd.Output()
	if err != nil {
		return errors.Wrap(err, "determine current branch")
	}
	branch := strings.TrimSpace(string(branchOut))

	// Pull the bundle's branch into the working tree via fast-forward.
	cmd := exec.CommandContext(ctx, "git", "-C", directory, "pull", "--ff-only", tmpFile.Name(), branch) //nolint:gosec
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "git pull from bundle: %s", string(output))
	}

	return nil
}

// gitPullOrigin runs `git pull --ff-only` from the working clone's origin,
// catching the working tree up to any commits made on the requested refs
// after the bundle was generated. The clone's origin is the upstream URL,
// so this respects the user's git insteadOf config to route through cachew.
func gitPullOrigin(ctx context.Context, directory string) error {
	cmd := exec.CommandContext(ctx, "git", "-C", directory, "pull", "--ff-only") //nolint:gosec
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "git pull: %s", string(output))
	}
	return nil
}
