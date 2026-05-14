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
	// (if needed) and pull from origin (if needed) to catch up.
	if len(c.Ref) > 0 {
		if err := c.satisfyRefs(ctx, api); err != nil {
			return err
		}
	}

	return nil
}

// satisfyRefs ensures the working tree contains every requested ref. It
// short-circuits whenever the local clone already has what was asked for,
// avoiding both /ensure-refs and git pull when the snapshot+bundle already
// brought down the requested SHAs.
func (c *GitRestoreCmd) satisfyRefs(ctx context.Context, api *client.Client) error {
	// Fast path: if every ref is pinned to a specific SHA and the local
	// clone already has all those commits, we're done.
	if allPinned(c.Ref) && localHasAllCommits(ctx, c.Directory, c.Ref) {
		fmt.Fprintf(os.Stderr, "All requested refs already present locally\n") //nolint:forbidigo
		return nil
	}

	fmt.Fprintf(os.Stderr, "Ensuring %d ref(s) are fresh for %s\n", len(c.Ref), c.RepoURL) //nolint:forbidigo
	resp, err := api.EnsureGitRefs(ctx, c.RepoURL, c.Ref)
	if err != nil {
		return errors.Wrap(err, "ensure refs")
	}
	if resp.Fetched {
		fmt.Fprintf(os.Stderr, "Server fetched fresh refs from upstream\n") //nolint:forbidigo
	}

	// If the server's resolved SHAs are already in our local clone (e.g.
	// the bundle brought them in), there's nothing new to pull.
	if len(resp.Refs) > 0 && localHasAllCommits(ctx, c.Directory, resp.Refs) {
		fmt.Fprintf(os.Stderr, "Local clone already contains the server's resolved refs\n") //nolint:forbidigo
		return nil
	}

	fmt.Fprintf(os.Stderr, "Pulling from origin...\n") //nolint:forbidigo
	if err := gitPullOrigin(ctx, c.Directory); err != nil {
		return errors.Wrap(err, "git pull from origin")
	}
	return nil
}

// allPinned reports whether every entry in refs has a non-empty SHA.
func allPinned(refs map[string]string) bool {
	for _, sha := range refs {
		if sha == "" {
			return false
		}
	}
	return true
}

// localHasAllCommits reports whether every non-empty SHA in refs exists in
// the working clone's object database. Refs with empty SHAs cause it to
// return false, since there's no SHA to look for.
func localHasAllCommits(ctx context.Context, directory string, refs map[string]string) bool {
	for _, sha := range refs {
		if sha == "" {
			return false
		}
		// #nosec G204 - directory and sha are controlled by us
		if err := exec.CommandContext(ctx, "git", "-C", directory, "cat-file", "-e", sha).Run(); err != nil {
			return false
		}
	}
	return true
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
