package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/snapshot"
)

// GitCmd groups git-aware subcommands that talk directly to cachew's
// /git/ strategy endpoints (not the generic object-store API).
type GitCmd struct {
	Restore GitRestoreCmd `cmd:"" help:"Restore a repository from a cachew git snapshot."`
}

// GitRestoreCmd fetches a git snapshot, extracts it, and optionally applies
// a delta bundle to bring the working copy up to the mirror's current HEAD.
type GitRestoreCmd struct {
	RepoURL     string `arg:"" help:"Repository URL (e.g. https://github.com/org/repo)."`
	Directory   string `arg:"" help:"Target directory for the clone." type:"path"`
	NoBundle    bool   `help:"Skip applying delta bundle."`
	ZstdThreads int    `help:"Threads for zstd decompression (0 = all CPU cores)." default:"0"`
}

func (c *GitRestoreCmd) Run(ctx context.Context, cli *CLI, client *http.Client) error {
	repoPath, err := gitclone.RepoPathFromURL(c.RepoURL)
	if err != nil {
		return errors.Wrap(err, "invalid repository URL")
	}

	snapshotURL := fmt.Sprintf("%s/git/%s/snapshot.tar.zst", cli.URL, repoPath)
	fmt.Fprintf(os.Stderr, "Fetching snapshot from %s\n", snapshotURL) //nolint:forbidigo

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapshotURL, nil)
	if err != nil {
		return errors.Wrap(err, "create snapshot request")
	}

	resp, err := client.Do(req) //nolint:gosec // URL constructed from CLI flags
	if err != nil {
		return errors.Wrap(err, "fetch snapshot")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return errors.Errorf("snapshot request failed with status %d", resp.StatusCode)
	}

	fmt.Fprintf(os.Stderr, "Extracting to %s...\n", c.Directory) //nolint:forbidigo
	if err := snapshot.Extract(ctx, resp.Body, c.Directory, c.ZstdThreads); err != nil {
		return errors.Wrap(err, "extract snapshot")
	}
	fmt.Fprintf(os.Stderr, "Snapshot restored to %s\n", c.Directory) //nolint:forbidigo

	bundleURL := resp.Header.Get("X-Cachew-Bundle-Url")
	if bundleURL == "" || c.NoBundle {
		return nil
	}

	fmt.Fprintf(os.Stderr, "Applying delta bundle...\n") //nolint:forbidigo
	if err := applyBundle(ctx, cli.URL, client, bundleURL, c.Directory); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to apply delta bundle: %v\n", err) //nolint:forbidigo
		return nil
	}
	fmt.Fprintf(os.Stderr, "Delta bundle applied\n") //nolint:forbidigo

	return nil
}

func applyBundle(ctx context.Context, baseURL string, client *http.Client, bundlePath, directory string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+bundlePath, nil)
	if err != nil {
		return errors.Wrap(err, "create bundle request")
	}

	resp, err := client.Do(req) //nolint:gosec // URL constructed from CLI flags
	if err != nil {
		return errors.Wrap(err, "fetch bundle")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return errors.Errorf("bundle request failed with status %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", "cachew-bundle-*.bundle")
	if err != nil {
		return errors.Wrap(err, "create temp bundle file")
	}
	defer os.Remove(tmpFile.Name()) //nolint:errcheck

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
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
