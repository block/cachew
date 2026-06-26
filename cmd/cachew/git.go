package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/snapshot"
)

//nolint:gochecknoglobals // OTel tracer instances are package-scoped by convention
var tracer = otel.Tracer("github.com/block/cachew/cmd/cachew")

// inSpan runs fn inside a named child span and records the error returned by
// fn on the span before propagating it.
func inSpan(ctx context.Context, name string, attrs []attribute.KeyValue, fn func(ctx context.Context) error) error {
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	defer span.End()
	if err := fn(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// GitCmd groups git-aware subcommands that talk directly to cachew's
// /git/ strategy endpoints (not the generic object-store API).
type GitCmd struct {
	Restore GitRestoreCmd `cmd:"" help:"Restore a repository from a cachew git snapshot."`
}

// GitRestoreCmd fetches a git snapshot, extracts it, and optionally applies
// a delta bundle. If --ref or --commit is set it then asks the server to
// ensure those refs/commits are fresh and runs `git pull --ff-only` so the
// working tree catches up to upstream.
type GitRestoreCmd struct {
	RepoURL     string            `arg:"" help:"Repository URL (e.g. https://github.com/org/repo)."`
	Directory   string            `arg:"" help:"Target directory for the clone." type:"path"`
	Ref         map[string]string `help:"Required refs to freshen on the server before pulling, in the form 'name=sha' (e.g. 'refs/heads/main=abc123'). An empty SHA means any SHA is acceptable. Setting this (or --commit) runs a final 'git pull' from origin so the working tree is brought up to date."`
	Commit      []string          `help:"Required commit SHAs that must exist on the server, regardless of which ref points at them. May be repeated."`
	NoBundle    bool              `help:"Skip applying delta bundle."`
	ZstdThreads int               `help:"Threads for zstd decompression (0 = all CPU cores)." default:"0"`
	// DownloadConcurrency > 1 fetches the snapshot with that many concurrent
	// range requests (requires server range support; falls back to a single
	// request otherwise). 1 keeps the streaming single-request download.
	DownloadConcurrency int `help:"Concurrent range requests for the snapshot download (1 = single streaming request)." default:"1"`
	DownloadChunkSizeMB int `help:"Chunk size in MiB for parallel snapshot downloads." default:"8"`
}

func (c *GitRestoreCmd) Run(ctx context.Context, api *client.Client) error {
	ctx, span := tracer.Start(ctx, "cachew.git_restore",
		trace.WithAttributes(
			attribute.String("cachew.repo_url", c.RepoURL),
			attribute.String("cachew.directory", c.Directory),
			attribute.Bool("cachew.no_bundle", c.NoBundle),
			attribute.Int("cachew.zstd_threads", c.ZstdThreads),
		),
	)
	defer span.End()

	totalStart := time.Now()
	defer func() {
		fmt.Fprintf(os.Stderr, "cachew git restore total elapsed=%s\n", time.Since(totalStart)) //nolint:forbidigo
	}()

	fmt.Fprintf(os.Stderr, "Fetching snapshot for %s\n", c.RepoURL) //nolint:forbidigo

	commit, bundleURL, err := c.fetchAndExtractSnapshot(ctx, api)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.Errorf("no snapshot available for %s", c.RepoURL)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.Wrap(err, "restore snapshot")
	}
	span.SetAttributes(attribute.String("cachew.snapshot_commit", commit))
	fmt.Fprintf(os.Stderr, "Snapshot restored to %s\n", c.Directory) //nolint:forbidigo

	if bundleURL != "" && !c.NoBundle {
		fmt.Fprintf(os.Stderr, "Applying delta bundle...\n") //nolint:forbidigo
		bundleStart := time.Now()
		if err := applyBundle(ctx, api, bundleURL, c.Directory); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to apply delta bundle: %v\n", err) //nolint:forbidigo
			span.RecordError(err)
		} else {
			fmt.Fprintf(os.Stderr, "Delta bundle applied in %s\n", time.Since(bundleStart)) //nolint:forbidigo
		}
	}

	// Snapshot + bundle leave the working tree at whatever the mirror had
	// when the bundle was last generated, which may be arbitrarily old. If
	// the caller asked for specific refs or commits to be fresh, freshen
	// the mirror (if needed) and pull from origin (if needed) to catch up.
	if len(c.Ref) > 0 || len(c.Commit) > 0 {
		if err := c.satisfyRefs(ctx, api); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	return nil
}

// fetchAndExtractSnapshot downloads the snapshot and extracts it into the target
// directory, returning its freshen metadata (commit and bundle URL). With a
// download concurrency above 1 it downloads in parallel into a temp file, since
// ParallelGet needs a WriterAt; otherwise it streams the single response
// directly into extraction.
func (c *GitRestoreCmd) fetchAndExtractSnapshot(ctx context.Context, api *client.Client) (commit, bundleURL string, err error) {
	if c.DownloadConcurrency > 1 {
		return c.parallelFetchAndExtract(ctx, api)
	}
	return c.streamFetchAndExtract(ctx, api)
}

// streamFetchAndExtract downloads the snapshot in a single request and pipes the
// response body straight into extraction, overlapping download and extraction.
func (c *GitRestoreCmd) streamFetchAndExtract(ctx context.Context, api *client.Client) (string, string, error) {
	var snap *client.GitSnapshot
	if err := inSpan(ctx, "cachew.download_snapshot",
		[]attribute.KeyValue{attribute.String("cachew.repo_url", c.RepoURL)},
		func(ctx context.Context) error {
			downloadStart := time.Now()
			s, err := api.OpenGitSnapshot(ctx, c.RepoURL)
			if err != nil {
				return err //nolint:wrapcheck // wrapped by caller
			}
			snap = s
			trace.SpanFromContext(ctx).SetAttributes(
				attribute.String("cachew.snapshot_commit", s.Commit),
				attribute.String("cachew.bundle_url", s.BundleURL),
				attribute.Float64("cachew.elapsed_seconds", time.Since(downloadStart).Seconds()),
			)
			return nil
		}); err != nil {
		return "", "", err
	}
	defer snap.Close()

	if err := c.extract(ctx, snap.Body); err != nil {
		return "", "", err
	}
	return snap.Commit, snap.BundleURL, nil
}

// parallelFetchAndExtract downloads the snapshot into a temp file using bounded
// concurrent range requests, then extracts from the file. ParallelGet writes via
// WriteAt so it cannot stream into extraction; the temp file is removed on
// return.
func (c *GitRestoreCmd) parallelFetchAndExtract(ctx context.Context, api *client.Client) (string, string, error) {
	// Stage the temp snapshot on the same filesystem as the restore target so a
	// small or separate /tmp can't fail a restore the target directory has room
	// for. The parent of c.Directory shares its filesystem and is created by
	// extraction anyway.
	tmpDir := filepath.Dir(c.Directory)
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return "", "", errors.Wrap(err, "create snapshot temp dir")
	}
	tmp, err := os.CreateTemp(tmpDir, ".cachew-snapshot-*.tar.zst")
	if err != nil {
		return "", "", errors.Wrap(err, "create snapshot temp file")
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name()) //nolint:gosec // name is from os.CreateTemp, not external input
	}()

	var meta client.GitSnapshotMetadata
	if err := inSpan(ctx, "cachew.download_snapshot",
		[]attribute.KeyValue{
			attribute.String("cachew.repo_url", c.RepoURL),
			attribute.Int("cachew.download_concurrency", c.DownloadConcurrency),
			attribute.Int("cachew.download_chunk_size_mb", c.DownloadChunkSizeMB),
		},
		func(ctx context.Context) error {
			downloadStart := time.Now()
			m, err := api.DownloadGitSnapshot(ctx, c.RepoURL, tmp, int64(c.DownloadChunkSizeMB)<<20, c.DownloadConcurrency)
			if err != nil {
				return err //nolint:wrapcheck // wrapped by caller
			}
			meta = m
			trace.SpanFromContext(ctx).SetAttributes(
				attribute.String("cachew.snapshot_commit", m.Commit),
				attribute.String("cachew.bundle_url", m.BundleURL),
				attribute.Float64("cachew.elapsed_seconds", time.Since(downloadStart).Seconds()),
			)
			return nil
		}); err != nil {
		return "", "", err
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return "", "", errors.Wrap(err, "rewind snapshot temp file")
	}
	if err := c.extract(ctx, tmp); err != nil {
		return "", "", err
	}
	return meta.Commit, meta.BundleURL, nil
}

// extract decompresses and unpacks the snapshot body into the target directory.
func (c *GitRestoreCmd) extract(ctx context.Context, body io.Reader) error {
	fmt.Fprintf(os.Stderr, "Extracting to %s...\n", c.Directory) //nolint:forbidigo,gosec // c.Directory is an operator-supplied CLI path
	return inSpan(ctx, "cachew.extract",
		[]attribute.KeyValue{attribute.String("cachew.directory", c.Directory)},
		func(ctx context.Context) error {
			extractStart := time.Now()
			if err := snapshot.Extract(ctx, body, c.Directory, c.ZstdThreads); err != nil {
				return err //nolint:wrapcheck // wrapped by caller
			}
			elapsed := time.Since(extractStart)
			trace.SpanFromContext(ctx).SetAttributes(attribute.Float64("cachew.elapsed_seconds", elapsed.Seconds()))
			fmt.Fprintf(os.Stderr, "Snapshot extracted in %s\n", elapsed) //nolint:forbidigo
			return nil
		})
}

// satisfyRefs ensures the working tree contains every requested ref and
// commit. It short-circuits whenever the local clone already has what was
// asked for, avoiding both /ensure-refs and git pull when the snapshot+bundle
// already brought down the required SHAs.
func (c *GitRestoreCmd) satisfyRefs(ctx context.Context, api *client.Client) error {
	ctx, span := tracer.Start(ctx, "cachew.satisfy_refs",
		trace.WithAttributes(
			attribute.Int("cachew.requested_refs", len(c.Ref)),
			attribute.Int("cachew.requested_commits", len(c.Commit)),
		),
	)
	defer span.End()

	// Fast path: if every ref is pinned and the local clone has every ref
	// SHA and every requested commit, we're done.
	if allPinned(c.Ref) &&
		localHasAllRefSHAs(ctx, c.Directory, c.Ref) &&
		localHasAllSHAs(ctx, c.Directory, c.Commit) {
		fmt.Fprintf(os.Stderr, "All requested refs/commits already present locally\n") //nolint:forbidigo
		span.SetAttributes(attribute.String("cachew.result", "local_hit"))
		return nil
	}

	fmt.Fprintf(os.Stderr, "Ensuring %d ref(s) and %d commit(s) are fresh for %s\n", //nolint:forbidigo
		len(c.Ref), len(c.Commit), c.RepoURL)
	var resp client.EnsureGitRefsResponse
	if err := inSpan(ctx, "cachew.ensure_refs", nil, func(ctx context.Context) error {
		r, err := api.EnsureGitRefs(ctx, c.RepoURL, client.EnsureGitRefsRequest{
			Refs:    c.Ref,
			Commits: c.Commit,
		})
		if err != nil {
			return err //nolint:wrapcheck // wrapped by caller
		}
		resp = r
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.Bool("cachew.fetched", r.Fetched),
			attribute.Int("cachew.missing_commits", len(r.MissingCommits)),
		)
		return nil
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.Wrap(err, "ensure refs")
	}
	if resp.Fetched {
		fmt.Fprintf(os.Stderr, "Server fetched fresh refs from upstream\n") //nolint:forbidigo
	}
	if len(resp.MissingCommits) > 0 {
		return errors.Errorf("server is missing %d commit(s) after fetch: %v",
			len(resp.MissingCommits), resp.MissingCommits)
	}

	// If the server's resolved SHAs and all requested commits are already
	// in our local clone (e.g. the bundle brought them in), there's nothing
	// new to pull. We only treat refs as "satisfied" when the server
	// actually resolved them; an empty resp.Refs (e.g. unknown ref) leaves
	// us no positive evidence, so fall through to the pull.
	refsSatisfied := len(c.Ref) == 0 ||
		(len(resp.Refs) == len(c.Ref) && localHasAllRefSHAs(ctx, c.Directory, resp.Refs))
	commitsSatisfied := localHasAllSHAs(ctx, c.Directory, c.Commit)
	if refsSatisfied && commitsSatisfied {
		fmt.Fprintf(os.Stderr, "Local clone already contains the server's resolved refs and commits\n") //nolint:forbidigo
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

// localHasAllRefSHAs reports whether every non-empty SHA in refs exists in
// the working clone's object database. An empty refs map returns true
// (nothing to check); a ref with an empty SHA causes it to return false
// because we don't know what to look for.
func localHasAllRefSHAs(ctx context.Context, directory string, refs map[string]string) bool {
	for _, sha := range refs {
		if sha == "" {
			return false
		}
		if !localHasSHA(ctx, directory, sha) {
			return false
		}
	}
	return true
}

// localHasAllSHAs reports whether every commit SHA exists in the working
// clone's object database. An empty slice returns true.
func localHasAllSHAs(ctx context.Context, directory string, shas []string) bool {
	for _, sha := range shas {
		if !localHasSHA(ctx, directory, sha) {
			return false
		}
	}
	return true
}

func localHasSHA(ctx context.Context, directory, sha string) bool {
	// #nosec G204 - directory and sha are controlled by us
	return exec.CommandContext(ctx, "git", "-C", directory, "cat-file", "-e", sha).Run() == nil
}

func applyBundle(ctx context.Context, api *client.Client, bundleURL, directory string) error {
	ctx, span := tracer.Start(ctx, "cachew.apply_bundle",
		trace.WithAttributes(
			attribute.String("cachew.bundle_url", bundleURL),
			attribute.String("cachew.directory", directory),
		),
	)
	defer span.End()

	body, err := api.OpenGitBundle(ctx, bundleURL)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.Wrap(err, "fetch bundle")
	}
	defer body.Close()

	tmpFile, err := os.CreateTemp("", "cachew-bundle-*.bundle")
	if err != nil {
		span.RecordError(err)
		return errors.Wrap(err, "create temp bundle file")
	}
	defer os.Remove(tmpFile.Name()) //nolint:errcheck

	downloadStart := time.Now()
	bytes, err := io.Copy(tmpFile, body)
	if err != nil {
		_ = tmpFile.Close()
		span.RecordError(err)
		return errors.Wrap(err, "download bundle")
	}
	span.SetAttributes(
		attribute.Int64("cachew.bundle_bytes", bytes),
		attribute.Float64("cachew.download_seconds", time.Since(downloadStart).Seconds()),
	)
	if err := tmpFile.Close(); err != nil {
		span.RecordError(err)
		return errors.Wrap(err, "close temp bundle file")
	}

	// Determine the current branch so we can pull from the bundle.
	branchCmd := exec.CommandContext(ctx, "git", "-C", directory, "symbolic-ref", "--short", "HEAD") //nolint:gosec
	branchOut, err := branchCmd.Output()
	if err != nil {
		span.RecordError(err)
		return errors.Wrap(err, "determine current branch")
	}
	branch := strings.TrimSpace(string(branchOut))
	span.SetAttributes(attribute.String("cachew.branch", branch))

	// Pull the bundle's branch into the working tree via fast-forward.
	applyStart := time.Now()
	cmd := exec.CommandContext(ctx, "git", "-C", directory, "pull", "--ff-only", tmpFile.Name(), branch) //nolint:gosec
	if output, err := cmd.CombinedOutput(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.Wrapf(err, "git pull from bundle: %s", string(output))
	}
	span.SetAttributes(attribute.Float64("cachew.git_pull_seconds", time.Since(applyStart).Seconds()))

	return nil
}

// gitPullOrigin runs `git pull --ff-only` from the working clone's origin,
// catching the working tree up to any commits made on the requested refs
// after the bundle was generated. The clone's origin is the upstream URL,
// so this respects the user's git insteadOf config to route through cachew.
func gitPullOrigin(ctx context.Context, directory string) error {
	ctx, span := tracer.Start(ctx, "cachew.pull_origin",
		trace.WithAttributes(attribute.String("cachew.directory", directory)),
	)
	defer span.End()
	start := time.Now()
	cmd := exec.CommandContext(ctx, "git", "-C", directory, "pull", "--ff-only") //nolint:gosec
	if output, err := cmd.CombinedOutput(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.Wrapf(err, "git pull: %s", string(output))
	}
	span.SetAttributes(attribute.Float64("cachew.elapsed_seconds", time.Since(start).Seconds()))
	return nil
}
