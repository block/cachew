package git

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

func (s *Strategy) handleCommitSnapshot(w http.ResponseWriter, r *http.Request, host, repoPath, artifact string) {
	sha := strings.TrimSuffix(artifact, ".tar.zst")
	if !strings.HasSuffix(artifact, ".tar.zst") || !commitSHARe.MatchString(sha) {
		http.Error(w, "expected a full lowercase commit SHA followed by .tar.zst", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	upstream := "https://" + host + "/" + strings.TrimSuffix(repoPath, ".git")
	if _, err := gitclone.RepoPathFromURL(upstream); err != nil {
		http.Error(w, "invalid repository", http.StatusBadRequest)
		return
	}
	ctx, span := tracer.Start(r.Context(), "git.commit_snapshot",
		trace.WithAttributes(attribute.String("cachew.upstream", upstream), attribute.String("cachew.commit", sha)))
	defer span.End()
	logger := logging.FromContext(ctx)
	start := time.Now()
	s.metrics.recordRequest(ctx, "commit-snapshot")
	key := cache.NewKey(upstream + ".commit-snapshot-v1." + sha)
	source := "sha_hit"
	_, err := s.cache.Stat(ctx, key)
	if errors.Is(err, os.ErrNotExist) {
		source = "sha_miss"
		err = s.prepareCommitSnapshot(ctx, upstream, sha, key)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorContext(ctx, "Commit snapshot unavailable", "upstream", upstream, "sha", sha, "error", err)
		http.Error(w, "commit snapshot unavailable", http.StatusBadGateway)
		return
	}
	span.SetAttributes(attribute.String("cachew.source", source))
	if r.Method == http.MethodHead {
		headers, statErr := s.cache.Stat(ctx, key, httputil.ConditionalOptions(r)...)
		if !httputil.ServeCacheStat(w, headers, statErr) {
			http.Error(w, "commit snapshot unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	body, headers, openErr := s.cache.Open(ctx, key, httputil.ConditionalOptions(r)...)
	handled, n, serveErr := httputil.ServeCacheHit(w, headers, body, openErr)
	if !handled {
		http.Error(w, "commit snapshot unavailable", http.StatusServiceUnavailable)
		return
	}
	if serveErr != nil {
		logger.ErrorContext(ctx, "Serve commit snapshot failed", "error", serveErr)
		span.RecordError(serveErr)
	}
	s.metrics.recordSnapshotServe(ctx, source, host+"/"+repoPath, n, time.Since(start))
}

func (s *Strategy) prepareCommitSnapshot(ctx context.Context, upstream, sha string, key cache.Key) error {
	// A disconnected consumer must not abort preparation for the other workers.
	// Keep just one expensive SHA build running per replica, even across repos.
	result := s.commitBuilds.DoChan(key.String(), func() (any, error) {
		buildCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Minute)
		defer cancel()
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		select {
		case s.commitBuildSlot <- struct{}{}:
			defer func() { <-s.commitBuildSlot }()
		case <-buildCtx.Done():
			return nil, errors.WithStack(buildCtx.Err())
		}
		if _, err := s.cache.Stat(buildCtx, key); !errors.Is(err, os.ErrNotExist) {
			return nil, errors.Wrap(err, "check commit snapshot")
		}
		return nil, s.generateCommitSnapshot(buildCtx, upstream, sha, key)
	})
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case res := <-result:
		return errors.WithStack(res.Err)
	}
}

func (s *Strategy) generateCommitSnapshot(ctx context.Context, upstream, sha string, key cache.Key) (returnErr error) {
	ctx, span := tracer.Start(ctx, "git.commit_snapshot.prepare")
	start := time.Now()
	defer func() {
		status := "success" //nolint:goconst // shared metric label vocabulary
		if returnErr != nil {
			status = "error" //nolint:goconst // shared metric label vocabulary
			span.RecordError(returnErr)
			span.SetStatus(codes.Error, returnErr.Error())
		}
		s.metrics.recordOperation(ctx, "commit-snapshot", status, time.Since(start))
		span.End()
	}()
	repo, err := s.cloneManager.GetOrCreate(ctx, upstream)
	if err != nil {
		return errors.Wrap(err, "get mirror")
	}
	if repo.State() != gitclone.StateReady {
		if err := s.ensureCloneReady(ctx, repo); err != nil {
			return errors.Wrap(err, "prepare mirror")
		}
	}
	_, missing, _, err := repo.EnsureRefs(ctx, nil, []string{sha})
	if err != nil {
		return errors.Wrap(err, "ensure snapshot commit")
	}
	if len(missing) != 0 {
		return errors.Errorf("commit %s not available in mirror", sha)
	}
	root := filepath.Join(s.cloneManager.Config().MirrorRoot, ".snapshots")
	if err := os.MkdirAll(root, 0o750); err != nil {
		return errors.Wrap(err, "create snapshot root")
	}
	workDir, err := os.MkdirTemp(root, "commit-*")
	if err != nil {
		return errors.Wrap(err, "create commit snapshot directory")
	}
	defer os.RemoveAll(workDir) //nolint:errcheck // best-effort temporary clone cleanup
	if err := checkoutSnapshotCommit(ctx, repo, workDir, sha); err != nil {
		return err
	}
	if err := materializeSnapshotLFS(ctx, repo, workDir); err != nil {
		return err
	}
	headers := http.Header{client.SnapshotCommitHeader: {sha}}
	return errors.Wrap(snapshot.Create(ctx, s.cache, key, workDir, 0, nil, s.config.ZstdThreads, headers), "publish commit snapshot")
}

func checkoutSnapshotCommit(ctx context.Context, repo *gitclone.Repository, workDir, sha string) error {
	format := "sha1"
	if len(sha) == 64 {
		format = "sha256"
	}
	if err := snapshotGit(ctx, workDir, "init", "--object-format="+format); err != nil {
		return err
	}
	// Fetch a complete depth-one tree instead of a delta from a filtered base:
	// the target can reuse old blobs absent from a blob:none base checkout.
	mirrorURL := (&url.URL{Scheme: "file", Path: repo.Path()}).String()
	if err := repo.WithReadLock(func() error {
		return snapshotGit(ctx, workDir, "fetch", "--depth=1", "--no-tags", mirrorURL, sha)
	}); err != nil {
		return errors.WithStack(err)
	}
	if err := snapshotGit(ctx, workDir, "remote", "add", "origin", repo.UpstreamURL()); err != nil {
		return err
	}
	if err := snapshotGit(ctx, workDir, "checkout", "--detach", sha); err != nil {
		return err
	}
	head, err := mirrorHead(ctx, workDir)
	if err != nil {
		return err
	}
	if head != sha {
		return errors.Errorf("snapshot HEAD %s does not match requested commit %s", head, sha)
	}
	return nil
}

func snapshotGit(ctx context.Context, directory string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", directory}, args...)...) //nolint:gosec // internal Git commands and validated SHAs
	cmd.Env = append(os.Environ(), "GIT_LFS_SKIP_SMUDGE=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "git %s: %s", args[0], output)
	}
	return nil
}

func materializeSnapshotLFS(ctx context.Context, repo *gitclone.Repository, workDir string) error {
	cmd := exec.CommandContext(ctx, "git", "-C", workDir, "lfs", "ls-files") //nolint:gosec // internal temporary path
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrap(err, "discover target LFS files")
	}
	if len(output) == 0 {
		return nil
	}
	if err := snapshotGit(ctx, workDir, "lfs", "install", "--local", "--skip-smudge"); err != nil {
		return err
	}
	fetchCtx, cancel := context.WithTimeout(ctx, lfsFetchTimeout)
	defer cancel()
	fetch, err := repo.GitCommand(fetchCtx, "-C", workDir, "-c", "lfs.fetchrecentalways=false", "lfs", "fetch", "--include=", "--exclude=", "origin", "HEAD")
	if err != nil {
		return errors.Wrap(err, "create target LFS fetch")
	}
	fetch.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	fetch.Cancel = func() error { return syscall.Kill(-fetch.Process.Pid, syscall.SIGKILL) }
	if output, err = fetch.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "fetch target LFS: %s", output)
	}
	if err := snapshotGit(ctx, workDir, "lfs", "fsck", "--objects", "HEAD"); err != nil {
		return err
	}
	return snapshotGit(ctx, workDir, "lfs", "checkout")
}
