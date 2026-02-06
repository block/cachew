package gomod

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"golang.org/x/mod/semver"

	"github.com/block/cachew/internal/gitclone"
)

type privateFetcher struct {
	logger       *slog.Logger
	cloneManager *gitclone.Manager
}

type moduleInfo struct {
	Version string `json:"Version"`
	Time    string `json:"Time"`
}

func newPrivateFetcher(logger *slog.Logger, cloneManager *gitclone.Manager) *privateFetcher {
	return &privateFetcher{
		logger:       logger,
		cloneManager: cloneManager,
	}
}

func (p *privateFetcher) Query(ctx context.Context, path, query string) (version string, t time.Time, err error) {
	logger := p.logger.With(slog.String("module", path), slog.String("query", query))
	logger.DebugContext(ctx, "Private fetcher: Query")

	gitURL := p.modulePathToGitURL(path)

	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return "", time.Time{}, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureReady(ctx, repo); err != nil {
		return "", time.Time{}, errors.Wrapf(err, "ensure repository ready for %s", gitURL)
	}

	resolvedVersion, commitTime, err := p.resolveVersionQuery(ctx, repo, query)
	if err != nil {
		return "", time.Time{}, errors.Wrapf(err, "resolve version query %s", query)
	}

	return resolvedVersion, commitTime, nil
}

func (p *privateFetcher) List(ctx context.Context, path string) (versions []string, err error) {
	logger := p.logger.With(slog.String("module", path))
	logger.DebugContext(ctx, "Private fetcher: List")

	gitURL := p.modulePathToGitURL(path)
	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return nil, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureReady(ctx, repo); err != nil {
		return nil, errors.Wrapf(err, "ensure repository ready for %s", gitURL)
	}

	versions, err = p.listVersions(ctx, repo)
	if err != nil {
		return nil, errors.Wrap(err, "list versions")
	}

	return versions, nil
}

func (p *privateFetcher) Download(ctx context.Context, path, version string) (info, mod, zip io.ReadSeekCloser, err error) {
	logger := p.logger.With(slog.String("module", path), slog.String("version", version))
	logger.DebugContext(ctx, "Private fetcher: Download")

	gitURL := p.modulePathToGitURL(path)
	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureReady(ctx, repo); err != nil {
		return nil, nil, nil, errors.Wrapf(err, "ensure repository ready for %s", gitURL)
	}

	if err := p.verifyCommitExists(ctx, repo, version); err != nil {
		return nil, nil, nil, err
	}

	infoReader, err := p.generateInfo(ctx, repo, version)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "generate info")
	}

	modReader := p.generateMod(ctx, repo, path, version)

	zipReader, err := p.generateZip(ctx, repo, path, version)
	if err != nil {
		_ = infoReader.Close()
		_ = modReader.Close()
		return nil, nil, nil, errors.Wrap(err, "generate zip")
	}

	return infoReader, modReader, zipReader, nil
}

func (p *privateFetcher) ensureReady(ctx context.Context, repo *gitclone.Repository) error {
	if repo.State() == gitclone.StateReady {
		return nil
	}

	config := p.cloneManager.Config()
	if err := repo.Clone(ctx, config); err != nil {
		return errors.Wrap(err, "clone repository")
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Minute) // reasonable timeout for cloning

	for {
		if repo.State() == gitclone.StateReady {
			return nil
		}

		select {
		case <-ticker.C:
			// Continue polling
		case <-timeout:
			return errors.Errorf("timeout waiting for repository %s to be ready", repo.UpstreamURL())
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for clone")
		}
	}
}

func (p *privateFetcher) modulePathToGitURL(modulePath string) string {
	return "https://" + modulePath
}

func (p *privateFetcher) verifyCommitExists(ctx context.Context, repo *gitclone.Repository, ref string) error {
	if !repo.HasCommit(ctx, ref) {
		return errors.Errorf("commit %s not found in repository %s", ref, repo.UpstreamURL())
	}
	return nil
}

func (p *privateFetcher) resolveVersionQuery(ctx context.Context, repo *gitclone.Repository, query string) (string, time.Time, error) {
	if query == "latest" {
		versions, err := p.listVersions(ctx, repo)
		if err != nil || len(versions) == 0 {
			return p.getDefaultBranchVersion(ctx, repo)
		}

		latestVersion := versions[len(versions)-1]
		commitTime, err := p.getCommitTime(ctx, repo, latestVersion)
		if err != nil {
			return "", time.Time{}, err
		}
		return latestVersion, commitTime, nil
	}

	if semver.IsValid(query) {
		commitTime, err := p.getCommitTime(ctx, repo, query)
		if err != nil {
			return "", time.Time{}, fs.ErrNotExist
		}
		return query, commitTime, nil
	}

	return p.getDefaultBranchVersion(ctx, repo)
}

func (p *privateFetcher) listVersions(ctx context.Context, repo *gitclone.Repository) ([]string, error) {
	output, err := gitclone.WithReadLockReturn(repo, func() ([]byte, error) {
		// #nosec G204 - repo.Path() is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "tag", "-l", "v*")
		return cmd.CombinedOutput()
	})

	if err != nil {
		return nil, errors.Wrapf(err, "git tag failed: %s", string(output))
	}

	var versions []string
	for line := range strings.Lines(string(output)) {
		line = strings.TrimSpace(line)
		if line != "" && semver.IsValid(line) {
			versions = append(versions, line)
		}
	}

	sort.Slice(versions, func(i, j int) bool {
		return semver.Compare(versions[i], versions[j]) < 0
	})

	return versions, nil
}

func (p *privateFetcher) getCommitTime(ctx context.Context, repo *gitclone.Repository, ref string) (time.Time, error) {
	output, err := gitclone.WithReadLockReturn(repo, func() ([]byte, error) {
		// #nosec G204 - repo.Path() and ref are controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "log", "-1", "--format=%cI", ref)
		return cmd.CombinedOutput()
	})

	if err != nil {
		return time.Time{}, errors.Wrapf(err, "git log failed: %s", string(output))
	}

	timeStr := strings.TrimSpace(string(output))
	t, err := time.Parse(time.RFC3339, timeStr)
	return t, errors.Wrap(err, "parse commit time")
}

func (p *privateFetcher) getDefaultBranchVersion(ctx context.Context, repo *gitclone.Repository) (string, time.Time, error) {
	output, err := gitclone.WithReadLockReturn(repo, func() ([]byte, error) {
		// #nosec G204 - repo.Path() is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "rev-parse", "HEAD")
		return cmd.CombinedOutput()
	})

	if err != nil {
		return "", time.Time{}, errors.Wrapf(err, "git rev-parse failed: %s", string(output))
	}

	commitHash := strings.TrimSpace(string(output))
	commitTime, err := p.getCommitTime(ctx, repo, "HEAD")
	if err != nil {
		return "", time.Time{}, err
	}

	pseudoVersion := fmt.Sprintf("v0.0.0-%s-%s",
		commitTime.UTC().Format("20060102150405"),
		commitHash[:12])

	return pseudoVersion, commitTime, nil
}

func (p *privateFetcher) generateInfo(ctx context.Context, repo *gitclone.Repository, version string) (io.ReadSeekCloser, error) {
	commitTime, err := p.getCommitTime(ctx, repo, version)
	if err != nil {
		return nil, err
	}

	info := moduleInfo{
		Version: version,
		Time:    commitTime.Format(time.RFC3339),
	}

	data, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Wrap(err, "marshal module info")
	}

	return newReadSeekCloser(bytes.NewReader(data)), nil
}

func (p *privateFetcher) generateMod(ctx context.Context, repo *gitclone.Repository, modulePath, version string) io.ReadSeekCloser {
	output, err := gitclone.WithReadLockReturn(repo, func() ([]byte, error) {
		// #nosec G204 - version and repo.Path() are controlled by this package, not user input
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "show", fmt.Sprintf("%s:go.mod", version))
		return cmd.CombinedOutput()
	})

	if err != nil {
		minimal := fmt.Sprintf("module %s\n\ngo 1.21\n", modulePath)
		return newReadSeekCloser(bytes.NewReader([]byte(minimal)))
	}

	return newReadSeekCloser(bytes.NewReader(output))
}

func (p *privateFetcher) generateZip(ctx context.Context, repo *gitclone.Repository, modulePath, version string) (io.ReadSeekCloser, error) {
	prefix := fmt.Sprintf("%s@%s/", modulePath, version)
	output, err := gitclone.WithReadLockReturn(repo, func() ([]byte, error) {
		// #nosec G204 - version and repo.Path() are controlled by this package, not user input
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "archive",
			"--format=zip",
			fmt.Sprintf("--prefix=%s", prefix),
			version)
		return cmd.CombinedOutput()
	})

	if err != nil {
		return nil, errors.Wrapf(err, "git archive failed: %s", string(output))
	}

	return newReadSeekCloser(bytes.NewReader(output)), nil
}

type readSeekCloser struct {
	*bytes.Reader
}

func newReadSeekCloser(r *bytes.Reader) io.ReadSeekCloser {
	return &readSeekCloser{Reader: r}
}

func (r *readSeekCloser) Close() error {
	return nil
}
