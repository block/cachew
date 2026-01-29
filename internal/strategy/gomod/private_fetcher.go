package gomod

import (
	"bytes"
	"context"
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
	gomod        *Strategy
	cloneManager *gitclone.Manager
}

func newPrivateFetcher(gomod *Strategy, cloneManager *gitclone.Manager) *privateFetcher {
	return &privateFetcher{
		gomod:        gomod,
		cloneManager: cloneManager,
	}
}

func (p *privateFetcher) Query(ctx context.Context, path, query string) (version string, t time.Time, err error) {
	logger := p.gomod.logger.With(slog.String("module", path), slog.String("query", query))
	logger.DebugContext(ctx, "Private fetcher: Query")

	gitURL := p.modulePathToGitURL(path)

	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return "", time.Time{}, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureCloneReady(ctx, repo); err != nil {
		return "", time.Time{}, errors.Wrapf(err, "ensure clone for %s", path)
	}

	resolvedVersion, commitTime, err := p.resolveVersionQuery(ctx, repo, query)
	if err != nil {
		return "", time.Time{}, errors.Wrapf(err, "resolve version query %s", query)
	}

	return resolvedVersion, commitTime, nil
}

func (p *privateFetcher) List(ctx context.Context, path string) (versions []string, err error) {
	logger := p.gomod.logger.With(slog.String("module", path))
	logger.DebugContext(ctx, "Private fetcher: List")

	gitURL := p.modulePathToGitURL(path)
	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return nil, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureCloneReady(ctx, repo); err != nil {
		return nil, errors.Wrapf(err, "ensure clone for %s", path)
	}

	versions, err = p.listVersions(ctx, repo)
	if err != nil {
		return nil, errors.Wrap(err, "list versions")
	}

	return versions, nil
}

func (p *privateFetcher) Download(ctx context.Context, path, version string) (info, mod, zip io.ReadSeekCloser, err error) {
	logger := p.gomod.logger.With(slog.String("module", path), slog.String("version", version))
	logger.DebugContext(ctx, "Private fetcher: Download")

	gitURL := p.modulePathToGitURL(path)
	repo, err := p.cloneManager.GetOrCreate(ctx, gitURL)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "get or create clone for %s", path)
	}

	if err := p.ensureCloneReady(ctx, repo); err != nil {
		return nil, nil, nil, errors.Wrapf(err, "ensure clone for %s", path)
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

func (p *privateFetcher) modulePathToGitURL(modulePath string) string {
	return "https://" + modulePath
}

// ensureCloneReady ensures the repository is cloned and ready to use.
// It handles the cloning state machine and waits for the clone to complete if necessary.
func (p *privateFetcher) ensureCloneReady(ctx context.Context, repo *gitclone.Repository) error {
	state := repo.State()

	switch state {
	case gitclone.StateEmpty:
		// Need to clone
		gitcloneConfig := gitclone.Config{
			RootDir:          p.gomod.config.MirrorRoot,
			FetchInterval:    p.gomod.config.FetchInterval,
			RefCheckInterval: p.gomod.config.RefCheckInterval,
			CloneDepth:       p.gomod.config.CloneDepth,
			GitConfig:        gitclone.DefaultGitTuningConfig(),
		}
		if err := repo.Clone(ctx, gitcloneConfig); err != nil {
			return errors.Wrap(err, "clone repository")
		}

	case gitclone.StateCloning:
		// Wait for clone to complete
		for {
			time.Sleep(100 * time.Millisecond)
			currentState := repo.State()

			if currentState == gitclone.StateReady {
				break
			}
			if currentState == gitclone.StateEmpty {
				return errors.New("clone failed")
			}

			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "context cancelled waiting for clone")
			default:
			}
		}

	case gitclone.StateReady:
		// Maybe fetch if needed
		if repo.NeedsFetch(p.gomod.config.FetchInterval) {
			gitcloneConfig := gitclone.Config{
				RootDir:          p.gomod.config.MirrorRoot,
				FetchInterval:    p.gomod.config.FetchInterval,
				RefCheckInterval: p.gomod.config.RefCheckInterval,
				CloneDepth:       p.gomod.config.CloneDepth,
				GitConfig:        gitclone.DefaultGitTuningConfig(),
			}
			if err := repo.Fetch(ctx, gitcloneConfig); err != nil {
				p.gomod.logger.WarnContext(ctx, "Failed to fetch updates",
					slog.String("upstream", repo.UpstreamURL()),
					slog.String("error", err.Error()))
			}
		}
	}

	return nil
}

// resolveVersionQuery resolves a version query (like "latest" or "v1.2.3") to a specific version.
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
	var output []byte
	var err error

	repo.WithReadLock(func() {
		// #nosec G204 - repo.Path() is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "tag", "-l", "v*")
		output, err = cmd.CombinedOutput()
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
	var output []byte
	var err error

	repo.WithReadLock(func() {
		// #nosec G204 - repo.Path() and ref are controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "log", "-1", "--format=%cI", ref)
		output, err = cmd.CombinedOutput()
	})

	if err != nil {
		return time.Time{}, errors.Wrapf(err, "git log failed: %s", string(output))
	}

	timeStr := strings.TrimSpace(string(output))
	t, err := time.Parse(time.RFC3339, timeStr)
	return t, errors.Wrap(err, "parse commit time")
}

func (p *privateFetcher) getDefaultBranchVersion(ctx context.Context, repo *gitclone.Repository) (string, time.Time, error) {
	var output []byte
	var err error

	repo.WithReadLock(func() {
		// #nosec G204 - repo.Path() is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "rev-parse", "HEAD")
		output, err = cmd.CombinedOutput()
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

	info := fmt.Sprintf(`{"Version":"%s","Time":"%s"}`, version, commitTime.Format(time.RFC3339))
	return newReadSeekCloser(bytes.NewReader([]byte(info))), nil
}

func (p *privateFetcher) generateMod(ctx context.Context, repo *gitclone.Repository, modulePath, version string) io.ReadSeekCloser {
	var output []byte
	var err error

	repo.WithReadLock(func() {
		// #nosec G204 - version and repo.Path() are controlled by this package, not user input
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "show", fmt.Sprintf("%s:go.mod", version))
		output, err = cmd.CombinedOutput()
	})

	if err != nil {
		minimal := fmt.Sprintf("module %s\n\ngo 1.21\n", modulePath)
		return newReadSeekCloser(bytes.NewReader([]byte(minimal)))
	}

	return newReadSeekCloser(bytes.NewReader(output))
}

func (p *privateFetcher) generateZip(ctx context.Context, repo *gitclone.Repository, modulePath, version string) (io.ReadSeekCloser, error) {
	prefix := fmt.Sprintf("%s@%s/", modulePath, version)
	var output []byte
	var err error

	repo.WithReadLock(func() {
		// #nosec G204 - version and repo.Path() are controlled by this package, not user input
		cmd := exec.CommandContext(ctx, "git", "-C", repo.Path(), "archive",
			"--format=zip",
			fmt.Sprintf("--prefix=%s", prefix),
			version)
		output, err = cmd.CombinedOutput()
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
