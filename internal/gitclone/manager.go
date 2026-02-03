package gitclone

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
)

type State int

const (
	StateEmpty   State = iota // Not cloned yet
	StateCloning              // Clone in progress
	StateReady                // Ready to use
)

func (s State) String() string {
	switch s {
	case StateEmpty:
		return "empty"
	case StateCloning:
		return "cloning"
	case StateReady:
		return "ready"
	default:
		return "unknown"
	}
}

type GitTuningConfig struct {
	PostBuffer    int           // http.postBuffer size in bytes
	LowSpeedLimit int           // http.lowSpeedLimit in bytes/sec
	LowSpeedTime  time.Duration // http.lowSpeedTime
}

func DefaultGitTuningConfig() GitTuningConfig {
	return GitTuningConfig{
		PostBuffer:    524288000, // 500MB buffer
		LowSpeedLimit: 1000,      // 1KB/s minimum speed
		LowSpeedTime:  10 * time.Minute,
	}
}

type Config struct {
	RootDir          string
	FetchInterval    time.Duration
	RefCheckInterval time.Duration
	CloneDepth       int
	GitConfig        GitTuningConfig
}

type Repository struct {
	mu            sync.RWMutex
	state         State
	path          string
	upstreamURL   string
	lastFetch     time.Time
	lastRefCheck  time.Time
	refCheckValid bool
	fetchSem      chan struct{}
}

type Manager struct {
	config   Config
	clones   map[string]*Repository
	clonesMu sync.RWMutex
}

func NewManager(_ context.Context, config Config) (*Manager, error) {
	if config.RootDir == "" {
		return nil, errors.New("RootDir is required")
	}

	if err := os.MkdirAll(config.RootDir, 0o750); err != nil {
		return nil, errors.Wrap(err, "create root directory")
	}

	return &Manager{
		config: config,
		clones: make(map[string]*Repository),
	}, nil
}

func (m *Manager) GetOrCreate(_ context.Context, upstreamURL string) (*Repository, error) {
	m.clonesMu.RLock()
	repo, exists := m.clones[upstreamURL]
	m.clonesMu.RUnlock()

	if exists {
		return repo, nil
	}

	m.clonesMu.Lock()
	defer m.clonesMu.Unlock()

	if repo, exists = m.clones[upstreamURL]; exists {
		return repo, nil
	}

	clonePath := m.clonePathForURL(upstreamURL)

	repo = &Repository{
		state:       StateEmpty,
		path:        clonePath,
		upstreamURL: upstreamURL,
		fetchSem:    make(chan struct{}, 1),
	}

	gitDir := filepath.Join(clonePath, ".git")
	if _, err := os.Stat(gitDir); err == nil {
		repo.state = StateReady
	}

	repo.fetchSem <- struct{}{}

	m.clones[upstreamURL] = repo
	return repo, nil
}

func (m *Manager) Get(upstreamURL string) *Repository {
	m.clonesMu.RLock()
	defer m.clonesMu.RUnlock()
	return m.clones[upstreamURL]
}

func (m *Manager) DiscoverExisting(_ context.Context) error {
	err := filepath.Walk(m.config.RootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		gitDir := filepath.Join(path, ".git")
		headPath := filepath.Join(path, ".git", "HEAD")
		if _, statErr := os.Stat(gitDir); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat .git directory")
		}
		if _, statErr := os.Stat(headPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat HEAD file")
		}

		relPath, err := filepath.Rel(m.config.RootDir, path)
		if err != nil {
			return errors.Wrap(err, "get relative path")
		}

		urlPath := filepath.ToSlash(relPath)

		idx := strings.Index(urlPath, "/")
		if idx == -1 {
			return nil
		}

		host := urlPath[:idx]
		repoPath := urlPath[idx+1:]
		upstreamURL := "https://" + host + "/" + repoPath

		repo := &Repository{
			state:       StateReady,
			path:        path,
			upstreamURL: upstreamURL,
			fetchSem:    make(chan struct{}, 1),
		}
		repo.fetchSem <- struct{}{}

		m.clonesMu.Lock()
		m.clones[upstreamURL] = repo
		m.clonesMu.Unlock()

		return fs.SkipDir
	})

	if err != nil {
		return errors.Wrap(err, "walk root directory")
	}

	return nil
}

func (m *Manager) clonePathForURL(upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return filepath.Join(m.config.RootDir, "unknown")
	}

	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(m.config.RootDir, parsed.Host, repoPath)
}

func (r *Repository) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *Repository) Path() string {
	return r.path
}

func (r *Repository) UpstreamURL() string {
	return r.upstreamURL
}

func (r *Repository) LastFetch() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastFetch
}

func (r *Repository) NeedsFetch(fetchInterval time.Duration) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return time.Since(r.lastFetch) >= fetchInterval
}

func (r *Repository) WithReadLock(fn func()) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn()
}

func (r *Repository) Clone(ctx context.Context, config Config) error {
	r.mu.Lock()
	if r.state != StateEmpty {
		r.mu.Unlock()
		return nil
	}
	r.state = StateCloning

	err := r.executeClone(ctx, config)

	if err != nil {
		r.state = StateEmpty
		r.mu.Unlock()
		return err
	}

	r.state = StateReady
	r.lastFetch = time.Now()
	r.mu.Unlock()
	return nil
}

func gitConfigArgs(config GitTuningConfig) []string {
	return []string{
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
		"-c", "http.lowSpeedLimit=" + strconv.Itoa(config.LowSpeedLimit),
		"-c", "http.lowSpeedTime=" + strconv.Itoa(int(config.LowSpeedTime.Seconds())),
	}
}

func (r *Repository) executeClone(ctx context.Context, config Config) error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	// #nosec G204 - r.upstreamURL and r.path are controlled by us
	args := []string{"clone"}
	if config.CloneDepth > 0 {
		args = append(args, "--depth", strconv.Itoa(config.CloneDepth))
	}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, r.upstreamURL, r.path)

	cmd, err := gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git clone: %s", string(output))
	}

	// #nosec G204 - r.path is controlled by us
	cmd = exec.CommandContext(ctx, "git", "-C", r.path, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*")
	output, err = cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "configure fetch refspec: %s", string(output))
	}

	args = []string{"-C", r.path}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, "fetch", "--all")
	cmd, err = gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command for fetch")
	}
	output, err = cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "fetch all branches: %s", string(output))
	}

	return nil
}

func (r *Repository) Fetch(ctx context.Context, config Config) error {
	select {
	case <-r.fetchSem:
		defer func() {
			r.fetchSem <- struct{}{}
		}()
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
	default:
		select {
		case <-r.fetchSem:
			r.fetchSem <- struct{}{}
			return nil
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for fetch")
		}
	}

	r.mu.Lock()

	// #nosec G204 - r.path is controlled by us
	args := []string{"-C", r.path}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, "remote", "update", "--prune")
	cmd, err := gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git remote update: %s", string(output))
	}

	r.lastFetch = time.Now()
	r.mu.Unlock()

	return nil
}

func (r *Repository) EnsureRefsUpToDate(ctx context.Context, config Config) error {
	r.mu.Lock()
	if r.refCheckValid && time.Since(r.lastRefCheck) < config.RefCheckInterval {
		r.mu.Unlock()
		return nil
	}
	r.lastRefCheck = time.Now()
	r.refCheckValid = true
	r.mu.Unlock()

	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		return errors.Wrap(err, "get local refs")
	}

	upstreamRefs, err := r.GetUpstreamRefs(ctx)
	if err != nil {
		return errors.Wrap(err, "get upstream refs")
	}

	needsFetch := false
	for ref, upstreamSHA := range upstreamRefs {
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		if !strings.HasPrefix(ref, "refs/heads/") {
			continue
		}
		localRef := "refs/remotes/origin/" + strings.TrimPrefix(ref, "refs/heads/")
		localSHA, exists := localRefs[localRef]
		if !exists || localSHA != upstreamSHA {
			needsFetch = true
			break
		}
	}

	if !needsFetch {
		r.mu.Lock()
		r.refCheckValid = true
		r.mu.Unlock()
		return nil
	}

	err = r.Fetch(ctx, config)
	if err != nil {
		r.mu.Lock()
		r.refCheckValid = false
		r.mu.Unlock()
	}
	return err
}

func (r *Repository) GetLocalRefs(ctx context.Context) (map[string]string, error) {
	var output []byte
	var err error

	r.mu.RLock()
	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "for-each-ref", "--format=%(objectname) %(refname)")
	output, err = cmd.CombinedOutput()
	r.mu.RUnlock()

	if err != nil {
		return nil, errors.Wrap(err, "git for-each-ref")
	}

	return ParseGitRefs(output), nil
}

func (r *Repository) GetUpstreamRefs(ctx context.Context) (map[string]string, error) {
	// #nosec G204 - r.upstreamURL is controlled by us
	cmd, err := gitCommand(ctx, r.upstreamURL, "ls-remote", r.upstreamURL)
	if err != nil {
		return nil, errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git ls-remote")
	}

	return ParseGitRefs(output), nil
}

type CommitError struct {
	SHA        string
	NotFound   bool  // doesn't exist anywhere (locally or upstream)
	NotFetched bool  // exists upstream but not locally
	Err        error // underlying error
}

func (e *CommitError) Error() string {
	if e.NotFound {
		return "commit " + e.SHA + " not found"
	}
	if e.NotFetched {
		return "commit " + e.SHA + " exists upstream but not fetched locally"
	}
	if e.Err != nil {
		return "commit " + e.SHA + ": " + e.Err.Error()
	}
	return "commit " + e.SHA + ": unknown error"
}

func (e *CommitError) Unwrap() error {
	return e.Err
}

func (r *Repository) HasCommit(ctx context.Context, sha string) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state != StateReady {
		return false, errors.New("repository not ready")
	}

	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", sha)
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, errors.Wrap(err, "git cat-file")
	}

	return true, nil
}

func (r *Repository) CommitExistsUpstream(ctx context.Context, sha string) (bool, error) {
	// #nosec G204 - r.upstreamURL is controlled by us
	cmd, err := gitCommand(ctx, r.upstreamURL, "ls-remote", r.upstreamURL, sha)
	if err != nil {
		return false, errors.Wrap(err, "create git command")
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, errors.Wrap(err, "git ls-remote")
	}

	return len(strings.TrimSpace(string(output))) > 0, nil
}

func (r *Repository) FetchCommit(ctx context.Context, sha string, config Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateReady {
		return errors.New("repository not ready")
	}

	shallowFile := filepath.Join(r.path, ".git", "shallow")
	isShallow := false
	if _, err := os.Stat(shallowFile); err == nil {
		isShallow = true
	}

	// #nosec G204 - r.path is controlled by us
	args := []string{"-C", r.path}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, "fetch", "origin", sha)

	// For shallow clones, we need to use --depth or --deepen to fetch specific commits
	if isShallow {
		args = append(args, "--depth=1")
	}

	cmd, err := gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		if isShallow && strings.Contains(string(output), "shallow") {
			r.mu.Unlock()
			unshallowErr := r.Unshallow(ctx, config)
			r.mu.Lock()
			if unshallowErr != nil {
				return errors.Wrapf(err, "git fetch commit %s failed and unshallow also failed: %v: %s", sha, unshallowErr, string(output))
			}

			args = []string{"-C", r.path}
			args = append(args, gitConfigArgs(config.GitConfig)...)
			args = append(args, "fetch", "origin", sha)
			cmd, err = gitCommand(ctx, r.upstreamURL, args...)
			if err != nil {
				return errors.Wrap(err, "create git command for retry")
			}
			output, err = cmd.CombinedOutput()
			if err != nil {
				return errors.Wrapf(err, "git fetch commit %s after unshallow: %s", sha, string(output))
			}
			return nil
		}
		return errors.Wrapf(err, "git fetch commit %s: %s", sha, string(output))
	}

	return nil
}

func (r *Repository) ResolveCommit(ctx context.Context, sha string, config Config) error {
	hasCommit, err := r.HasCommit(ctx, sha)
	if err != nil {
		return &CommitError{SHA: sha, Err: err}
	}

	if hasCommit {
		return nil
	}

	existsUpstream, err := r.CommitExistsUpstream(ctx, sha)
	if err != nil {
		return &CommitError{SHA: sha, Err: err}
	}

	if !existsUpstream {
		return &CommitError{SHA: sha, NotFound: true}
	}

	if err := r.FetchCommit(ctx, sha, config); err != nil {
		return &CommitError{SHA: sha, NotFetched: true, Err: err}
	}

	return nil
}

func (r *Repository) FetchRef(ctx context.Context, ref string, config Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateReady {
		return errors.New("repository not ready")
	}

	// #nosec G204 - r.path is controlled by us
	args := []string{"-C", r.path}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, "fetch", "origin", ref)
	cmd, err := gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git fetch ref %s: %s", ref, string(output))
	}

	return nil
}

func (r *Repository) Unshallow(ctx context.Context, config Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateReady {
		return errors.New("repository not ready")
	}

	shallowFile := filepath.Join(r.path, ".git", "shallow")
	if _, err := os.Stat(shallowFile); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	// #nosec G204 - r.path is controlled by us
	args := []string{"-C", r.path}
	args = append(args, gitConfigArgs(config.GitConfig)...)
	args = append(args, "fetch", "--unshallow")
	cmd, err := gitCommand(ctx, r.upstreamURL, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git fetch --unshallow: %s", string(output))
	}

	return nil
}
