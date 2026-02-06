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

var (
	sharedManager   *Manager
	sharedManagerMu sync.RWMutex
)

func SetShared(m *Manager) {
	sharedManagerMu.Lock()
	defer sharedManagerMu.Unlock()
	sharedManager = m
}

func GetShared() *Manager {
	sharedManagerMu.RLock()
	defer sharedManagerMu.RUnlock()
	return sharedManager
}

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

func (m *Manager) Config() Config {
	return m.config
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

func WithReadLockReturn[T any](repo *Repository, fn func() (T, error)) (T, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	return fn()
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

func (r *Repository) executeClone(ctx context.Context, config Config) error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	// #nosec G204 - r.upstreamURL and r.path are controlled by us
	args := []string{
		"clone",
		"-c", "http.postBuffer=" + strconv.Itoa(config.GitConfig.PostBuffer),
		"-c", "http.lowSpeedLimit=" + strconv.Itoa(config.GitConfig.LowSpeedLimit),
		"-c", "http.lowSpeedTime=" + strconv.Itoa(int(config.GitConfig.LowSpeedTime.Seconds())),
		r.upstreamURL, r.path,
	}

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

	cmd, err = gitCommand(ctx, r.upstreamURL, "-C", r.path,
		"-c", "http.postBuffer="+strconv.Itoa(config.GitConfig.PostBuffer),
		"-c", "http.lowSpeedLimit="+strconv.Itoa(config.GitConfig.LowSpeedLimit),
		"-c", "http.lowSpeedTime="+strconv.Itoa(int(config.GitConfig.LowSpeedTime.Seconds())),
		"fetch", "--all")
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
	cmd, err := gitCommand(ctx, r.upstreamURL, "-C", r.path,
		"-c", "http.postBuffer="+strconv.Itoa(config.GitConfig.PostBuffer),
		"-c", "http.lowSpeedLimit="+strconv.Itoa(config.GitConfig.LowSpeedLimit),
		"-c", "http.lowSpeedTime="+strconv.Itoa(int(config.GitConfig.LowSpeedTime.Seconds())),
		"remote", "update", "--prune")
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

func (r *Repository) HasCommit(ctx context.Context, ref string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// #nosec G204 - r.path and ref are controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", ref)
	err := cmd.Run()
	return err == nil
}
