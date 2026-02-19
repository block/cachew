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

	"github.com/block/cachew/internal/logging"
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
	MirrorRoot       string        `hcl:"mirror-root" help:"Directory to store git clones."`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
}

// CredentialProvider provides credentials for git operations.
type CredentialProvider interface {
	GetTokenForURL(ctx context.Context, url string) (string, error)
}

type CredentialProviderProvider func() (CredentialProvider, error)

type Repository struct {
	mu                 sync.RWMutex
	config             Config
	state              State
	path               string
	upstreamURL        string
	lastFetch          time.Time
	lastRefCheck       time.Time
	refCheckValid      bool
	fetchSem           chan struct{}
	credentialProvider CredentialProvider
}

type Manager struct {
	config             Config
	gitTuningConfig    GitTuningConfig
	clones             map[string]*Repository
	clonesMu           sync.RWMutex
	credentialProvider CredentialProvider
}

// ManagerProvider is a function that lazily creates a singleton Manager.
type ManagerProvider func() (*Manager, error)

func NewManagerProvider(ctx context.Context, config Config, credentialProviderProvider CredentialProviderProvider) ManagerProvider {
	return sync.OnceValues(func() (*Manager, error) {
		var credentialProvider CredentialProvider
		if credentialProviderProvider != nil {
			var err error
			credentialProvider, err = credentialProviderProvider()
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		return NewManager(ctx, config, credentialProvider)
	})
}

func NewManager(ctx context.Context, config Config, credentialProvider CredentialProvider) (*Manager, error) {
	if config.MirrorRoot == "" {
		return nil, errors.New("mirror-root is required")
	}

	if config.FetchInterval == 0 {
		config.FetchInterval = 15 * time.Minute
	}

	if config.RefCheckInterval == 0 {
		config.RefCheckInterval = 10 * time.Second
	}

	if err := os.MkdirAll(config.MirrorRoot, 0o750); err != nil {
		return nil, errors.Wrap(err, "create root directory")
	}

	logging.FromContext(ctx).InfoContext(ctx, "Git clone manager initialised",
		"mirror_root", config.MirrorRoot,
		"fetch_interval", config.FetchInterval,
		"ref_check_interval", config.RefCheckInterval)

	return &Manager{
		config:             config,
		gitTuningConfig:    DefaultGitTuningConfig(),
		clones:             make(map[string]*Repository),
		credentialProvider: credentialProvider,
	}, nil
}

func (m *Manager) Config() Config {
	return m.config
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
		state:              StateEmpty,
		config:             m.config,
		path:               clonePath,
		upstreamURL:        upstreamURL,
		fetchSem:           make(chan struct{}, 1),
		credentialProvider: m.credentialProvider,
	}

	headFile := filepath.Join(clonePath, "HEAD")
	if _, err := os.Stat(headFile); err == nil {
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

func (m *Manager) DiscoverExisting(ctx context.Context) ([]*Repository, error) {
	var discovered []*Repository
	err := filepath.Walk(m.config.MirrorRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		headPath := filepath.Join(path, "HEAD")
		if _, statErr := os.Stat(headPath); statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			return errors.Wrap(statErr, "stat HEAD file")
		}

		relPath, err := filepath.Rel(m.config.MirrorRoot, path)
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
			state:              StateReady,
			config:             m.config,
			path:               path,
			upstreamURL:        upstreamURL,
			fetchSem:           make(chan struct{}, 1),
			credentialProvider: m.credentialProvider,
		}
		repo.fetchSem <- struct{}{}

		if err := repo.configureMirror(ctx); err != nil {
			return errors.Wrapf(err, "configure mirror for %s", upstreamURL)
		}

		m.clonesMu.Lock()
		m.clones[upstreamURL] = repo
		m.clonesMu.Unlock()

		discovered = append(discovered, repo)

		return fs.SkipDir
	})

	if err != nil {
		return nil, errors.Wrap(err, "walk root directory")
	}

	return discovered, nil
}

func (m *Manager) clonePathForURL(upstreamURL string) string {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return filepath.Join(m.config.MirrorRoot, "unknown")
	}

	repoPath := strings.TrimSuffix(parsed.Path, ".git")
	return filepath.Join(m.config.MirrorRoot, parsed.Host, repoPath)
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

func (r *Repository) WithReadLock(fn func() error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return fn()
}

func WithReadLockReturn[T any](repo *Repository, fn func() (T, error)) (T, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	return fn()
}

func (r *Repository) Clone(ctx context.Context) error {
	r.mu.Lock()
	if r.state != StateEmpty {
		r.mu.Unlock()
		return nil
	}
	r.state = StateCloning
	r.mu.Unlock()

	err := r.executeClone(ctx)

	r.mu.Lock()
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

func (r *Repository) executeClone(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	config := DefaultGitTuningConfig()
	// #nosec G204 - r.upstreamURL and r.path are controlled by us
	args := []string{
		"clone", "--mirror",
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
		"-c", "http.lowSpeedLimit=" + strconv.Itoa(config.LowSpeedLimit),
		"-c", "http.lowSpeedTime=" + strconv.Itoa(int(config.LowSpeedTime.Seconds())),
		r.upstreamURL, r.path,
	}

	cmd, err := r.gitCommand(ctx, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git clone --mirror: %s", string(output))
	}

	if err := r.configureMirror(ctx); err != nil {
		return errors.Wrap(err, "configure mirror")
	}

	return nil
}

func (r *Repository) configureMirror(ctx context.Context) error {
	configs := []struct {
		key   string
		value string
	}{
		// Protocol
		{"protocol.version", "2"},
		{"uploadpack.allowFilter", "true"},
		{"uploadpack.allowReachableSHA1InWant", "true"},

		// Bitmaps (biggest win for upload-pack)
		{"repack.writeBitmaps", "true"},
		{"pack.useBitmaps", "true"},
		{"pack.useBitmapBoundaryTraversal", "true"},

		// Commit graph (no --changed-paths; Bloom filters do not help upload-pack)
		{"core.commitGraph", "true"},
		{"gc.writeCommitGraph", "true"},
		{"fetch.writeCommitGraph", "true"},

		// Multi-pack-index (avoids full repack on every fetch)
		{"core.multiPackIndex", "true"},

		// Never unpack loose — keep fetched objects as packs
		{"transfer.unpackLimit", "1"},
		{"fetch.unpackLimit", "1"},

		// Disable auto GC — maintenance is explicit
		{"gc.auto", "0"},

		// Pack performance
		{"pack.threads", "0"},
		{"pack.deltaCacheSize", "512m"},
		{"pack.windowMemory", "1g"},
	}

	for _, cfg := range configs {
		// #nosec G204 - r.path is controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", r.path, "config", cfg.key, cfg.value)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "configure %s: %s", cfg.key, string(output))
		}
	}

	return nil
}

func (r *Repository) Fetch(ctx context.Context) error {
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
	defer r.mu.Unlock()

	config := DefaultGitTuningConfig()

	// #nosec G204 - r.path is controlled by us
	cmd, err := r.gitCommand(ctx, "-C", r.path,
		"-c", "http.postBuffer="+strconv.Itoa(config.PostBuffer),
		"-c", "http.lowSpeedLimit="+strconv.Itoa(config.LowSpeedLimit),
		"-c", "http.lowSpeedTime="+strconv.Itoa(int(config.LowSpeedTime.Seconds())),
		"fetch", "--prune", "--prune-tags")
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git fetch: %s", string(output))
	}

	r.lastFetch = time.Now()
	return nil
}

func (r *Repository) EnsureRefsUpToDate(ctx context.Context) error {
	r.mu.Lock()
	if r.refCheckValid && time.Since(r.lastRefCheck) < r.config.RefCheckInterval {
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
		localSHA, exists := localRefs[ref]
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

	err = r.Fetch(ctx)
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
	cmd, err := r.gitCommand(ctx, "ls-remote", r.upstreamURL)
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
