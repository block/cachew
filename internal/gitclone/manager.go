package gitclone

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

type State int

var errUnsafeMirrorMetadata = errors.New("unsafe mirror metadata")

const (
	StateEmpty   State = iota // Not cloned yet
	StateCloning              // Clone in progress
	StateReady                // Ready to use
)

// cloneTempPrefix is the prefix for the temporary directory created during a
// clone before it is atomically renamed into place. Interrupted clones (e.g. a
// killed process) can leave these behind, so they are skipped and removed by
// DiscoverExisting on startup.
const cloneTempPrefix = ".clone-"

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
		LowSpeedTime:  60 * time.Second,
	}
}

type Config struct {
	MirrorRoot       string        `hcl:"mirror-root,optional" help:"Directory to store git clones." default:"${CACHEW_STATE}/git-mirrors"`
	FetchInterval    time.Duration `hcl:"fetch-interval,optional" help:"How often to fetch from upstream in minutes." default:"15m"`
	RefCheckInterval time.Duration `hcl:"ref-check-interval,optional" help:"How long to cache ref checks." default:"10s"`
	Maintenance      bool          `hcl:"maintenance,optional" help:"Enable git maintenance scheduling for mirror repos." default:"false"`
	PackThreads      int           `hcl:"pack-threads,optional" help:"Threads for git pack operations (0 = all CPU cores)." default:"0"`
	CloneTimeout     time.Duration `hcl:"clone-timeout,optional" help:"Upper bound for 'git clone --mirror'. Generous by default since large repos can take 30+ minutes." default:"1h"`
	FetchTimeout     time.Duration `hcl:"fetch-timeout,optional" help:"Upper bound for 'git fetch' so a slow upstream cannot block the fetch path indefinitely." default:"5m"`
	LsRemoteTimeout  time.Duration `hcl:"ls-remote-timeout,optional" help:"Upper bound for 'git ls-remote' so a slow upstream cannot block the request path indefinitely." default:"1m"`
	RepackTimeout    time.Duration `hcl:"repack-timeout,optional" help:"Upper bound for 'git repack' so a slow repack on a large repository cannot block the scheduler queue indefinitely." default:"10m"`
	RepackThreads    int           `hcl:"repack-threads,optional" help:"Threads for git repack operations. Limits memory since windowMemory and deltaCacheSize are per-thread. 0 = pack-threads." default:"4"`
}

// CredentialProvider provides credentials for git operations.
type CredentialProvider interface {
	GetTokenForURL(ctx context.Context, url string) (string, error)
}

type CredentialProviderProvider func() (CredentialProvider, error)

type Repository struct {
	// mu guards state and is read-held for minutes during snapshot
	// generation; a queued writer would park every new reader behind it.
	mu          sync.RWMutex
	config      Config
	state       State
	path        string
	upstreamURL string
	// metaMu guards per-request/per-fetch bookkeeping so it never writes mu.
	metaMu             sync.Mutex
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

	if config.PackThreads <= 0 {
		config.PackThreads = runtime.GOMAXPROCS(0)
	}

	if config.CloneTimeout == 0 {
		config.CloneTimeout = 1 * time.Hour
	}

	if config.FetchTimeout == 0 {
		config.FetchTimeout = 5 * time.Minute
	}

	if config.LsRemoteTimeout == 0 {
		config.LsRemoteTimeout = 60 * time.Second
	}

	if config.RepackTimeout == 0 {
		config.RepackTimeout = 10 * time.Minute
	}

	if err := os.MkdirAll(config.MirrorRoot, 0o750); err != nil {
		return nil, errors.Wrap(err, "create root directory")
	}

	if config.Maintenance {
		if err := startMaintenance(ctx); err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Failed to start git maintenance scheduler", "error", err)
		}
	}

	logging.FromContext(ctx).InfoContext(ctx, "Git clone manager initialised", "mirror_root", config.MirrorRoot,
		"fetch_interval", config.FetchInterval, "ref_check_interval", config.RefCheckInterval)

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

func (m *Manager) GetOrCreate(ctx context.Context, upstreamURL string) (*Repository, error) {
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

	clonePath, err := m.clonePathForURL(upstreamURL)
	if err != nil {
		return nil, err
	}

	repo = &Repository{
		state:              StateEmpty,
		config:             m.config,
		path:               clonePath,
		upstreamURL:        upstreamURL,
		fetchSem:           make(chan struct{}, 1),
		credentialProvider: m.credentialProvider,
	}

	headFile := filepath.Join(clonePath, "HEAD")
	if _, err := os.Lstat(headFile); err == nil {
		// Mirrors can survive from an older process or cache restore, so they must cross the same trust boundary as new restores.
		if err := resetMirrorMetadata(ctx, clonePath, repo.upstreamURL); err != nil {
			if !errors.Is(err, errUnsafeMirrorMetadata) {
				return nil, errors.Wrap(err, "sanitize existing mirror")
			}
			if removeErr := os.RemoveAll(clonePath); removeErr != nil {
				return nil, errors.Join(errors.Wrap(err, "sanitize existing mirror"),
					errors.Wrap(removeErr, "remove unsafe existing mirror"))
			}
		} else if err := repo.configureMirror(ctx, clonePath); err != nil {
			return nil, errors.Wrap(err, "configure existing mirror")
		} else {
			repo.state = StateReady
		}
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

		if strings.HasPrefix(info.Name(), cloneTempPrefix) {
			logging.FromContext(ctx).InfoContext(ctx, "Removing leftover clone temp dir", "path", path)
			if rmErr := os.RemoveAll(path); rmErr != nil {
				return errors.Wrapf(rmErr, "remove leftover clone temp dir %s", path)
			}
			return fs.SkipDir
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

		host, repoPath, found := strings.Cut(urlPath, "/")
		if !found {
			return nil
		}
		upstreamURL := "https://" + host + "/" + repoPath

		repo, err := m.GetOrCreate(ctx, upstreamURL)
		if err != nil {
			return errors.Wrapf(err, "discover mirror for %s", upstreamURL)
		}
		if repo.State() == StateReady {
			discovered = append(discovered, repo)
		}

		return fs.SkipDir
	})

	if err != nil {
		return nil, errors.Wrap(err, "walk root directory")
	}

	return discovered, nil
}

// RepoPathFromURL extracts a normalised "host/path" from an upstream Git URL,
// stripping any ".git" suffix.
func RepoPathFromURL(upstreamURL string) (string, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "parse upstream URL")
	}
	if parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || strings.ContainsAny(parsed.Host, `/\`) {
		return "", errors.Errorf("unsupported upstream URL %q", upstreamURL)
	}
	repoPath := strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), "/"), ".git")
	if repoPath == "" {
		return "", errors.Errorf("upstream URL %q has no repository path", upstreamURL)
	}
	for segment := range strings.SplitSeq(repoPath, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.Contains(segment, `\`) {
			return "", errors.Errorf("unsupported upstream URL path %q", parsed.Path)
		}
	}
	repoPath = filepath.Join(parsed.Host, filepath.FromSlash(repoPath))
	if !filepath.IsLocal(repoPath) {
		return "", errors.Errorf("upstream URL path %q is not local", parsed.Path)
	}
	return repoPath, nil
}

func (m *Manager) clonePathForURL(upstreamURL string) (string, error) {
	repoPath, err := RepoPathFromURL(upstreamURL)
	if err != nil {
		return "", err
	}
	return filepath.Join(m.config.MirrorRoot, repoPath), nil
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
	r.metaMu.Lock()
	defer r.metaMu.Unlock()
	return r.lastFetch
}

func (r *Repository) setLastFetch(at time.Time) {
	r.metaMu.Lock()
	defer r.metaMu.Unlock()
	r.lastFetch = at
}

func (r *Repository) NeedsFetch(fetchInterval time.Duration) bool {
	return time.Since(r.LastFetch()) >= fetchInterval
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

// ResetToEmpty transitions the repository back to StateEmpty so that a
// subsequent call to TryStartCloning can re-attempt the clone. Use this
// when a restored snapshot turns out to be corrupt or empty and needs to
// be replaced with a fresh clone.
func (r *Repository) ResetToEmpty() {
	r.mu.Lock()
	r.state = StateEmpty
	r.mu.Unlock()
	r.setLastFetch(time.Time{})
}

// TryStartCloning atomically transitions the repository from StateEmpty to
// StateCloning. Returns true if this goroutine won the transition and should
// proceed with the clone/restore; false if another goroutine already claimed it.
func (r *Repository) TryStartCloning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != StateEmpty {
		return false
	}
	r.state = StateCloning
	return true
}

// WithFetchExclusion runs fn while holding the fetch semaphore, preventing
// concurrent git fetch operations. Use this for operations like tar that
// read the repository directory non-atomically and need a consistent view.
func (r *Repository) WithFetchExclusion(ctx context.Context, fn func() error) error {
	select {
	case <-r.fetchSem:
		defer func() { r.fetchSem <- struct{}{} }()
		return fn()
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "context cancelled waiting for fetch exclusion")
	}
}

// ConfigureMirror sanitizes and configures a git directory at repoPath as a mirror.
func (r *Repository) ConfigureMirror(ctx context.Context, repoPath string) error {
	if err := resetMirrorMetadata(ctx, repoPath, r.upstreamURL); err != nil {
		return errors.Wrap(err, "reset mirror metadata")
	}
	return r.configureMirror(ctx, repoPath)
}

func (r *Repository) configureMirror(ctx context.Context, repoPath string) error {
	if err := configureMirror(ctx, repoPath, r.config.PackThreads); err != nil {
		return errors.Wrap(err, "configure mirror")
	}
	if r.config.Maintenance {
		if err := registerMaintenance(ctx, repoPath); err != nil {
			return errors.Wrap(err, "register maintenance")
		}
	}
	return nil
}

func resetMirrorMetadata(ctx context.Context, repoPath, upstreamURL string) error {
	configPath := filepath.Join(repoPath, "config")
	info, err := os.Lstat(configPath)
	if err != nil {
		return errors.Wrap(err, "stat mirror config")
	}
	if !info.Mode().IsRegular() {
		return errors.Wrap(errUnsafeMirrorMetadata, "mirror config is not a regular file")
	}
	// Cached metadata can invoke commands; retain only storage features needed to read objects, then rebuild trusted settings.
	configValues, err := mirrorConfigValues(ctx, configPath)
	if err != nil {
		return err
	}
	objectFormat := configValues["extensions.objectformat"]
	if objectFormat != "" && objectFormat != "sha1" && objectFormat != "sha256" {
		return errors.Wrapf(errUnsafeMirrorMetadata, "unsupported mirror object format %q", objectFormat)
	}
	refStorage := configValues["extensions.refstorage"]
	if refStorage != "" && refStorage != "files" && refStorage != "reftable" {
		return errors.Wrapf(errUnsafeMirrorMetadata, "unsupported mirror ref storage %q", refStorage)
	}
	// Cleanup must not resolve through a cached symlink and remove files outside the mirror.
	if err := filepath.WalkDir(repoPath, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return errors.Wrapf(errUnsafeMirrorMetadata, "unsafe mirror entry %s", path)
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "validate mirror entries")
	}

	// Git interprets these paths as configuration, hooks, or object-store indirection rather than inert cached data.
	for _, path := range []string{
		filepath.Join(repoPath, ".git"),
		filepath.Join(repoPath, ".lfsconfig"),
		filepath.Join(repoPath, "commondir"),
		filepath.Join(repoPath, "config"),
		filepath.Join(repoPath, "config.worktree"),
		filepath.Join(repoPath, "hooks"),
		filepath.Join(repoPath, "index"),
		filepath.Join(repoPath, "objects", "info", "alternates"),
		filepath.Join(repoPath, "objects", "info", "http-alternates"),
		filepath.Join(repoPath, "modules"),
		filepath.Join(repoPath, "worktrees"),
	} {
		if err := os.RemoveAll(path); err != nil {
			return errors.Wrapf(err, "remove unsafe mirror metadata %s", path)
		}
	}

	repositoryFormat := "0"
	if objectFormat != "" || refStorage != "" {
		repositoryFormat = "1"
	}
	escapedRemoteURL, err := gitConfigValue(upstreamURL)
	if err != nil {
		return err
	}
	config := "[core]\n\trepositoryformatversion = " + repositoryFormat + "\n\tfilemode = true\n\tbare = true\n" +
		"[remote \"origin\"]\n\turl = " + escapedRemoteURL + "\n\tfetch = +refs/*:refs/*\n\tmirror = true\n"
	if objectFormat != "" || refStorage != "" {
		config += "[extensions]\n"
	}
	if objectFormat != "" {
		config += "\tobjectFormat = " + objectFormat + "\n"
	}
	if refStorage != "" {
		config += "\trefStorage = " + refStorage + "\n"
	}
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return errors.Wrap(err, "create mirror config")
	}
	return nil
}

func gitConfigValue(value string) (string, error) {
	if strings.ContainsAny(value, "\x00\r\n") {
		return "", errors.New("mirror origin contains a control character")
	}
	value = strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `"`, `\"`)
	return `"` + value + `"`, nil
}

func mirrorConfigValues(ctx context.Context, configPath string) (map[string]string, error) {
	cmd := exec.CommandContext(ctx, "git", "config", "--file", configPath, "--no-includes", "--null", "--list") // #nosec G204
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrapf(err, "read mirror config: %s", string(output))
	}
	values := map[string]string{}
	for entry := range strings.SplitSeq(string(output), "\x00") {
		key, value, ok := strings.Cut(entry, "\n")
		if ok {
			values[strings.ToLower(key)] = value
		}
	}
	return values, nil
}

// MarkReady transitions the repository to StateReady, indicating it is
// up-to-date and can serve requests from the local mirror.
func (r *Repository) MarkReady() {
	r.mu.Lock()
	r.state = StateReady
	r.mu.Unlock()
	r.setLastFetch(time.Now())
}

// Clone starts one mirror clone. Other callers wait for the result.
func (r *Repository) Clone(ctx context.Context) error {
	return r.clone(ctx, 0)
}

// CloneWithWaitTimeout limits how long a caller waits for another clone.
// The limit does not apply to the caller that starts the clone.
func (r *Repository) CloneWithWaitTimeout(ctx context.Context, waitTimeout time.Duration) error {
	return r.clone(ctx, waitTimeout)
}

func (r *Repository) clone(ctx context.Context, waitTimeout time.Duration) error {
	if r.TryStartCloning() {
		return r.CloneClaimed(ctx)
	}
	if waitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, waitTimeout)
		defer cancel()
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		switch r.State() {
		case StateReady:
			return nil
		case StateEmpty:
			return errors.New("repository clone did not complete")
		case StateCloning:
		}
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "wait for repository clone")
		case <-ticker.C:
		}
	}
}

// CloneClaimed requires the caller to acquire ownership with TryStartCloning first.
func (r *Repository) CloneClaimed(ctx context.Context) error {
	r.mu.Lock()
	if r.state == StateReady {
		r.mu.Unlock()
		return nil
	}
	if r.state != StateCloning {
		r.mu.Unlock()
		return errors.New("repository clone was not claimed")
	}
	r.mu.Unlock()

	err := r.executeClone(ctx)

	r.mu.Lock()
	if err != nil {
		r.state = StateEmpty
		r.mu.Unlock()
		return err
	}

	r.state = StateReady
	r.mu.Unlock()
	r.setLastFetch(time.Now())
	return nil
}

// mirrorConfigSettings returns git config key-value pairs applied to mirror
// clones to optimise upload-pack serving performance.
func mirrorConfigSettings(packThreads int) [][2]string {
	return [][2]string{
		// Protocol
		{"protocol.version", "2"},
		{"uploadpack.allowFilter", "true"},
		{"uploadpack.allowAnySHA1InWant", "true"},
		// Bitmaps
		{"repack.writeBitmaps", "true"},
		{"pack.useBitmaps", "true"},
		{"pack.useBitmapBoundaryTraversal", "true"},
		// Commit graph
		{"core.commitGraph", "true"},
		{"gc.writeCommitGraph", "true"},
		{"fetch.writeCommitGraph", "true"},
		// Multi-pack-index
		{"core.multiPackIndex", "true"},
		// Keep fetched objects as packs
		{"transfer.unpackLimit", "1"},
		{"fetch.unpackLimit", "1"},
		// Disable auto GC
		{"gc.auto", "0"},
		// Pack performance
		{"pack.threads", strconv.Itoa(packThreads)},
		{"pack.deltaCacheSize", "512m"},
		{"pack.windowMemory", "1g"},
		// LFS fetches are I/O-bound; the git-lfs default of 8 is too low.
		// See git-lfs/git-lfs#6241 for an upstream change (unreleased) that
		// raises the default to 3×NCPU.
		{"lfs.concurrenttransfers", "100"},
	}
}

func registerMaintenance(ctx context.Context, repoPath string) error {
	// #nosec G204 - repoPath is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "config", "maintenance.strategy", "incremental")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "set maintenance.strategy: %s", string(output))
	}
	// #nosec G204 - repoPath is controlled by us
	cmd = exec.CommandContext(ctx, "git", "-C", repoPath, "maintenance", "register")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "maintenance register: %s", string(output))
	}
	return nil
}

func startMaintenance(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "git", "maintenance", "start")
	if output, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrapf(err, "maintenance start: %s", string(output))
	}
	return nil
}

func configureMirror(ctx context.Context, repoPath string, packThreads int) error {
	for _, kv := range mirrorConfigSettings(packThreads) {
		// #nosec G204 - repoPath and config values are controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "config", kv[0], kv[1])
		output, err := cmd.CombinedOutput()
		if err != nil {
			return errors.Wrapf(err, "configure %s: %s", kv[0], string(output))
		}
	}
	return nil
}

func (r *Repository) executeClone(ctx context.Context) error {
	parentDir := filepath.Dir(r.path)
	if err := os.MkdirAll(parentDir, 0o750); err != nil {
		return errors.Wrap(err, "create clone directory")
	}

	tmpDir, err := os.MkdirTemp(parentDir, cloneTempPrefix+"*")
	if err != nil {
		return errors.Wrap(err, "create temp clone directory")
	}
	defer os.RemoveAll(tmpDir) //nolint:errcheck // best-effort cleanup on failure

	// git clone --mirror creates a directory inside tmpDir; we point it at a
	// known subdirectory so we can rename it atomically afterwards.
	cloneDest := filepath.Join(tmpDir, "repo")

	cloneCtx, cancel := context.WithTimeout(ctx, r.config.CloneTimeout)
	defer cancel()

	config := DefaultGitTuningConfig()
	// lowSpeedLimit is intentionally omitted: during initial clone of large
	// repos the server-side pack computation can take minutes at near-zero
	// transfer rate, which would trip the speed check. The cloneTimeout
	// provides the overall safety net instead.
	// #nosec G204 - r.upstreamURL and cloneDest are controlled by us
	args := []string{
		"clone", "--mirror",
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
		r.upstreamURL, cloneDest,
	}

	cmd, err := r.GitCommand(cloneCtx, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git clone --mirror: %s", string(output))
	}

	if err := r.ConfigureMirror(ctx, cloneDest); err != nil {
		return errors.WithStack(err)
	}

	if err := os.Rename(cloneDest, r.path); err != nil {
		return errors.Wrap(err, "move clone into place")
	}
	return nil
}

func (r *Repository) Fetch(ctx context.Context) error {
	return r.FetchWithTimeout(ctx, r.config.FetchTimeout)
}

// FetchWithTimeout fetches from upstream with a configurable timeout. Use this
// for catch-up fetches after snapshot restore where the delta may be large and
// the default Config.FetchTimeout is too short.
func (r *Repository) FetchWithTimeout(ctx context.Context, timeout time.Duration) error {
	return r.fetchInternal(ctx, timeout, true, true)
}

// FetchLenient fetches from upstream with the given timeout but without the
// low-speed transfer check. Use this for post-restore catch-up fetches where
// the delta may be very large and GitHub's server-side pack computation can
// stall at near-zero transfer rate for minutes — the same situation that
// executeClone handles by omitting lowSpeedLimit.
func (r *Repository) FetchLenient(ctx context.Context, timeout time.Duration) error {
	return r.fetchInternal(ctx, timeout, false, true)
}

// FetchVerified fetches from upstream, waiting for the fetch semaphore if
// another operation holds it. Unlike Fetch, it never coalesces with a
// concurrent semaphore holder — the holder may not be fetching at all (e.g.
// WithFetchExclusion) or its fetch may have failed — so a nil return
// guarantees this call ran a successful git fetch.
func (r *Repository) FetchVerified(ctx context.Context) error {
	return r.fetchInternal(ctx, r.config.FetchTimeout, true, false)
}

func (r *Repository) fetchInternal(ctx context.Context, timeout time.Duration, enforceSpeedLimit, coalesce bool) error {
	lastFetch := r.LastFetch()
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
			defer func() {
				r.fetchSem <- struct{}{}
			}()
			if coalesce && r.LastFetch().After(lastFetch) {
				return nil
			}
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled before acquiring fetch semaphore")
		}
	}

	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	config := DefaultGitTuningConfig()

	args := []string{
		"-C", r.path,
		"-c", "http.postBuffer=" + strconv.Itoa(config.PostBuffer),
	}
	if enforceSpeedLimit {
		args = append(args,
			"-c", "http.lowSpeedLimit="+strconv.Itoa(config.LowSpeedLimit),
			"-c", "http.lowSpeedTime="+strconv.Itoa(int(config.LowSpeedTime.Seconds())),
		)
	}
	args = append(args, "fetch", "--prune", "--prune-tags")

	cmd, err := r.GitCommand(fetchCtx, args...)
	if err != nil {
		return errors.Wrap(err, "create git command")
	}
	// Start the process in its own process group so we can kill the entire
	// tree (git spawns child processes like git-remote-https that inherit
	// stdout/stderr pipes and prevent CombinedOutput from returning).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "git fetch: %s", string(output))
	}

	r.setLastFetch(time.Now())
	return nil
}

// EnsureRefsUpToDate checks whether the local mirror's refs match upstream.
// If refs are stale it returns NeedsFetch=true so the caller can schedule a
// background fetch via the job scheduler, rather than fetching synchronously
// on the request path.
func (r *Repository) EnsureRefsUpToDate(ctx context.Context) (needsFetch bool, err error) {
	r.metaMu.Lock()
	if r.refCheckValid && time.Since(r.lastRefCheck) < r.config.RefCheckInterval {
		r.metaMu.Unlock()
		return false, nil
	}
	r.refCheckValid = false
	r.metaMu.Unlock()

	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		return false, errors.Wrap(err, "get local refs")
	}

	lsCtx, cancel := context.WithTimeout(ctx, r.config.LsRemoteTimeout)
	defer cancel()

	upstreamRefs, err := r.GetUpstreamRefs(lsCtx)
	if err != nil {
		return false, errors.Wrap(err, "get upstream refs")
	}

	for ref, upstreamSHA := range upstreamRefs {
		if strings.HasSuffix(ref, "^{}") {
			continue
		}
		if !strings.HasPrefix(ref, "refs/heads/") {
			continue
		}
		localSHA, exists := localRefs[ref]
		if !exists || localSHA != upstreamSHA {
			r.metaMu.Lock()
			r.refCheckValid = false
			r.metaMu.Unlock()
			return true, nil
		}
	}

	r.metaMu.Lock()
	r.lastRefCheck = time.Now()
	r.refCheckValid = true
	r.metaMu.Unlock()
	return false, nil
}

// EnsureRefs guarantees the local mirror satisfies the caller's freshness
// requirements. A ref entry is satisfied when the local mirror has the ref
// at the given SHA (or any SHA, if the value is empty). A commit entry is
// satisfied when the commit exists in the local object database, regardless
// of which ref points at it. If anything is unsatisfied, EnsureRefs
// synchronously fetches from upstream once and then re-checks.
//
// Resolved maps each requested ref to its current local SHA (empty if the
// ref is still missing after the fetch). MissingCommits lists requested
// commits that still are not present locally after the fetch.
//
// This bypasses RefCheckInterval because callers are explicitly asserting
// they require these refs/commits to be fresh right now.
func (r *Repository) EnsureRefs(
	ctx context.Context, refs map[string]string, commits []string,
) (resolved map[string]string, missingCommits []string, fetched bool, err error) {
	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		return nil, nil, false, errors.Wrap(err, "get local refs")
	}

	if refsSatisfied(localRefs, refs) {
		missing := r.missingCommitsLocked(ctx, commits)
		if len(missing) == 0 {
			return resolvedRefs(localRefs, refs), nil, false, nil
		}
	}

	if err := r.FetchWithTimeout(ctx, r.config.FetchTimeout); err != nil {
		return nil, nil, false, errors.Wrap(err, "fetch upstream")
	}

	// Invalidate the cached ref-check so the normal transparent path also
	// re-evaluates after our forced fetch.
	r.metaMu.Lock()
	r.refCheckValid = false
	r.metaMu.Unlock()

	localRefs, err = r.GetLocalRefs(ctx)
	if err != nil {
		return nil, nil, true, errors.Wrap(err, "get local refs after fetch")
	}
	return resolvedRefs(localRefs, refs), r.missingCommitsLocked(ctx, commits), true, nil
}

// missingCommitsLocked returns the subset of commits that are absent from
// the local object database. It takes the repository's read lock to stay
// consistent with concurrent fetches.
func (r *Repository) missingCommitsLocked(ctx context.Context, commits []string) []string {
	if len(commits) == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var missing []string
	for _, sha := range commits {
		if sha == "" {
			continue
		}
		// #nosec G204 - r.path and sha are controlled by us
		cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", sha)
		if err := cmd.Run(); err != nil {
			missing = append(missing, sha)
		}
	}
	return missing
}

// refsSatisfied reports whether every ref in expect is present in localRefs at
// the expected SHA (or at any SHA when the expected SHA is empty).
func refsSatisfied(localRefs, expect map[string]string) bool {
	for ref, wantSHA := range expect {
		localSHA, ok := localRefs[ref]
		if !ok {
			return false
		}
		if wantSHA != "" && localSHA != wantSHA {
			return false
		}
	}
	return true
}

func resolvedRefs(localRefs, expect map[string]string) map[string]string {
	if len(expect) == 0 {
		return nil
	}
	out := make(map[string]string, len(expect))
	for ref := range expect {
		out[ref] = localRefs[ref]
	}
	return out
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
	cmd, err := r.GitCommand(ctx, "ls-remote", r.upstreamURL)
	if err != nil {
		return nil, errors.Wrap(err, "create git command")
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.Wrap(err, "git ls-remote")
	}

	return ParseGitRefs(output), nil
}

// Repack consolidates pack files using geometric repacking. Unlike a full
// repack (-a), geometric repacking only merges packs when there is significant
// fragmentation (many small packs), making it orders of magnitude faster on
// large repositories in steady state. The --write-midx and --write-bitmap-index
// flags maintain the multi-pack index and reachability bitmaps for efficient
// serving via git http-backend.
func (r *Repository) Repack(ctx context.Context) error {
	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Geometric repack started", "upstream", r.upstreamURL)

	repackCtx, cancel := context.WithTimeout(ctx, r.config.RepackTimeout)
	defer cancel()

	threads := r.config.RepackThreads
	if threads <= 0 {
		threads = r.config.PackThreads
	}

	// Override pack settings for repack to bound memory usage. The repo-level
	// config uses high values (512m deltaCacheSize, 1g windowMemory) tuned for
	// serving performance with many threads. Repack is a background task that
	// can afford to be slower in exchange for bounded memory.
	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(repackCtx, "git", "-C", r.path,
		"-c", "pack.threads="+strconv.Itoa(threads),
		"-c", "pack.windowMemory=256m",
		"-c", "pack.deltaCacheSize=128m",
		"repack", "-d", "--geometric=2", "--write-midx", "--write-bitmap-index")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		// When git repack is killed (e.g. by our timeout), it cannot clean up
		// the multi-pack-index.lock it holds. A stale lock blocks all future
		// repacks, causing pack fragmentation that eventually degrades snapshot
		// serving performance. Remove it so the next attempt can proceed.
		lockPath := filepath.Join(r.path, "objects", "pack", "multi-pack-index.lock")
		if rmErr := os.Remove(lockPath); rmErr == nil {
			logger.WarnContext(ctx, "Removed stale multi-pack-index.lock after repack failure", "upstream", r.upstreamURL)
		}
		return errors.Wrapf(err, "git repack: %s", string(output))
	}

	logger.InfoContext(ctx, "Geometric repack completed", "upstream", r.upstreamURL)
	return nil
}

func (r *Repository) HasCommit(ctx context.Context, ref string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// #nosec G204 - r.path and ref are controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "-e", ref)
	err := cmd.Run()
	return err == nil
}
