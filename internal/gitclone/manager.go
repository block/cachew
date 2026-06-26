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

	FullRepackTimeout time.Duration `hcl:"full-repack-timeout,optional" help:"Upper bound for the full (delta-recomputing) repack, which is far slower than the geometric repack on large repositories. Size it to complete the largest mirror, since a timeout kills the job and wastes the work; it also holds a scheduler slot for its whole duration, so pair it with a slow full-repack-interval. 0 falls back to repack-timeout." default:"6h"`
}

// Delta search window and chain depth for the full repack. A wider window finds
// tighter deltas (smaller packs) at the cost of more CPU.
const (
	fullRepackWindow = 100
	fullRepackDepth  = 50
)

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

		repo := &Repository{
			state:              StateReady,
			config:             m.config,
			path:               path,
			upstreamURL:        upstreamURL,
			fetchSem:           make(chan struct{}, 1),
			credentialProvider: m.credentialProvider,
		}
		repo.fetchSem <- struct{}{}

		if err := configureMirror(ctx, path, m.config.PackThreads); err != nil {
			return errors.Wrapf(err, "configure mirror for %s", upstreamURL)
		}

		if m.config.Maintenance {
			if err := registerMaintenance(ctx, path); err != nil {
				return errors.Wrapf(err, "register maintenance for %s", upstreamURL)
			}
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

// RepoPathFromURL extracts a normalised "host/path" from an upstream Git URL,
// stripping any ".git" suffix.
func RepoPathFromURL(upstreamURL string) (string, error) {
	parsed, err := url.Parse(upstreamURL)
	if err != nil {
		return "", errors.Wrap(err, "parse upstream URL")
	}
	return filepath.Join(parsed.Host, strings.TrimSuffix(parsed.Path, ".git")), nil
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

// ResetToEmpty transitions the repository back to StateEmpty so that a
// subsequent call to TryStartCloning can re-attempt the clone. Use this
// when a restored snapshot turns out to be corrupt or empty and needs to
// be replaced with a fresh clone.
func (r *Repository) ResetToEmpty() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = StateEmpty
	r.lastFetch = time.Time{}
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

// ConfigureMirror configures a git directory at repoPath as a mirror with
// the repository's pack threads and optional maintenance settings.
func (r *Repository) ConfigureMirror(ctx context.Context, repoPath string) error {
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

// MarkReady transitions the repository to StateReady, indicating it is
// up-to-date and can serve requests from the local mirror.
func (r *Repository) MarkReady() {
	r.mu.Lock()
	r.state = StateReady
	r.lastFetch = time.Now()
	r.mu.Unlock()
}

func (r *Repository) Clone(ctx context.Context) error {
	r.mu.Lock()
	if r.state == StateReady {
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
	return r.fetchInternal(ctx, timeout, true)
}

// FetchLenient fetches from upstream with the given timeout but without the
// low-speed transfer check. Use this for post-restore catch-up fetches where
// the delta may be very large and GitHub's server-side pack computation can
// stall at near-zero transfer rate for minutes — the same situation that
// executeClone handles by omitting lowSpeedLimit.
func (r *Repository) FetchLenient(ctx context.Context, timeout time.Duration) error {
	return r.fetchInternal(ctx, timeout, false)
}

func (r *Repository) fetchInternal(ctx context.Context, timeout time.Duration, enforceSpeedLimit bool) error {
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

	r.mu.Lock()
	r.lastFetch = time.Now()
	r.mu.Unlock()
	return nil
}

// EnsureRefsUpToDate checks whether the local mirror's refs match upstream.
// If refs are stale it returns NeedsFetch=true so the caller can schedule a
// background fetch via the job scheduler, rather than fetching synchronously
// on the request path (which would acquire a write lock and block all serving).
func (r *Repository) EnsureRefsUpToDate(ctx context.Context) (needsFetch bool, err error) {
	r.mu.Lock()
	if r.refCheckValid && time.Since(r.lastRefCheck) < r.config.RefCheckInterval {
		r.mu.Unlock()
		return false, nil
	}
	r.lastRefCheck = time.Now()
	r.refCheckValid = true
	r.mu.Unlock()

	localRefs, err := r.GetLocalRefs(ctx)
	if err != nil {
		return false, errors.Wrap(err, "get local refs")
	}

	lsCtx, cancel := context.WithTimeout(ctx, r.config.LsRemoteTimeout)
	defer cancel()

	upstreamRefs, err := r.GetUpstreamRefs(lsCtx)
	if err != nil {
		r.mu.Lock()
		r.refCheckValid = false
		r.mu.Unlock()
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
			r.mu.Lock()
			r.refCheckValid = false
			r.mu.Unlock()
			return true, nil
		}
	}

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
	r.mu.Lock()
	r.refCheckValid = false
	r.mu.Unlock()

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
	if err := r.runRepack(ctx, r.config.RepackTimeout,
		"-d", "--geometric=2", "--write-midx", "--write-bitmap-index"); err != nil {
		return err
	}
	logger.InfoContext(ctx, "Geometric repack completed", "upstream", r.upstreamURL)
	return nil
}

// RepackFull runs a full repack that re-selects deltas across all objects with
// a wide search window (-a -d -f --window --depth). Unlike the geometric
// Repack, which reuses existing deltas and only consolidates packs, this
// recovers the cross-pack redundancy that accumulates from incremental fetches
// — materially shrinking the mirror, and therefore the snapshots derived from
// it, at the cost of significant one-time CPU. It is meant to run on a slow
// cadence in between the frequent geometric repacks.
func (r *Repository) RepackFull(ctx context.Context) error {
	logger := logging.FromContext(ctx)

	timeout := r.config.FullRepackTimeout
	if timeout <= 0 {
		timeout = r.config.RepackTimeout
	}

	logger.InfoContext(ctx, "Full repack started", "upstream", r.upstreamURL, "window", fullRepackWindow, "depth", fullRepackDepth)
	if err := r.runRepack(ctx, timeout,
		"-a", "-d", "-f",
		"--window="+strconv.Itoa(fullRepackWindow), "--depth="+strconv.Itoa(fullRepackDepth),
		"--write-midx", "--write-bitmap-index"); err != nil {
		return err
	}
	logger.InfoContext(ctx, "Full repack completed", "upstream", r.upstreamURL)
	return nil
}

// runRepack executes "git repack <args>" with bounded threads/memory and a
// timeout, cleaning up a stale multi-pack-index.lock on failure.
func (r *Repository) runRepack(ctx context.Context, timeout time.Duration, repackArgs ...string) error {
	logger := logging.FromContext(ctx)

	repackCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	threads := r.config.RepackThreads
	if threads <= 0 {
		threads = r.config.PackThreads
	}

	// Override pack settings for repack to bound memory usage. The repo-level
	// config uses high values (512m deltaCacheSize, 1g windowMemory) tuned for
	// serving performance with many threads. Repack is a background task that
	// can afford to be slower in exchange for bounded memory.
	args := []string{"-C", r.path,
		"-c", "pack.threads=" + strconv.Itoa(threads),
		"-c", "pack.windowMemory=256m",
		"-c", "pack.deltaCacheSize=128m",
		"repack"}
	args = append(args, repackArgs...)

	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(repackCtx, "git", args...)
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
