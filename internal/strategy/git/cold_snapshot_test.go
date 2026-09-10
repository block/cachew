package git_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/strategy/git"
)

const coldTestUpstream = "https://example.test/example/repo"

type controlledGit struct {
	countFile   string
	releaseFile string
}

type authoritativeStatErrorCache struct {
	cache.Cache
}

func (c authoritativeStatErrorCache) AuthoritativeStat(context.Context, cache.Key, ...cache.Option) (http.Header, error) {
	return nil, errors.New("authoritative stat unavailable")
}

type coldPublicationGate struct {
	cache.Cache
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *coldPublicationGate) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, options ...cache.Option) (cache.Writer, error) {
	if key == cache.NewKey(coldTestUpstream+".snapshot") {
		c.once.Do(func() { close(c.started) })
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.Cache.Create(ctx, key, headers, ttl, options...)
}

func installControlledGit(t *testing.T, failFirst bool) controlledGit {
	t.Helper()
	realGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	root := t.TempDir()
	upstream := filepath.Join(root, "upstream.git")
	createTestMirrorRepo(t, upstream)
	countFile := filepath.Join(root, "clone-count")
	releaseFile := filepath.Join(root, "release")
	binDir := filepath.Join(root, "bin")
	assert.NoError(t, os.Mkdir(binDir, 0o750))
	script := `#!/bin/sh
set -eu
saw_clone=0
saw_mirror=0
last=""
for arg in "$@"; do
  if [ "$arg" = "clone" ]; then saw_clone=1; fi
  if [ "$arg" = "--mirror" ]; then saw_mirror=1; fi
  last="$arg"
done
if [ "$saw_clone" = "1" ] && [ "$saw_mirror" = "1" ]; then
  printf 'clone\n' >> "$CLONE_COUNT_FILE"
  if [ "${FAIL_FIRST_CLONE:-0}" = "1" ] && [ "$(wc -l < "$CLONE_COUNT_FILE")" -eq 1 ]; then
    exit 42
  fi
  while [ ! -e "$CLONE_RELEASE_FILE" ]; do sleep 0.01; done
  exec "$REAL_GIT" clone --mirror "$FAKE_UPSTREAM" "$last"
fi
exec "$REAL_GIT" "$@"
`
	assert.NoError(t, os.WriteFile(filepath.Join(binDir, "git"), []byte(script), 0o750))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("REAL_GIT", realGit)
	t.Setenv("FAKE_UPSTREAM", upstream)
	t.Setenv("CLONE_COUNT_FILE", countFile)
	t.Setenv("CLONE_RELEASE_FILE", releaseFile)
	if failFirst {
		t.Setenv("FAIL_FIRST_CLONE", "1")
	}
	resolvedGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	assert.Equal(t, filepath.Join(binDir, "git"), resolvedGit)
	return controlledGit{countFile: countFile, releaseFile: releaseFile}
}

func (c controlledGit) release(t *testing.T) {
	t.Helper()
	assert.NoError(t, os.WriteFile(c.releaseFile, nil, 0o600))
}

func (c controlledGit) count() int {
	body, err := os.ReadFile(c.countFile)
	if err != nil {
		return 0
	}
	return strings.Count(string(body), "clone\n")
}

func waitForColdCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func newColdSnapshotStrategy(
	ctx context.Context,
	t *testing.T,
	scheduler jobscheduler.Provider,
	c cache.Cache,
	mirrorRoot string,
) (*git.Strategy, *testMux, *gitclone.Manager) {
	return newColdSnapshotStrategyWithConfig(ctx, t, scheduler, c, mirrorRoot, git.Config{})
}

func newColdSnapshotStrategyWithConfig(
	ctx context.Context,
	t *testing.T,
	scheduler jobscheduler.Provider,
	c cache.Cache,
	mirrorRoot string,
	config git.Config,
) (*git.Strategy, *testMux, *gitclone.Manager) {
	t.Helper()
	mux := newTestMux()
	managerProvider := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
	strategy, err := git.New(ctx, config, scheduler, c, mux, managerProvider,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	if config.SnapshotInterval > 0 {
		strategy.SetMetadataStore(nil)
	}
	waitForReady(t, strategy)
	manager, err := managerProvider()
	assert.NoError(t, err)
	return strategy, mux, manager
}

func coldSnapshotRequest(ctx context.Context, mux *testMux) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/git/example.test/example/repo/snapshot.tar.zst", nil).WithContext(ctx)
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "example/repo/snapshot.tar.zst")
	w := httptest.NewRecorder()
	mux.handlers["GET /git/{host}/{path...}"].ServeHTTP(w, req)
	return w
}

func TestColdSnapshotMissIsFastCoalescedAndSurvivesDisconnect(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategy(ctx, t, newTestScheduler(ctx, t), memCache,
		filepath.Join(t.TempDir(), "mirrors"))

	requestCtx, cancelRequest := context.WithCancel(ctx)
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() { response <- coldSnapshotRequest(requestCtx, mux) }()
	select {
	case w := <-response:
		assert.Equal(t, http.StatusNotFound, w.Code)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	case <-time.After(time.Second):
		controlled.release(t)
		t.Fatal("cold snapshot miss blocked on mirror clone")
	}
	cancelRequest()
	waitForColdCondition(t, func() bool { return controlled.count() >= 1 })
	assert.Equal(t, 1, controlled.count())

	const requests = 16
	responses := make(chan int, requests)
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() { responses <- coldSnapshotRequest(ctx, mux).Code })
	}
	wg.Wait()
	close(responses)
	for status := range responses {
		assert.Equal(t, http.StatusNotFound, status)
	}
	assert.Equal(t, 1, controlled.count())

	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	controlled.release(t)
	waitForColdCondition(t, func() bool { return repo.State() == gitclone.StateReady })
	assert.Equal(t, 1, controlled.count())
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
	_, err = cache.StatAuthoritative(ctx, memCache, cache.NewKey(coldTestUpstream+".snapshot"))
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestColdSnapshotPreparationFailureCanRetry(t *testing.T) {
	controlled := installControlledGit(t, true)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategy(ctx, t, newTestScheduler(ctx, t), memCache,
		filepath.Join(t.TempDir(), "mirrors"))

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	waitForColdCondition(t, func() bool {
		return controlled.count() >= 1 && !strategy.MirrorPreparationScheduled(coldTestUpstream)
	})
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	assert.Equal(t, gitclone.StateEmpty, repo.State())

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	waitForColdCondition(t, func() bool { return controlled.count() >= 2 })
	controlled.release(t)
	waitForColdCondition(t, func() bool { return repo.State() == gitclone.StateReady })
	assert.Equal(t, 2, controlled.count())
}

func TestColdSnapshotPreparationRetriesAfterCompetingCloneFails(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategy(ctx, t, newTestScheduler(ctx, t), memCache,
		filepath.Join(t.TempDir(), "mirrors"))
	repo, err := manager.GetOrCreate(ctx, coldTestUpstream)
	assert.NoError(t, err)
	assert.True(t, repo.TryStartCloning())

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	waitForColdCondition(t, func() bool { return strategy.MirrorPreparationScheduled(coldTestUpstream) })
	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 0, controlled.count())
	repo.ResetToEmpty()
	waitForColdCondition(t, func() bool { return controlled.count() >= 1 })
	controlled.release(t)
	waitForColdCondition(t, func() bool { return repo.State() == gitclone.StateReady })
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
}

func TestCoordinatedSnapshotFailureReleasesClaim(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, _, manager := newColdSnapshotStrategy(ctx, t, newTestScheduler(ctx, t), memCache,
		filepath.Join(t.TempDir(), "mirrors"))
	store := metadatadb.New(ctx, metadatadb.NewMemoryBackend())
	prime := metadatadb.NewMap[string, string](store.Namespace("git"), "prime")
	assert.NoError(t, prime.Set("key", "value"))
	strategy.SetMetadataStore(store)
	repo, err := manager.GetOrCreate(ctx, coldTestUpstream)
	assert.NoError(t, err)
	assert.Error(t, strategy.RunFailingCoordinatedSnapshot(ctx, repo))

	claimed, err := strategy.ClaimSnapshotForTest(coldTestUpstream)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestColdSnapshotPreparationContinuesAfterSharedStatError(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategy(ctx, t, newTestScheduler(ctx, t),
		authoritativeStatErrorCache{Cache: memCache}, filepath.Join(t.TempDir(), "mirrors"))

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	waitForColdCondition(t, func() bool { return controlled.count() >= 1 })
	controlled.release(t)
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	waitForColdCondition(t, func() bool { return repo.State() == gitclone.StateReady })
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
}

func TestColdSnapshotPreparationReusesSharedPublication(t *testing.T) {
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	scheduler, err := jobscheduler.New(schedulerCtx, jobscheduler.Config{Concurrency: 1})
	assert.NoError(t, err)
	t.Cleanup(func() {
		cancelScheduler()
		scheduler.Wait()
		assert.NoError(t, scheduler.Close())
	})

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	scheduler.Submit("blocker", "blocker", func(context.Context) error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	provider := func() (*jobscheduler.RootScheduler, error) { return scheduler, nil }
	strategy, mux, manager := newColdSnapshotStrategyWithConfig(ctx, t, provider, memCache,
		mirrorRoot, git.Config{SnapshotInterval: time.Hour})
	strategy.SetColdPreparationDelay(func() time.Duration { return 0 })
	strategy.SetMetadataStore(metadatadb.New(ctx, metadatadb.NewMemoryBackend()))
	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	assert.True(t, strategy.MirrorPreparationScheduled(coldTestUpstream))

	err = cache.WriteFunc(ctx, memCache, cache.NewKey(coldTestUpstream+".snapshot"), nil, time.Hour,
		func(w io.Writer) error {
			_, err := w.Write([]byte("shared snapshot"))
			return err
		})
	assert.NoError(t, err)
	close(releaseBlocker)
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	assert.Equal(t, gitclone.StateEmpty, repo.State())
}

func TestColdSnapshotPreparationRejectsWhenSchedulerQueueIsFull(t *testing.T) {
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	scheduler, err := jobscheduler.New(schedulerCtx, jobscheduler.Config{Concurrency: 1})
	assert.NoError(t, err)
	t.Cleanup(func() {
		cancelScheduler()
		scheduler.Wait()
		assert.NoError(t, scheduler.Close())
	})

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	scheduler.Submit("blocker", "blocker", func(context.Context) error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted
	t.Cleanup(func() { close(releaseBlocker) })
	for i := range git.ColdPreparationQueueLimitForTest() {
		scheduler.Submit(fmt.Sprintf("queued-%d", i), "queued", func(context.Context) error { return nil })
	}

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	provider := func() (*jobscheduler.RootScheduler, error) { return scheduler, nil }
	strategy, mux, manager := newColdSnapshotStrategy(ctx, t, provider, memCache, mirrorRoot)
	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	assert.False(t, strategy.MirrorPreparationScheduled(coldTestUpstream))
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	assert.Equal(t, gitclone.StateEmpty, repo.State())
}

func TestColdSnapshotPreparationRechecksPublicationAfterCoordinationDelay(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategyWithConfig(ctx, t, newTestScheduler(ctx, t), memCache,
		filepath.Join(t.TempDir(), "mirrors"), git.Config{SnapshotInterval: time.Hour})
	strategy.SetMetadataStore(metadatadb.New(ctx, metadatadb.NewMemoryBackend()))
	delayStarted := make(chan struct{})
	strategy.SetColdPreparationDelay(func() time.Duration {
		close(delayStarted)
		return 200 * time.Millisecond
	})

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	<-delayStarted
	err = cache.WriteFunc(ctx, memCache, cache.NewKey(coldTestUpstream+".snapshot"), nil, time.Hour,
		func(w io.Writer) error {
			_, err := w.Write([]byte("shared snapshot"))
			return err
		})
	assert.NoError(t, err)

	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
	assert.Equal(t, 0, controlled.count())
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	assert.Equal(t, gitclone.StateEmpty, repo.State())
}

func TestColdSnapshotPreparationPublishesFromReadyMirror(t *testing.T) {
	mirrorRoot := filepath.Join(t.TempDir(), "mirrors")
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	scheduler, err := jobscheduler.New(schedulerCtx, jobscheduler.Config{Concurrency: 1})
	assert.NoError(t, err)
	t.Cleanup(func() {
		cancelScheduler()
		scheduler.Wait()
		assert.NoError(t, scheduler.Close())
	})

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	scheduler.Submit("blocker", "blocker", func(context.Context) error {
		close(blockerStarted)
		<-releaseBlocker
		return nil
	})
	<-blockerStarted

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	gate := &coldPublicationGate{Cache: memCache, started: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(gate.release) })
	t.Cleanup(unblock)
	provider := func() (*jobscheduler.RootScheduler, error) { return scheduler, nil }
	strategy, mux, manager := newColdSnapshotStrategyWithConfig(ctx, t, provider, gate,
		mirrorRoot, git.Config{SnapshotInterval: time.Hour, RepackInterval: time.Hour})
	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	createTestMirrorRepo(t, repo.Path())
	repo.MarkReady()
	close(releaseBlocker)

	select {
	case <-gate.started:
	case <-time.After(30 * time.Second):
		t.Fatal("cold snapshot did not reach publication")
	}
	assert.True(t, strategy.MirrorPreparationScheduled(coldTestUpstream))
	response := coldSnapshotRequest(ctx, mux)
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	unblock()
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
	snapshotScheduled, repackScheduled := strategy.PeriodicJobsScheduled(coldTestUpstream)
	assert.True(t, snapshotScheduled)
	assert.True(t, repackScheduled)
	_, err = cache.StatAuthoritative(ctx, memCache, cache.NewKey(coldTestUpstream+".snapshot"))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, coldSnapshotRequest(ctx, mux).Code)
}

func TestColdSnapshotPreparationCoordinatesAcrossReplicas(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	sharedCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	store := metadatadb.New(ctx, metadatadb.NewMemoryBackend())
	prime := metadatadb.NewMap[string, string](store.Namespace("git"), "prime")
	assert.NoError(t, prime.Set("key", "value"))

	newReplica := func(mirrorRoot string) (*git.Strategy, *testMux, *gitclone.Manager) {
		mux := newTestMux()
		managerProvider := gitclone.NewManagerProvider(ctx, gitclone.Config{MirrorRoot: mirrorRoot}, nil)
		strategy, err := git.New(ctx, git.Config{SnapshotInterval: time.Hour}, newTestScheduler(ctx, t), sharedCache, mux,
			managerProvider, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
		assert.NoError(t, err)
		strategy.SetColdPreparationDelay(func() time.Duration { return 0 })
		strategy.SetMetadataStore(store)
		waitForColdCondition(t, strategy.Ready)
		manager, err := managerProvider()
		assert.NoError(t, err)
		return strategy, mux, manager
	}

	strategyA, muxA, managerA := newReplica(filepath.Join(t.TempDir(), "mirror-a"))
	strategyB, muxB, managerB := newReplica(filepath.Join(t.TempDir(), "mirror-b"))
	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, muxA).Code)
	waitForColdCondition(t, func() bool { return controlled.count() >= 1 })
	assert.Equal(t, 1, controlled.count())

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, muxB).Code)
	waitForColdCondition(t, func() bool { return !strategyB.MirrorPreparationScheduled(coldTestUpstream) })
	assert.Equal(t, 1, controlled.count())
	repoB := managerB.Get(coldTestUpstream)
	assert.True(t, repoB != nil)
	assert.Equal(t, gitclone.StateEmpty, repoB.State())

	controlled.release(t)
	repoA := managerA.Get(coldTestUpstream)
	assert.True(t, repoA != nil)
	waitForColdCondition(t, func() bool { return repoA.State() == gitclone.StateReady })
	waitForColdCondition(t, func() bool { return !strategyA.MirrorPreparationScheduled(coldTestUpstream) })
	assert.Equal(t, 1, controlled.count())
	assert.Equal(t, http.StatusOK, coldSnapshotRequest(ctx, muxB).Code)
	assert.Equal(t, 1, controlled.count())

	assert.NoError(t, sharedCache.Delete(ctx, cache.NewKey(coldTestUpstream+".snapshot")))
	strategyC, muxC, managerC := newReplica(filepath.Join(t.TempDir(), "mirror-c"))
	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, muxC).Code)
	waitForColdCondition(t, func() bool { return controlled.count() >= 2 })
	repoC := managerC.Get(coldTestUpstream)
	assert.True(t, repoC != nil)
	waitForColdCondition(t, func() bool { return repoC.State() == gitclone.StateReady })
	waitForColdCondition(t, func() bool { return !strategyC.MirrorPreparationScheduled(coldTestUpstream) })
}

func TestColdSnapshotPreparationStopsOnSchedulerShutdown(t *testing.T) {
	controlled := installControlledGit(t, false)
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	scheduler, err := jobscheduler.New(schedulerCtx, jobscheduler.Config{Concurrency: 1})
	assert.NoError(t, err)
	provider := func() (*jobscheduler.RootScheduler, error) { return scheduler, nil }
	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	strategy, mux, manager := newColdSnapshotStrategyWithConfig(ctx, t, provider, memCache,
		filepath.Join(t.TempDir(), "mirrors"), git.Config{SnapshotInterval: time.Hour})
	strategy.SetColdPreparationDelay(func() time.Duration { return 0 })
	strategy.SetMetadataStore(metadatadb.New(ctx, metadatadb.NewMemoryBackend()))

	assert.Equal(t, http.StatusNotFound, coldSnapshotRequest(ctx, mux).Code)
	waitForColdCondition(t, func() bool { return controlled.count() >= 1 })
	cancelScheduler()
	scheduler.Wait()
	assert.NoError(t, scheduler.Close())
	waitForColdCondition(t, func() bool { return !strategy.MirrorPreparationScheduled(coldTestUpstream) })
	repo := manager.Get(coldTestUpstream)
	assert.True(t, repo != nil)
	assert.Equal(t, gitclone.StateEmpty, repo.State())
	claimed, err := strategy.ClaimSnapshotForTest(coldTestUpstream)
	assert.NoError(t, err)
	assert.True(t, claimed)
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(repo.Path()), ".clone-*"))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(leftovers))
}
