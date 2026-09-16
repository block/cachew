package git //nolint:testpackage // white-box testing required for clock injection

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

func newTestSnapshotCoordinators(t *testing.T, now func() time.Time, replicas int) []*SnapshotCoordinator {
	t.Helper()
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	backend := metadatadb.NewMemoryBackend()
	coords := make([]*SnapshotCoordinator, replicas)
	for i := range coords {
		store := metadatadb.New(ctx, backend)
		coords[i] = NewSnapshotCoordinator(store.Namespace("git"))
		coords[i].now = now
	}
	return coords
}

type coldSnapshotProbeCache struct {
	cache.Cache
	probeErr error
	probes   int
}

func (c *coldSnapshotProbeCache) AuthoritativeStat(ctx context.Context, key cache.Key, options ...cache.Option) (http.Header, error) {
	c.probes++
	if c.probes <= 2 {
		return nil, c.probeErr
	}
	return c.Cache.Stat(ctx, key, options...)
}

type snapshotScheduleRecorder struct {
	jobscheduler.Scheduler
	jobs chan string
}

func (s *snapshotScheduleRecorder) SubmitPeriodicJob(_, id string, _ time.Duration, _ func(context.Context) error) {
	s.jobs <- id
}

type coldSnapshotSlowCache struct {
	cache.Cache
	beforeCreate func() error
}

func (c *coldSnapshotSlowCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, options ...cache.Option) (cache.Writer, error) {
	if err := c.beforeCreate(); err != nil {
		return nil, err
	}
	return c.Cache.Create(ctx, key, headers, ttl, options...)
}

func TestColdSnapshotDefersInitialBaseJob(t *testing.T) {
	for _, publishErr := range []error{nil, errors.New("upload failed")} {
		t.Run(fmt.Sprint(publishErr), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := logging.ContextWithLogger(t.Context(), slog.Default())
				manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
				assert.NoError(t, err)
				repo, err := manager.GetOrCreate(ctx, "https://example.test/org/repo")
				assert.NoError(t, err)
				for _, args := range [][]string{
					{"init", repo.Path()},
					{"-C", repo.Path(), "-c", "user.name=Test", "-c", "user.email=test@example.com",
						"commit", "--allow-empty", "--no-gpg-sign", "-m", "initial"},
				} {
					output, err := exec.CommandContext(ctx, "git", args...).CombinedOutput()
					assert.NoError(t, err, string(output))
				}
				repo.MarkReady()
				scheduler := &snapshotScheduleRecorder{jobs: make(chan string, 3)}
				mem, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
				assert.NoError(t, err)
				s := &Strategy{
					scheduler: scheduler, cloneManager: manager, metrics: newGitMetrics(),
					config: Config{SnapshotInterval: time.Hour, ZstdThreads: 1},
				}
				s.cache = &coldSnapshotSlowCache{Cache: mem, beforeCreate: func() error {
					s.scheduleSnapshotJobs(repo)
					time.Sleep(2 * time.Hour)
					synctest.Wait()
					assert.Equal(t, 0, len(scheduler.jobs))
					return publishErr
				}}
				s.mirrorPreparations.Store(repo.UpstreamURL(), true)
				err = s.prepareColdSnapshot(ctx, repo)
				if publishErr != nil {
					assert.True(t, errors.Is(err, publishErr))
				} else {
					assert.NoError(t, err)
					_, err = mem.Stat(ctx, snapshotCacheKey(repo.UpstreamURL()))
					assert.NoError(t, err)
				}
				s.mirrorPreparations.Delete(repo.UpstreamURL())
				synctest.Wait()
				assert.Equal(t, 2, len(scheduler.jobs))
				assert.Equal(t, snapshotJobLFS+"-periodic", <-scheduler.jobs)
				assert.Equal(t, snapshotJobMirror+"-periodic", <-scheduler.jobs)
				time.Sleep(time.Hour - time.Nanosecond)
				synctest.Wait()
				assert.Equal(t, 0, len(scheduler.jobs))
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assert.Equal(t, 1, len(scheduler.jobs))
				assert.Equal(t, snapshotJobBase+"-periodic", <-scheduler.jobs)
			})
		})
	}
}

func TestColdSnapshotSkipPreservesCompletion(t *testing.T) {
	for _, probeErr := range []error{os.ErrNotExist, errors.New("stat unavailable")} {
		t.Run(probeErr.Error(), func(t *testing.T) {
			ctx := logging.ContextWithLogger(t.Context(), slog.Default())
			const upstream = "https://example.test/org/repo"
			manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
			assert.NoError(t, err)
			repo, err := manager.GetOrCreate(ctx, upstream)
			assert.NoError(t, err)
			for _, args := range [][]string{
				{"init", repo.Path()},
				{"-C", repo.Path(), "-c", "user.name=Test", "-c", "user.email=test@example.com",
					"commit", "--allow-empty", "--no-gpg-sign", "-m", "initial"},
			} {
				output, err := exec.CommandContext(ctx, "git", args...).CombinedOutput()
				assert.NoError(t, err, string(output))
			}
			head, err := exec.CommandContext(ctx, "git", "-C", repo.Path(), "rev-parse", "HEAD").Output()
			assert.NoError(t, err)
			commit := strings.TrimSpace(string(head))
			repo.MarkReady()

			clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
			coord := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 1)[0]
			assert.NoError(t, coord.Complete(snapshotJobBase, upstream, commit))
			completedAt := clock
			clock = clock.Add(30 * time.Minute)
			mem, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
			assert.NoError(t, err)
			assert.NoError(t, cache.WriteFunc(ctx, mem, snapshotCacheKey(upstream), nil, time.Hour, func(w io.Writer) error {
				_, err := io.WriteString(w, "existing snapshot")
				return err
			}))
			c := &coldSnapshotProbeCache{Cache: mem, probeErr: probeErr}
			s := &Strategy{
				config:               Config{SnapshotInterval: time.Hour, SnapshotMaxAge: time.Hour},
				cache:                c,
				cloneManager:         manager,
				snapshotCoord:        coord,
				metrics:              newGitMetrics(),
				coldPreparationDelay: func() time.Duration { return 0 },
			}
			s.snapshotJobsScheduled.Store(upstream, true)
			assert.NoError(t, s.prepareColdSnapshot(ctx, repo))
			assert.Equal(t, 3, c.probes)
			rec, ok := coord.gens.Get(snapshotGenKey(snapshotJobBase, upstream))
			assert.True(t, ok)
			assert.Equal(t, completedAt, rec.CompletedAt)
			assert.Equal(t, clock, rec.CheckedAt)
			assert.Zero(t, rec.ClaimID)
			assert.True(t, coord.Unchanged(snapshotJobBase, upstream, commit, time.Hour))
			clock = completedAt.Add(time.Hour)
			assert.False(t, coord.Unchanged(snapshotJobBase, upstream, commit, time.Hour))
		})
	}
}

func TestSnapshotCoordinatorNilSafe(t *testing.T) {
	var c *SnapshotCoordinator
	claimed, err := c.Claim("snapshot", "https://github.com/foo/bar", time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, c.Complete("snapshot", "https://github.com/foo/bar", "abc123"))
	assert.NoError(t, c.Skip("snapshot", "https://github.com/foo/bar"))
	assert.NoError(t, c.SkipClaim(context.Background(), "snapshot", "https://github.com/foo/bar", "claim"))
	assert.NoError(t, c.Fail(context.Background(), "snapshot", "https://github.com/foo/bar", "claim"))
	assert.False(t, c.Unchanged("snapshot", "https://github.com/foo/bar", "abc123", time.Hour))
	assert.NoError(t, c.Prime(context.Background()))
	assert.Zero(t, NewSnapshotCoordinator(nil))
}

func TestSnapshotCoordinatorFailedClaimCanRetryImmediately(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/example/repo"

	claimID, claimed, err := coords[0].ClaimWithTTL(snapshotJobBase, upstream, snapshotClaimTTL, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NotZero(t, claimID)
	assert.NoError(t, coords[0].Fail(context.Background(), snapshotJobBase, upstream, claimID))

	claimed, err = coords[1].Claim(snapshotJobBase, upstream, snapshotClaimTTL)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorOwnedClaimSurvivesStaleRelease(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 3)
	const upstream = "https://github.com/example/repo"

	firstID, claimed, err := coords[0].ClaimWithTTL(snapshotJobBase, upstream, 0, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	clock = clock.Add(time.Hour)
	secondID, claimed, err := coords[1].ClaimWithTTL(snapshotJobBase, upstream, 0, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NotEqual(t, firstID, secondID)

	assert.NoError(t, coords[0].Fail(context.Background(), snapshotJobBase, upstream, firstID))
	assert.NoError(t, coords[0].CompleteClaim(context.Background(), snapshotJobBase, upstream, firstID, "stale"))
	assert.NoError(t, coords[0].SkipClaim(context.Background(), snapshotJobBase, upstream, firstID))
	_, claimed, err = coords[2].ClaimWithTTL(snapshotJobBase, upstream, 0, time.Hour)
	assert.NoError(t, err)
	assert.False(t, claimed)
	assert.NoError(t, coords[1].CompleteClaim(context.Background(), snapshotJobBase, upstream, secondID, "abc123"))
}

func TestSnapshotCoordinatorCustomClaimTTL(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/example/repo"

	_, claimed, err := coords[0].ClaimWithTTL(snapshotJobBase, upstream, 0, 2*time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	clock = clock.Add(snapshotClaimTTL)
	claimed, err = coords[1].Claim(snapshotJobBase, upstream, 0)
	assert.NoError(t, err)
	assert.False(t, claimed)
}

func TestSnapshotCoordinatorUnchanged(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"
	const maxAge = 24 * time.Hour

	// No completion recorded yet.
	assert.False(t, coords[0].Unchanged("snapshot", upstream, "abc123", maxAge))

	assert.NoError(t, coords[0].Complete("snapshot", upstream, "abc123"))

	// Peers share the record: same commit within maxAge skips, everything
	// else regenerates.
	clock = clock.Add(12 * time.Hour)
	assert.True(t, coords[1].Unchanged("snapshot", upstream, "abc123", maxAge))
	assert.False(t, coords[1].Unchanged("snapshot", upstream, "def456", maxAge))
	assert.False(t, coords[1].Unchanged("snapshot", upstream, "", maxAge))
	assert.False(t, coords[1].Unchanged("snapshot", upstream, "abc123", 0))
	assert.False(t, coords[1].Unchanged("lfs-snapshot", upstream, "abc123", maxAge))

	clock = clock.Add(12 * time.Hour)
	assert.False(t, coords[1].Unchanged("snapshot", upstream, "abc123", maxAge))
}

func TestSnapshotCoordinatorClaimWithoutCompleteIsNotUnchanged(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 1)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.False(t, coords[0].Unchanged("snapshot", upstream, "abc123", 24*time.Hour))
}

func TestSnapshotCoordinatorFreshArtifactSkips(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	clock = clock.Add(2 * time.Minute)
	assert.NoError(t, coords[0].Complete("snapshot", upstream, "abc123"))

	clock = clock.Add(10 * time.Minute)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.False(t, claimed)

	clock = clock.Add(time.Hour)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorFreshUntilFullInterval(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, coords[0].Complete("snapshot", upstream, "abc123"))

	// A completion just shy of the full interval still suppresses peers.
	clock = clock.Add(time.Hour - time.Minute)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.False(t, claimed)

	clock = clock.Add(time.Minute)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorShortIntervalStillSuppresses(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, 5*time.Minute)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, coords[0].Complete("snapshot", upstream, "abc123"))

	clock = clock.Add(2 * time.Minute)
	claimed, err = coords[1].Claim("snapshot", upstream, 5*time.Minute)
	assert.NoError(t, err)
	assert.False(t, claimed)
}

func TestSnapshotCoordinatorInProgressClaimSuppressesPeers(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("lfs-snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)

	clock = clock.Add(5 * time.Minute)
	claimed, err = coords[1].Claim("lfs-snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.False(t, claimed)

	// An expired claim (crashed generator) no longer suppresses peers.
	clock = clock.Add(snapshotClaimTTL)
	claimed, err = coords[1].Claim("lfs-snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorSkipSuppressesPeersForInterval(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, coords[0].Skip("snapshot", upstream))

	// A skip is not a completion, so it never validates an unchanged check.
	assert.False(t, coords[1].Unchanged("snapshot", upstream, "abc123", 24*time.Hour))

	// But it counts as freshness for claiming: unchanged repos get one
	// fetch-and-check per interval, not one per replica.
	clock = clock.Add(time.Minute)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.False(t, claimed)

	// Once the interval elapses a peer claims again, well before the claim
	// TTL would have allowed had the claim lingered.
	clock = clock.Add(time.Hour)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorFailedGenerationDoesNotMarkFresh(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	// Generation fails: Complete is never called. After the claim expires the
	// peer generates instead of waiting a full interval.
	clock = clock.Add(snapshotClaimTTL)
	claimed, err = coords[1].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestSnapshotCoordinatorKeysAreIndependent(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 1)
	c := coords[0]

	claimed, err := c.Claim("snapshot", "https://github.com/foo/bar", time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, c.Complete("snapshot", "https://github.com/foo/bar", "abc123"))

	for _, job := range []string{"lfs-snapshot", "mirror-snapshot"} {
		claimed, err = c.Claim(job, "https://github.com/foo/bar", time.Hour)
		assert.NoError(t, err)
		assert.True(t, claimed)
	}
	claimed, err = c.Claim("snapshot", "https://github.com/foo/other", time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
}

func TestJitterInterval(t *testing.T) {
	assert.Equal(t, time.Duration(0), jitterInterval(0))
	for range 100 {
		j := jitterInterval(time.Hour)
		assert.True(t, j >= time.Hour)
		assert.True(t, j < time.Hour+time.Hour/8)
	}
}

func TestStartupSpreadDelay(t *testing.T) {
	for range 100 {
		d := startupSpreadDelay()
		assert.True(t, d >= 0)
		assert.True(t, d < snapshotStartupSpread)
	}
}

func TestSnapshotSchedule(t *testing.T) {
	uncoordinated := &Strategy{}
	delay, interval := uncoordinated.snapshotSchedule(time.Hour)
	assert.Equal(t, time.Duration(0), delay)
	assert.Equal(t, time.Hour, interval)

	coordinated := &Strategy{snapshotCoord: newTestSnapshotCoordinators(t, time.Now, 1)[0]}
	for range 100 {
		delay, interval = coordinated.snapshotSchedule(time.Hour)
		assert.True(t, delay >= 0 && delay < snapshotStartupSpread)
		assert.True(t, interval >= time.Hour && interval < time.Hour+time.Hour/8)
	}
}
