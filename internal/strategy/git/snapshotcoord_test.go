package git //nolint:testpackage // white-box testing required for clock injection

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

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

func TestSnapshotCoordinatorNilSafe(t *testing.T) {
	var c *SnapshotCoordinator
	claimed, err := c.Claim("snapshot", "https://github.com/foo/bar", time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	assert.NoError(t, c.Complete("snapshot", "https://github.com/foo/bar"))
	assert.NoError(t, c.Prime(context.Background()))
	assert.Zero(t, NewSnapshotCoordinator(nil))
}

func TestSnapshotCoordinatorFreshArtifactSkips(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	coords := newTestSnapshotCoordinators(t, func() time.Time { return clock }, 2)
	const upstream = "https://github.com/foo/bar"

	claimed, err := coords[0].Claim("snapshot", upstream, time.Hour)
	assert.NoError(t, err)
	assert.True(t, claimed)
	clock = clock.Add(2 * time.Minute)
	assert.NoError(t, coords[0].Complete("snapshot", upstream))

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
	assert.NoError(t, coords[0].Complete("snapshot", upstream))

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
	assert.NoError(t, coords[0].Complete("snapshot", upstream))

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
	assert.NoError(t, c.Complete("snapshot", "https://github.com/foo/bar"))

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
