package git

import (
	"context"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/metadatadb"
)

const (
	snapshotGenMapName = "snapshot_generations"
	// '|' cannot appear in a Git upstream URL, so the split is unambiguous.
	snapshotGenKeySeparator = "|"
	// snapshotClaimTTL bounds how long an in-progress claim suppresses other
	// replicas. It exceeds lfsFetchTimeout so a live LFS generation is never
	// preempted. Peers that skipped re-arm for a full interval rather than
	// re-checking at expiry (which would herd them into the same sync
	// window), so a crashed generator delays regeneration by up to this TTL
	// plus one interval in the worst case.
	snapshotClaimTTL = 30 * time.Minute
)

// snapshotGenRecord is the shared per-artifact generation state. StartedAt is
// the most recent claim; CompletedAt is the most recent successful generation;
// CheckedAt is the most recent claim that ended in a skip (nothing uploaded);
// Commit is the mirror HEAD that generation captured.
type snapshotGenRecord struct {
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
	CheckedAt   time.Time `json:"checked_at,omitzero"`
	Commit      string    `json:"commit,omitempty"`
}

// SnapshotCoordinator shares per-artifact generation state across replicas so
// that each interval one replica regenerates a given snapshot instead of all
// of them. Coordination is advisory: the metadata store is last-write-wins
// and syncs asynchronously, so replicas that decide within the same sync
// window can still generate concurrently — jittered schedules make that rare,
// and a duplicate generation is wasteful but harmless.
//
// All methods are nil-safe; without a metadata store every replica generates.
type SnapshotCoordinator struct {
	ns   *metadatadb.Namespace
	gens *metadatadb.Map[string, snapshotGenRecord]
	now  func() time.Time
}

// NewSnapshotCoordinator returns nil if ns is nil so callers don't need a
// separate "no metadata configured" code path.
func NewSnapshotCoordinator(ns *metadatadb.Namespace) *SnapshotCoordinator {
	if ns == nil {
		return nil
	}
	return &SnapshotCoordinator{
		ns:   ns,
		gens: metadatadb.NewMap[string, snapshotGenRecord](ns, snapshotGenMapName),
		now:  time.Now,
	}
}

// Prime forces a synchronous refresh of shared state. Backends that sync
// asynchronously (S3) populate a fresh replica's local view lazily, so
// without a prime the first claim after startup could run against an empty
// view and regenerate an artifact a peer completed recently.
func (c *SnapshotCoordinator) Prime(ctx context.Context) error {
	if c == nil {
		return nil
	}
	return errors.Wrap(c.ns.Flush(ctx), "prime snapshot coordination state")
}

// Claim reports whether this replica should generate the artifact now, and
// records the claim when it should. It declines when another replica
// completed a generation or checked-and-skipped within the interval, or
// holds an unexpired in-progress claim.
func (c *SnapshotCoordinator) Claim(job, upstreamURL string, interval time.Duration) (bool, error) {
	if c == nil {
		return true, nil
	}
	key := snapshotGenKey(job, upstreamURL)
	now := c.now()
	rec, ok := c.gens.Get(key)
	if ok {
		// A completion within the interval means the artifact is still fresh.
		// This also absorbs schedule drift between replicas: a tick that lands
		// just before the generator's next one sees an almost-interval-old
		// completion and skips rather than duplicating the imminent generation.
		if !rec.CompletedAt.IsZero() && now.Sub(rec.CompletedAt) < interval {
			return false, nil
		}
		// A checked-and-skipped decision is as fresh as a completion for
		// claiming purposes: without it, once the last upload ages past the
		// interval every replica would re-fetch an unchanged repo each
		// interval instead of one.
		if !rec.CheckedAt.IsZero() && now.Sub(rec.CheckedAt) < interval {
			return false, nil
		}
		inProgress := rec.CompletedAt.Before(rec.StartedAt)
		if inProgress && now.Sub(rec.StartedAt) < snapshotClaimTTL {
			return false, nil
		}
	}
	rec.StartedAt = now
	if err := c.gens.Set(key, rec); err != nil {
		return true, errors.Wrap(err, "record snapshot claim")
	}
	return true, nil
}

// Complete records a successful generation and the commit it captured so
// other replicas skip the artifact until it goes stale again.
func (c *SnapshotCoordinator) Complete(job, upstreamURL, commit string) error {
	if c == nil {
		return nil
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, _ := c.gens.Get(key)
	rec.CompletedAt = c.now()
	rec.Commit = commit
	return errors.Wrap(c.gens.Set(key, rec), "record snapshot completion")
}

// Skip records a claim that ended without an upload (unchanged artifact or
// nothing to snapshot). It clears the in-progress claim and stamps CheckedAt
// so peers stay suppressed for the freshness interval rather than the claim
// TTL, without counting as a completion: CompletedAt must keep tracking the
// last actual upload so Unchanged's maxAge bound holds.
func (c *SnapshotCoordinator) Skip(job, upstreamURL string) error {
	if c == nil {
		return nil
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, _ := c.gens.Get(key)
	rec.CheckedAt = c.now()
	if rec.CompletedAt.Before(rec.StartedAt) {
		rec.StartedAt = rec.CompletedAt
	}
	return errors.Wrap(c.gens.Set(key, rec), "record snapshot skip")
}

// Unchanged reports whether the last completed generation captured the same
// commit and is recent enough (within maxAge) that its cache entry cannot
// have expired. Callers skipping on this must not call Complete, so
// CompletedAt keeps tracking the last actual upload and maxAge bounds both
// cache-entry age and drift of non-HEAD refs captured in the artifact.
func (c *SnapshotCoordinator) Unchanged(job, upstreamURL, commit string, maxAge time.Duration) bool {
	if c == nil || commit == "" || maxAge <= 0 {
		return false
	}
	rec, ok := c.gens.Get(snapshotGenKey(job, upstreamURL))
	return ok && rec.Commit == commit && !rec.CompletedAt.IsZero() && c.now().Sub(rec.CompletedAt) < maxAge
}

func snapshotGenKey(job, upstreamURL string) string {
	return job + snapshotGenKeySeparator + upstreamURL
}
