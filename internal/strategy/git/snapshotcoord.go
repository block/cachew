package git

import (
	"context"
	"time"

	"github.com/alecthomas/errors"
	"github.com/google/uuid"

	"github.com/block/cachew/internal/metadatadb"
)

const (
	snapshotGenMapName = "snapshot_generations"
	// '|' cannot appear in a Git upstream URL, so the split is unambiguous.
	snapshotGenKeySeparator = "|"
	// snapshotClaimTTL bounds how long an in-progress claim suppresses other replicas.
	//
	// It exceeds lfsFetchTimeout so a live LFS generation is never preempted. Skipped peers re-arm for a full interval
	// (not at expiry, which would herd them), so a crashed generator delays regeneration by up to TTL plus one interval.
	snapshotClaimTTL = 30 * time.Minute
)

// snapshotGenRecord stores the generation state for each artifact.
//
// ClaimID identifies the owner for release and completion checks. ClaimExpiresAt gives all replicas the same expiry.
type snapshotGenRecord struct {
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at,omitzero"`
	CheckedAt      time.Time `json:"checked_at,omitzero"`
	ClaimExpiresAt time.Time `json:"claim_expires_at,omitzero"`
	Commit         string    `json:"commit,omitempty"`
	ClaimID        string    `json:"claim_id,omitempty"`
}

// SnapshotCoordinator shares generation state so one replica per interval regenerates each snapshot, not all.
//
// It is advisory: the store is last-write-wins and syncs asynchronously, so replicas may still duplicate work, which
// jitter makes rare and is harmless. All methods are nil-safe; without a metadata store every replica generates.
type SnapshotCoordinator struct {
	ns   *metadatadb.Namespace
	gens *metadatadb.Map[string, snapshotGenRecord]
	now  func() time.Time
}

// NewSnapshotCoordinator returns nil if ns is nil so callers need no separate "no metadata configured" path.
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

// Prime forces a synchronous refresh of shared state.
//
// Asynchronous backends (S3) fill a fresh replica's view lazily, so without a prime the first claim after startup
// could see an empty view and regenerate an artifact a peer completed recently.
func (c *SnapshotCoordinator) Prime(ctx context.Context) error {
	if c == nil {
		return nil
	}
	return errors.Wrap(c.ns.Flush(ctx), "prime snapshot coordination state")
}

// Claim reports whether this replica should generate the artifact now, and records the claim if so.
//
// It declines when another replica completed or checked-and-skipped within the interval, or holds an unexpired claim.
func (c *SnapshotCoordinator) Claim(job, upstreamURL string, interval time.Duration) (bool, error) {
	_, claimed, err := c.ClaimWithTTL(job, upstreamURL, interval, snapshotClaimTTL)
	return claimed, err
}

// ClaimWithTTL lets the caller set the claim duration.
//
// The caller must use the returned claim ID to complete or release the claim.
func (c *SnapshotCoordinator) ClaimWithTTL(job, upstreamURL string, interval, claimTTL time.Duration) (string, bool, error) {
	if c == nil {
		return "", true, nil
	}
	if claimTTL <= 0 {
		claimTTL = snapshotClaimTTL
	}
	key := snapshotGenKey(job, upstreamURL)
	now := c.now()
	rec, ok := c.gens.Get(key)
	if ok {
		// A completion within the interval means the artifact is still fresh.
		//
		// This also absorbs schedule drift: a tick just before the generator's next one skips instead of duplicating it.
		if !rec.CompletedAt.IsZero() && now.Sub(rec.CompletedAt) < interval {
			return "", false, nil
		}
		// A checked-and-skipped decision is as fresh as a completion for claiming purposes.
		//
		// Without it, once the last upload ages past the interval every replica would re-fetch an unchanged repo.
		if !rec.CheckedAt.IsZero() && now.Sub(rec.CheckedAt) < interval {
			return "", false, nil
		}
		inProgress := rec.CompletedAt.Before(rec.StartedAt)
		claimExpiresAt := rec.ClaimExpiresAt
		if claimExpiresAt.IsZero() {
			claimExpiresAt = rec.StartedAt.Add(snapshotClaimTTL)
		}
		if inProgress && now.Before(claimExpiresAt) {
			return "", false, nil
		}
	}
	rec.StartedAt = now
	rec.ClaimID = uuid.NewString()
	rec.ClaimExpiresAt = now.Add(claimTTL)
	if err := c.gens.Set(key, rec); err != nil {
		return rec.ClaimID, true, errors.Wrap(err, "record snapshot claim")
	}
	return rec.ClaimID, true, nil
}

// Complete records a successful generation and its commit so other replicas skip the artifact until it goes stale.
func (c *SnapshotCoordinator) Complete(job, upstreamURL, commit string) error {
	if c == nil {
		return nil
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, _ := c.gens.Get(key)
	rec.CompletedAt = c.now()
	rec.Commit = commit
	rec.ClaimID = ""
	rec.ClaimExpiresAt = time.Time{}
	return errors.Wrap(c.gens.Set(key, rec), "record snapshot completion")
}

// CompleteClaim reads the shared state again. It completes the claim only if the claim ID still matches.
func (c *SnapshotCoordinator) CompleteClaim(ctx context.Context, job, upstreamURL, claimID, commit string) error {
	if c == nil || claimID == "" {
		return nil
	}
	if err := c.Prime(ctx); err != nil {
		return errors.Wrap(err, "refresh before completing snapshot claim")
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, ok := c.gens.Get(key)
	if !ok || rec.ClaimID != claimID {
		return nil
	}
	rec.CompletedAt = c.now()
	rec.Commit = commit
	rec.ClaimID = ""
	rec.ClaimExpiresAt = time.Time{}
	return errors.Wrap(c.gens.Set(key, rec), "record owned snapshot completion")
}

// Skip records a claim that ended without an upload, clearing the claim and stamping CheckedAt.
//
// Peers stay suppressed for the freshness interval rather than the claim TTL. It is not a completion: CompletedAt
// must keep tracking the last actual upload so Unchanged's maxAge bound holds.
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
	rec.ClaimID = ""
	rec.ClaimExpiresAt = time.Time{}
	return errors.Wrap(c.gens.Set(key, rec), "record snapshot skip")
}

// SkipClaim reads the shared state again. It records a skip only if the claim ID still matches.
func (c *SnapshotCoordinator) SkipClaim(ctx context.Context, job, upstreamURL, claimID string) error {
	if c == nil || claimID == "" {
		return nil
	}
	if err := c.Prime(ctx); err != nil {
		return errors.Wrap(err, "refresh before skipping snapshot claim")
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, ok := c.gens.Get(key)
	if !ok || rec.ClaimID != claimID {
		return nil
	}
	rec.CheckedAt = c.now()
	if rec.CompletedAt.Before(rec.StartedAt) {
		rec.StartedAt = rec.CompletedAt
	}
	rec.ClaimID = ""
	rec.ClaimExpiresAt = time.Time{}
	return errors.Wrap(c.gens.Set(key, rec), "record owned snapshot skip")
}

// Fail releases a failed claim only if the claim ID still matches the shared state.
//
// It does not record a successful check, so another caller can try again immediately.
func (c *SnapshotCoordinator) Fail(ctx context.Context, job, upstreamURL, claimID string) error {
	if c == nil || claimID == "" {
		return nil
	}
	if err := c.Prime(ctx); err != nil {
		return errors.Wrap(err, "refresh before releasing snapshot claim")
	}
	key := snapshotGenKey(job, upstreamURL)
	rec, ok := c.gens.Get(key)
	if !ok || rec.ClaimID != claimID || !rec.CompletedAt.Before(rec.StartedAt) {
		return nil
	}
	rec.StartedAt = rec.CompletedAt
	rec.ClaimID = ""
	rec.ClaimExpiresAt = time.Time{}
	return errors.Wrap(c.gens.Set(key, rec), "release failed snapshot claim")
}

// Unchanged reports whether the last completed generation captured commit within maxAge, so its entry is unexpired.
//
// Callers skipping on this must not call Complete, so CompletedAt keeps tracking the last upload and maxAge bounds
// both cache-entry age and drift of non-HEAD refs captured in the artifact.
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
