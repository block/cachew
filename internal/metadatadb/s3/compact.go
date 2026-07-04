package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// ladder runs the compaction/probe bookkeeping after a successful background
// tick, per the algorithm in docs/metadatadb-s3.md. Leaderless: any replica
// may compact or probe; the conditional PUT of the rollup is both election
// and commit. Ages are measured entirely in S3's time frame (newest listed
// stamp minus segment stamp); the replica's clock is never consulted.
func (n *namespace) ladder(ctx context.Context, lst *listing) error {
	if lst.newest.After(n.lastNewest) {
		n.lastNewest = lst.newest
		n.stall = 0
	}

	var leftovers, candidates []string
	for key, lm := range lst.entries {
		if lst.newest.Sub(lm) < n.b.ageThreshold {
			continue
		}
		if n.rollup.mark.covers(lm, key) {
			// Folded by a compaction whose compactor died before finishing
			// its deletes.
			leftovers = append(leftovers, key)
		} else {
			candidates = append(candidates, key)
		}
	}
	if len(leftovers) > 0 {
		n.remove(ctx, leftovers)
	}

	switch {
	case len(candidates) >= n.b.segmentThreshold:
		n.stall = 0
		n.sustain++
		if n.sustain < sustainTicks {
			return nil
		}
		n.sustain = 0
		return n.compact(ctx, lst, candidates)
	case len(lst.entries) >= n.b.segmentThreshold:
		// Aging is stalled: enough live segments, too few aged. A clock
		// probe advances the reference so the tail can age.
		n.sustain = 0
		n.stall++
		if n.stall < sustainTicks {
			return nil
		}
		n.stall = 0
		return n.probe(ctx, lst)
	default:
		n.sustain, n.stall = 0, 0
		return nil
	}
}

// compact folds the candidates into a new rollup and commits it with a CAS
// on the rollup's ETag — the election. A 412/409 means another replica won,
// benignly. The winner deletes the folded segments; failures there are
// ignored since any replica's leftover cleanup finishes the job.
func (n *namespace) compact(ctx context.Context, lst *listing, candidates []string) error {
	select {
	case <-time.After(n.b.jitter()):
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
	prevETag := n.rollup.etag
	if err := n.refreshRollup(ctx); err != nil {
		return errors.Wrap(err, "re-check rollup")
	}
	if n.rollup.etag != prevETag {
		return nil // another compactor already won
	}

	slices.SortFunc(candidates, func(a, b string) int {
		if c := lst.entries[a].Compare(lst.entries[b]); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	})

	state := make(map[string]any)
	if err := json.Unmarshal(n.rollup.state, &state); err != nil {
		return errors.Wrap(err, "unmarshal rollup state")
	}
	newMark := n.rollup.mark
	n.stateMu.RLock()
	for _, key := range candidates {
		e, ok := n.cache[key]
		if !ok {
			// The tick fetched every listed segment above the mark, so this
			// cannot happen; bail rather than fold an incomplete prefix.
			n.stateMu.RUnlock()
			return errors.Errorf("candidate %s missing from cache", key)
		}
		for _, o := range e.ops {
			metadatadb.ApplyOp(state, o)
		}
		newMark = mark{LM: lst.entries[key], Key: key}
	}
	n.stateMu.RUnlock()

	stateData, err := json.Marshal(state)
	if err != nil {
		return errors.Wrap(err, "marshal state")
	}
	body := rollupBody{State: stateData, Mark: newMark}
	data, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "marshal rollup")
	}

	opts := minio.PutObjectOptions{ContentType: "application/json"}
	opts.SetMatchETag(n.rollup.etag)
	info, err := n.b.client.PutObject(ctx, n.b.bucket, n.rollupKey(),
		bytes.NewReader(data), int64(len(data)), opts)
	if isPreconditionFailed(err) || isConflict(err) {
		return nil // lost the election benignly
	}
	if err != nil {
		return errors.Wrap(err, "commit rollup")
	}
	n.rollup = &heldRollup{etag: info.ETag, mark: newMark, state: stateData}
	n.remove(ctx, candidates)
	return nil
}

// probe PUTs an empty segment to advance the aging reference clock. It is a
// raw PUT outside the group-commit writer, never applied locally or
// cache-inserted; the next tick discovers it like any other segment. The
// recheck LIST suppresses duplicates (rare, and harmless no-ops anyway).
func (n *namespace) probe(ctx context.Context, lst *listing) error {
	select {
	case <-time.After(n.b.jitter()):
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
	recheck, err := n.list(ctx)
	if err != nil {
		return errors.Wrap(err, "probe recheck")
	}
	if recheck.newest.After(lst.newest) {
		return nil // someone else probed or wrote
	}
	data, err := marshalSegment(nil)
	if err != nil {
		return errors.Wrap(err, "marshal probe")
	}
	key, err := newSegmentKey()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = n.b.client.PutObject(ctx, n.b.bucket, n.prefix()+key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"})
	return errors.Wrap(err, "put probe")
}

// remove best-effort deletes segments via the multi-object delete API.
// Failures are logged and ignored: leftover cleanup on any replica's future
// tick finishes the job.
func (n *namespace) remove(ctx context.Context, keys []string) {
	objects := make(chan minio.ObjectInfo, len(keys))
	for _, key := range keys {
		objects <- minio.ObjectInfo{Key: n.prefix() + key}
	}
	close(objects)
	logger := logging.FromContext(ctx)
	for rerr := range n.b.client.RemoveObjects(ctx, n.b.bucket, objects, minio.RemoveObjectsOptions{}) {
		logger.WarnContext(ctx, "metadata segment cleanup failed", "namespace", n.name,
			"key", rerr.ObjectName, "error", rerr.Err)
	}
}
