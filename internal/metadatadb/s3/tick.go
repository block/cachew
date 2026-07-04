package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// listing is one tick's full-prefix LIST: segment name -> LastModified, plus
// the newest stamp (the aging reference clock) and the local monotonic start
// time (compared against cache insert times for unlisted-absence eviction).
type listing struct {
	entries map[string]time.Time
	newest  time.Time
	start   time.Time
}

func (n *namespace) flush(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case n.flushCh <- reply:
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-n.b.ctx.Done():
		return errors.New("backend closed")
	}
	select {
	case err := <-reply:
		return errors.WithStack(err)
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
}

// runLoop serializes all ticks for the namespace: background ticks run the
// compaction/probe ladder, Flush ticks do not (so a Flush never sleeps a
// jitter delay). Channel ordering provides the fairness the design requires.
// maxConsecutiveFlushes bounds the Flush priority so sustained Flush traffic
// cannot starve the background ladder forever: every N-th iteration the main
// select gives the ticker a fair chance.
const maxConsecutiveFlushes = 16

func (n *namespace) runLoop() {
	defer n.b.wg.Done()
	logger := logging.FromContext(n.b.ctx)
	background := func() {
		lst, err := n.tick(n.b.ctx)
		if err != nil {
			logger.WarnContext(n.b.ctx, "metadata sync tick failed", "namespace", n.name, "error", err)
			return
		}
		if err := n.ladder(n.b.ctx, lst); err != nil {
			// Ladder failures cost staleness only: conditions re-derive
			// from stamps next tick.
			logger.WarnContext(n.b.ctx, "metadata compaction failed", "namespace", n.name, "error", err)
		}
	}

	// An immediate first tick so a restarted replica converges without
	// waiting a full interval.
	if n.b.initialTick {
		background()
	}

	ticker := time.NewTicker(n.b.syncInterval)
	defer ticker.Stop()
	flushes := 0
	for {
		// A waiting Flush takes priority over the ticker so it cannot be
		// starved when tick duration approaches the interval.
		if flushes < maxConsecutiveFlushes {
			select {
			case reply := <-n.flushCh:
				_, err := n.tick(n.b.ctx)
				reply <- err
				flushes++
				continue
			default:
			}
		} else {
			flushes = 0
		}
		select {
		case <-ticker.C:
			flushes = 0
			background()
		case reply := <-n.flushCh:
			_, err := n.tick(n.b.ctx)
			reply <- err
			flushes++
		case <-n.b.ctx.Done():
			return
		}
	}
}

// tick is one sync pass: LIST, refresh (or seed) the rollup, fetch unseen
// segments into staging, evict, rebuild, swap. It aborts at the first
// failure, discarding staged fetches; the next tick self-heals.
func (n *namespace) tick(ctx context.Context) (*listing, error) {
	lst, err := n.list(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list segments")
	}
	// LIST deliberately precedes the rollup GET: the rollup is then always
	// at least as new as the listing, so a rebuild can never have a hole
	// between rollup coverage and tail.
	if err := n.refreshRollup(ctx); err != nil {
		return nil, errors.Wrap(err, "refresh rollup")
	}
	staged, err := n.fetchUnseen(ctx, lst)
	if err != nil {
		return nil, errors.Wrap(err, "fetch segments")
	}
	n.rebuild(lst, staged)
	return lst, nil
}

func (n *namespace) list(ctx context.Context) (*listing, error) {
	lst := &listing{entries: make(map[string]time.Time), start: time.Now()}
	for obj := range n.b.client.ListObjects(ctx, n.b.bucket, minio.ListObjectsOptions{Prefix: n.segPrefix(), Recursive: true}) {
		if obj.Err != nil {
			return nil, errors.WithStack(obj.Err)
		}
		key := strings.TrimPrefix(obj.Key, n.prefix())
		lst.entries[key] = obj.LastModified
		if obj.LastModified.After(lst.newest) {
			lst.newest = obj.LastModified
		}
	}
	// minio-go closes the listing channel without an error if the context
	// is cancelled between pages; a truncated listing must never count as
	// successful — eviction and rebuild would be unsound.
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return lst, nil
}

// refreshRollup conditionally fetches the rollup, seeding it if none exists.
// The held (etag, state, mark) triple is only ever replaced as a unit.
func (n *namespace) refreshRollup(ctx context.Context) error {
	opts := minio.GetObjectOptions{}
	if n.rollup != nil {
		if err := opts.SetMatchETagExcept(n.rollup.etag); err != nil {
			return errors.Wrap(err, "set if-none-match")
		}
	}
	obj, err := n.b.client.GetObject(ctx, n.b.bucket, n.rollupKey(), opts)
	if err != nil {
		return errors.Wrap(err, "get rollup")
	}
	defer obj.Close() //nolint:errcheck // read-only body
	data, err := io.ReadAll(obj)
	switch {
	case isNotModified(err):
		return nil
	case isNotFound(err):
		return n.seed(ctx)
	case err != nil:
		return errors.Wrap(err, "read rollup")
	}
	info, err := obj.Stat()
	if err != nil {
		return errors.Wrap(err, "stat rollup")
	}
	return n.adoptRollup(info.ETag, data)
}

func (n *namespace) adoptRollup(etag string, data []byte) error {
	var body rollupBody
	if err := json.Unmarshal(data, &body); err != nil {
		return errors.Wrap(err, "unmarshal rollup")
	}
	// Validate the inner state now so replay can rely on it; a tampered
	// rollup aborts the tick (loudly, every tick) rather than serving
	// state silently rebuilt from nothing.
	var state map[string]any
	if err := json.Unmarshal(body.State, &state); err != nil {
		return errors.Wrap(err, "unmarshal rollup state")
	}
	n.rollup = &heldRollup{etag: etag, mark: body.Mark, state: body.State}
	return nil
}

// seed creates the initial rollup with a zero mark ("covers nothing"), from
// legacy state when present or empty otherwise, so "no rollup exists" is a
// once-per-namespace transient. Concurrent seeders race safely on
// If-None-Match:* and carry identical state; a loser adopts the winner.
func (n *namespace) seed(ctx context.Context) error {
	state, err := n.readLegacy(ctx)
	if err != nil {
		return errors.Wrap(err, "read legacy state")
	}
	if state == nil {
		state = []byte("{}")
	}
	body := rollupBody{State: state}
	data, err := json.Marshal(body)
	if err != nil {
		return errors.Wrap(err, "marshal rollup")
	}
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	opts.SetMatchETagExcept("*")
	info, err := n.b.client.PutObject(ctx, n.b.bucket, n.rollupKey(),
		bytes.NewReader(data), int64(len(data)), opts)
	if err == nil {
		n.rollup = &heldRollup{etag: info.ETag, mark: body.Mark, state: state}
		return nil
	}
	if !isPreconditionFailed(err) && !isConflict(err) {
		return errors.Wrap(err, "seed rollup")
	}
	// Lost the seed race: adopt the winner's rollup (and its ETag, so a
	// later compaction this tick still holds one).
	n.rollup = nil
	return errors.Wrap(n.refreshRollup(ctx), "adopt winning seed")
}

// readLegacy fetches the pre-journal state object. It returns (nil, nil)
// when absent — or present but corrupt, which is logged loudly and treated
// as absent, since aborting forever on a permanently corrupt object would
// freeze the namespace. "Corrupt" applies only to a complete read: a
// truncated body surfaces as a read error and aborts the tick, so a mid-body
// connection reset cannot shadow intact legacy state.
func (n *namespace) readLegacy(ctx context.Context) ([]byte, error) {
	obj, err := n.b.client.GetObject(ctx, n.b.bucket, n.legacyKey(), minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "get legacy")
	}
	defer obj.Close() //nolint:errcheck // read-only body
	data, err := io.ReadAll(obj)
	if isNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read legacy")
	}
	// "Unparseable" means unparseable as namespace state: a valid JSON
	// scalar or array would seed a rollup that every replay and compaction
	// chokes on forever.
	var state map[string]any
	if len(data) == 0 || json.Unmarshal(data, &state) != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "legacy metadata state is corrupt; treating as absent",
			"namespace", n.name, "key", n.legacyKey(), "size", len(data))
		return nil, nil
	}
	return data, nil
}

// fetchUnseen stamps already-cached entries from the listing, then fetches
// listed segments above the mark that are not in the committed cache into a
// private staging area. Staging is invisible to the writer and to the
// "unseen" computation; it is committed to the cache only at swap time.
func (n *namespace) fetchUnseen(ctx context.Context, lst *listing) (map[string]*cacheEntry, error) {
	var unseen []string
	n.stateMu.Lock()
	for key, lm := range lst.entries {
		if e, ok := n.cache[key]; ok {
			// Stamp adoption: the first listing that includes a cached
			// entry supplies its canonical position — including entries at
			// or below the mark, which eviction needs the stamp for.
			// Adopted stamps are the object's true immutable LastModified,
			// so they survive a later abort harmlessly.
			if !e.listed {
				e.listed = true
				e.lm = lm
			}
			continue
		}
		if n.rollup.mark.covers(lm, key) {
			continue
		}
		unseen = append(unseen, key)
	}
	n.stateMu.Unlock()

	staged := make(map[string]*cacheEntry, len(unseen))
	var stagedMu sync.Mutex
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(fetchConcurrency)
	for _, key := range unseen {
		eg.Go(func() error {
			ops, err := n.fetchSegment(egCtx, key)
			if err != nil {
				return errors.Wrapf(err, "fetch %s", key)
			}
			stagedMu.Lock()
			staged[key] = &cacheEntry{ops: ops, lm: lst.entries[key], listed: true, insertedAt: time.Now()}
			stagedMu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.WithStack(err)
	}
	return staged, nil
}

func (n *namespace) fetchSegment(ctx context.Context, key string) ([]metadatadb.Op, error) {
	obj, err := n.b.client.GetObject(ctx, n.b.bucket, n.prefix()+key, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "get segment")
	}
	defer obj.Close() //nolint:errcheck // read-only body
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, errors.Wrap(err, "read segment")
	}
	return unmarshalSegment(data)
}

// rebuild evicts covered and folded-while-unlisted cache entries, snapshots
// the replay input (committed cache ∪ staging, staged copies winning),
// replays it unlocked, then re-applies any writer inserts that landed after
// the snapshot, commits staging into the cache, and swaps the state in.
func (n *namespace) rebuild(lst *listing, staged map[string]*cacheEntry) {
	n.stateMu.Lock()
	for key, e := range n.cache {
		if e.listed {
			if n.rollup.mark.covers(e.lm, key) {
				delete(n.cache, key)
			}
			continue
		}
		// An unlisted entry absent from a LIST that started after its
		// insert was necessarily folded (its PUT completed before the LIST
		// began, and only compaction removes segments), so its ops are in
		// the rollup this tick holds.
		if _, listed := lst.entries[key]; !listed && e.insertedAt.Before(lst.start) {
			delete(n.cache, key)
		}
	}
	input := make(map[string]*cacheEntry, len(n.cache)+len(staged))
	maps.Copy(input, n.cache)
	maps.Copy(input, staged) // staged (stamped) copies win the union
	n.stateMu.Unlock()

	state := n.replay(input)

	n.stateMu.Lock()
	// Late inserts are unlisted, so key order matches write order under the
	// serialized writer; map iteration order would reorder same-key ops.
	var late []string
	for key := range n.cache {
		if _, ok := input[key]; !ok {
			late = append(late, key)
		}
	}
	slices.Sort(late)
	for _, key := range late {
		for _, o := range n.cache[key].ops {
			metadatadb.ApplyOp(state, o)
		}
	}
	maps.Copy(n.cache, staged)
	n.state = state
	n.stateMu.Unlock()
}

// replay rebuilds state from the rollup plus the input entries in canonical
// order: listed entries above the mark by (LastModified, key), then unlisted
// entries (which always pass the mark filter) by key.
func (n *namespace) replay(input map[string]*cacheEntry) map[string]any {
	type replayEntry struct {
		key string
		e   *cacheEntry
	}
	entries := make([]replayEntry, 0, len(input))
	for key, e := range input {
		if e.listed && n.rollup.mark.covers(e.lm, key) {
			continue
		}
		entries = append(entries, replayEntry{key: key, e: e})
	}
	slices.SortFunc(entries, func(a, b replayEntry) int {
		if a.e.listed != b.e.listed {
			if a.e.listed {
				return -1
			}
			return 1
		}
		if a.e.listed {
			if c := a.e.lm.Compare(b.e.lm); c != 0 {
				return c
			}
		}
		return strings.Compare(a.key, b.key)
	})

	state := make(map[string]any)
	if err := json.Unmarshal(n.rollup.state, &state); err != nil {
		// adoptRollup and seed validate the state object, so this cannot
		// happen short of memory corruption; never serve a torn state.
		logging.FromContext(n.b.ctx).ErrorContext(n.b.ctx, "unmarshal held rollup state", "namespace", n.name, "error", err)
		state = make(map[string]any)
	}
	for _, re := range entries {
		for _, o := range re.e.ops {
			metadatadb.ApplyOp(state, o)
		}
	}
	return state
}
