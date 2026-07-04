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

// listing is one tick's full-prefix LIST. newest is the aging reference
// clock; start is monotonic, compared only against cache insert times.
type listing struct {
	entries map[string]time.Time
	newest  time.Time
	start   time.Time
}

type flushReq struct {
	ctx   context.Context //nolint:containedctx // carries the Flush deadline into the tick
	reply chan error
}

func (n *namespace) flush(ctx context.Context) error {
	req := flushReq{ctx: ctx, reply: make(chan error, 1)}
	select {
	case n.flushCh <- req:
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-n.b.ctx.Done():
		return errors.New("backend closed")
	}
	select {
	case err := <-req.reply:
		return errors.WithStack(err)
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	}
}

// flushTick runs a tick under the Flush caller's context so an expired
// deadline aborts it promptly instead of occupying the loop, while backend
// Close still cancels it.
func (n *namespace) flushTick(ctx context.Context) error {
	tickCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(n.b.ctx, cancel)
	defer stop()
	_, err := n.tick(tickCtx)
	return err
}

// maxConsecutiveFlushes bounds the Flush priority so sustained Flush traffic
// cannot starve the background ladder forever.
const maxConsecutiveFlushes = 16

// runLoop serializes all ticks for the namespace; only background ticks run
// the compaction/probe ladder, so a Flush never sleeps a jitter delay.
func (n *namespace) runLoop() {
	defer n.b.wg.Done()
	logger := logging.FromContext(n.b.ctx)
	background := func() {
		lst, err := n.tick(n.b.ctx)
		if err != nil {
			logger.WarnContext(n.b.ctx, "metadata sync tick failed", "namespace", n.name, "error", err)
			return
		}
		// Ladder failures cost staleness only; conditions re-derive next tick.
		if err := n.ladder(n.b.ctx, lst); err != nil {
			logger.WarnContext(n.b.ctx, "metadata compaction failed", "namespace", n.name, "error", err)
		}
	}

	// Immediate first tick so a restarted replica converges without waiting
	// a full interval.
	if n.b.initialTick {
		background()
	}

	ticker := time.NewTicker(n.b.syncInterval)
	defer ticker.Stop()
	flushes := 0
	for {
		// A waiting Flush beats the ticker so it cannot be starved.
		if flushes < maxConsecutiveFlushes {
			select {
			case req := <-n.flushCh:
				req.reply <- n.flushTick(req.ctx)
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
		case req := <-n.flushCh:
			req.reply <- n.flushTick(req.ctx)
			flushes++
		case <-n.b.ctx.Done():
			return
		}
	}
}

// tick is one sync pass, aborting at the first failure; the next tick
// self-heals.
func (n *namespace) tick(ctx context.Context) (*listing, error) {
	lst, err := n.list(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "list segments")
	}
	// LIST must precede the rollup GET: the rollup is then at least as new
	// as the listing, so a rebuild can never have a coverage hole.
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
	// minio-go closes the channel without error on mid-listing cancellation;
	// a truncated listing must never count as successful.
	if err := ctx.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return lst, nil
}

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
	// Validate the inner state now so replay can rely on it: a tampered
	// rollup aborts the tick rather than silently serving empty state.
	if _, err := unmarshalState(body.State); err != nil {
		return errors.Wrap(err, "unmarshal rollup state")
	}
	n.rollup = &heldRollup{etag: etag, mark: body.Mark, state: body.State}
	return nil
}

// seed creates the initial rollup — legacy state or empty, zero mark — so
// "no rollup exists" is a once-per-namespace transient.
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
	// Lost the seed race: adopt the winner's rollup and ETag.
	n.rollup = nil
	return errors.Wrap(n.refreshRollup(ctx), "adopt winning seed")
}

// readLegacy fetches the pre-journal state object, returning (nil, nil) when
// absent or corrupt. Only a complete read counts as corrupt: a truncated body
// is a read error that aborts the tick, so a mid-body connection reset cannot
// shadow intact legacy state.
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
	// Must parse as namespace state: a valid JSON scalar or array would
	// seed a rollup that every replay and compaction chokes on forever.
	if _, err := unmarshalState(data); len(data) == 0 || err != nil {
		logging.FromContext(ctx).ErrorContext(ctx, "legacy metadata state is corrupt; treating as absent",
			"namespace", n.name, "key", n.legacyKey(), "size", len(data))
		return nil, nil
	}
	return data, nil
}

// fetchUnseen stamps already-cached entries, then fetches unseen segments
// above the mark into staging, which stays invisible to the writer until
// committed at swap time.
func (n *namespace) fetchUnseen(ctx context.Context, lst *listing) (map[string]*cacheEntry, error) {
	var unseen []string
	n.stateMu.Lock()
	for key, lm := range lst.entries {
		if e, ok := n.cache[key]; ok {
			// Stamp adoption supplies a cached entry's canonical position —
			// including entries at or below the mark, whose eviction needs it.
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

// rebuild evicts, snapshots the replay input, replays it unlocked, then
// re-applies late writer inserts, commits staging, and swaps the state in.
func (n *namespace) rebuild(lst *listing, staged map[string]*cacheEntry) {
	n.stateMu.Lock()
	for key, e := range n.cache {
		if e.listed {
			if n.rollup.mark.covers(e.lm, key) {
				delete(n.cache, key)
			}
			continue
		}
		// An unlisted entry absent from a LIST that started after its insert
		// was necessarily folded: its ops are in the rollup this tick holds.
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
	// Late inserts replay in key order == write order; map iteration order
	// would reorder same-key ops.
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

// replay rebuilds state from the rollup plus input entries above the mark in
// canonical order; unlisted entries always pass the filter and order last.
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

	// The state object was validated at adoption; never serve a torn state.
	state, err := unmarshalState(n.rollup.state)
	if err != nil {
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
