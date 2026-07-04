package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
)

func newBackend(t *testing.T, bucket string) *Backend {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	b, err := New(ctx, s3client.ClientProvider(func() (*minio.Client, error) { return s3clienttest.Client(t), nil }), Config{
		Bucket:       bucket,
		SyncInterval: time.Hour, // ticks are driven by Flush (or directly) in tests
	})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, b.Close(context.Background())) })
	return b
}

func TestS3Backend(t *testing.T) {
	bucket := s3clienttest.Start(t)
	metadatadbtest.Suite(t, func(t *testing.T, n int) []metadatadb.Backend {
		t.Helper()
		backends := make([]metadatadb.Backend, n)
		for i := range backends {
			backends[i] = newBackend(t, bucket)
		}
		return backends
	})
}

func TestS3BackendSoak(t *testing.T) {
	bucket := s3clienttest.Start(t)
	metadatadbtest.Soak(t, newBackend(t, bucket), metadatadbtest.SoakConfig{
		Duration:    5 * time.Second,
		Concurrency: 4,
		NumKeys:     10,
	})
}

func TestWireRoundTrip(t *testing.T) {
	ops := []metadatadb.Op{
		metadatadb.ScalarSet{Key: "s", Value: "hello"},
		metadatadb.ScalarDelete{Key: "s"},
		metadatadb.IntSet{Key: "i", Value: 42},
		metadatadb.IntAdd{Key: "i", Delta: -3},
		metadatadb.IntMul{Key: "i", Factor: 2},
		metadatadb.IntDiv{Key: "i", Divisor: 4},
		metadatadb.SetAdd{Key: "set", Member: "a"},
		metadatadb.SetRemove{Key: "set", Member: "a"},
		metadatadb.IntMapSet{Key: "im", MapKey: "k", Value: 7},
		metadatadb.IntMapAdd{Key: "im", MapKey: "k", Delta: 1},
		metadatadb.IntMapMul{Key: "im", MapKey: "k", Factor: 3},
		metadatadb.IntMapDiv{Key: "im", MapKey: "k", Divisor: 2},
		metadatadb.IntMapDelete{Key: "im", MapKey: "k"},
		metadatadb.MapSet{Key: "m", MapKey: "k", Value: "v"},
		metadatadb.MapDelete{Key: "m", MapKey: "k"},
		metadatadb.ListAppend{Key: "l", Value: "entry"},
	}
	data, err := marshalSegment(ops)
	assert.NoError(t, err)
	decoded, err := unmarshalSegment(data)
	assert.NoError(t, err)
	assert.Equal(t, len(ops), len(decoded))

	// Replaying original and decoded ops must yield identical state.
	want, got := map[string]any{}, map[string]any{}
	for _, o := range ops {
		metadatadb.ApplyOp(want, o)
	}
	for _, o := range decoded {
		metadatadb.ApplyOp(got, o)
	}
	assert.Equal(t, want, got)
}

func TestUUIDv7Monotonic(t *testing.T) {
	prev := ""
	for range 10000 {
		id, err := uuid.NewV7()
		assert.NoError(t, err)
		s := id.String()
		assert.True(t, s > prev, "UUIDv7 not strictly monotonic: %s then %s", prev, s)
		prev = s
	}
}

func TestReadYourOwnWrites(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	ctx := t.Context()

	assert.NoError(t, b.Apply(ctx, "rw", metadatadb.IntAdd{Key: "n", Delta: 5}))
	var v int64
	assert.NoError(t, b.Query(ctx, "rw", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(5), v)
}

func TestGroupCommitConcurrent(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	ctx := t.Context()

	const writers = 20
	wg := sync.WaitGroup{}
	for range writers {
		wg.Go(func() {
			assert.NoError(t, b.Apply(ctx, "gc", metadatadb.IntAdd{Key: "n", Delta: 1}))
		})
	}
	wg.Wait()

	var v int64
	assert.NoError(t, b.Query(ctx, "gc", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(writers), v)

	// Concurrent Applies must have coalesced into fewer segments.
	lst, err := b.namespace("gc").list(b.ctx)
	assert.NoError(t, err)
	assert.True(t, len(lst.entries) < writers, "expected group commit to coalesce: %d segments for %d writers", len(lst.entries), writers)

	// A second replica must converge to the same total from the journal.
	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, "gc"))
	assert.NoError(t, b2.Query(ctx, "gc", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(writers), v)
}

func TestCompaction(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	b.initialTick = false // ticks are driven manually below
	b.segmentThreshold = 2
	b.ageThreshold = time.Second
	b.jitter = func() time.Duration { return 0 }
	ctx := t.Context()

	n := b.namespace("compact")
	for range 3 {
		assert.NoError(t, b.Apply(ctx, "compact", metadatadb.IntAdd{Key: "n", Delta: 1}))
	}
	// Age the tail past the threshold relative to a newer organic write.
	time.Sleep(1200 * time.Millisecond)
	assert.NoError(t, b.Apply(ctx, "compact", metadatadb.IntAdd{Key: "n", Delta: 1}))

	// Two sustained ticks trigger the election; the third proves stability.
	for range 3 {
		lst, err := n.tick(b.ctx)
		assert.NoError(t, err)
		assert.NoError(t, n.ladder(b.ctx, lst))
	}

	assert.NotZero(t, n.rollup.mark.Key, "expected a compaction to have committed a rollup with a mark")
	var v int64
	assert.NoError(t, b.Query(ctx, "compact", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(4), v)

	// The folded segments must be deleted; only the young write remains.
	lst, err := n.list(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lst.entries))

	// A cold replica converges from rollup + tail alone.
	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, "compact"))
	assert.NoError(t, b2.Query(ctx, "compact", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(4), v)
}

func TestClockProbe(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	b.initialTick = false // ticks are driven manually below
	b.segmentThreshold = 2
	b.ageThreshold = time.Hour // nothing can age organically
	b.jitter = func() time.Duration { return 0 }
	ctx := t.Context()

	n := b.namespace("probe")
	for range 2 {
		assert.NoError(t, b.Apply(ctx, "probe", metadatadb.IntAdd{Key: "n", Delta: 1}))
	}

	// Stall condition: live ≥ threshold, zero candidates. Two sustained
	// ticks fire the probe.
	for range 2 {
		lst, err := n.tick(b.ctx)
		assert.NoError(t, err)
		assert.NoError(t, n.ladder(b.ctx, lst))
	}

	lst, err := n.list(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(lst.entries), "expected a probe segment to have been written")

	// The probe is a no-op in replay.
	assert.NoError(t, b.Flush(ctx, "probe"))
	var v int64
	assert.NoError(t, b.Query(ctx, "probe", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(2), v)
}

func TestLegacySeed(t *testing.T) {
	bucket := s3clienttest.Start(t)
	client := s3clienttest.Client(t)
	ctx := t.Context()

	legacy := []byte(`{"counter": 42, "labels": {"\"a\"": "alpha"}}`)
	_, err := client.PutObject(ctx, bucket, ".metadata/legacy.json",
		bytes.NewReader(legacy), int64(len(legacy)), minio.PutObjectOptions{ContentType: "application/json"})
	assert.NoError(t, err)

	b := newBackend(t, bucket)
	assert.NoError(t, b.Flush(ctx, "legacy"))

	var v int64
	assert.NoError(t, b.Query(ctx, "legacy", metadatadb.IntGet{Key: "counter"}, &v))
	assert.Equal(t, int64(42), v)
	var result struct {
		Value string
		OK    bool
	}
	assert.NoError(t, b.Query(ctx, "legacy", metadatadb.MapGet{Key: "labels", MapKey: "a"}, &result))
	assert.True(t, result.OK)
	assert.Equal(t, "alpha", result.Value)

	// New writes replay on top of the seeded state.
	assert.NoError(t, b.Apply(ctx, "legacy", metadatadb.IntAdd{Key: "counter", Delta: 1}))
	assert.NoError(t, b.Flush(ctx, "legacy"))
	assert.NoError(t, b.Query(ctx, "legacy", metadatadb.IntGet{Key: "counter"}, &v))
	assert.Equal(t, int64(43), v)
}

func TestEmptyRootNamespace(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	ctx := t.Context()

	assert.NoError(t, b.Apply(ctx, "", metadatadb.MapSet{Key: "cache-etags", MapKey: "obj", Value: `"etag"`}))

	// The root namespace maps to the reserved .root directory (MinIO
	// rejects the "//" an empty component would produce).
	client := s3clienttest.Client(t)
	found := false
	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: ".metadata/.root/", Recursive: true}) {
		assert.NoError(t, obj.Err)
		if strings.HasPrefix(obj.Key, ".metadata/.root/segment-") {
			found = true
		}
	}
	assert.True(t, found, "expected a segment under the root namespace prefix")

	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, ""))
	var result struct {
		Value string
		OK    bool
	}
	assert.NoError(t, b2.Query(ctx, "", metadatadb.MapGet{Key: "cache-etags", MapKey: "obj"}, &result))
	assert.True(t, result.OK)
	assert.Equal(t, `"etag"`, result.Value)
}

func TestCloseIdempotent(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	assert.NoError(t, b.Apply(t.Context(), "close", metadatadb.IntAdd{Key: "n", Delta: 1}))
	assert.NoError(t, b.Close(t.Context()))
	assert.NoError(t, b.Close(t.Context()))
}

func TestApplyAfterCloseDoesNotHang(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	assert.NoError(t, b.Close(t.Context()))

	// Callers pass context.Background() in production, so Apply must be
	// unblocked by backend shutdown alone. Repeat to cover both select arms.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			assert.Error(t, b.Apply(context.Background(), "hang", metadatadb.IntAdd{Key: "n", Delta: 1}))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Apply hung after Close")
	}
}

func TestLegacyCorruptTreatedAsAbsent(t *testing.T) {
	bucket := s3clienttest.Start(t)
	client := s3clienttest.Client(t)
	ctx := t.Context()

	garbage := []byte("{not json")
	_, err := client.PutObject(ctx, bucket, ".metadata/corrupt.json",
		bytes.NewReader(garbage), int64(len(garbage)), minio.PutObjectOptions{})
	assert.NoError(t, err)

	b := newBackend(t, bucket)
	assert.NoError(t, b.Flush(ctx, "corrupt"))

	var v int64
	assert.NoError(t, b.Query(ctx, "corrupt", metadatadb.IntGet{Key: "counter"}, &v))
	assert.Equal(t, int64(0), v)

	// The namespace is not frozen: writes proceed on the empty seed.
	assert.NoError(t, b.Apply(ctx, "corrupt", metadatadb.IntAdd{Key: "counter", Delta: 1}))
	assert.NoError(t, b.Flush(ctx, "corrupt"))
	assert.NoError(t, b.Query(ctx, "corrupt", metadatadb.IntGet{Key: "counter"}, &v))
	assert.Equal(t, int64(1), v)
}

func TestLeftoverCleanup(t *testing.T) {
	bucket := s3clienttest.Start(t)
	client := s3clienttest.Client(t)
	b := newBackend(t, bucket)
	b.initialTick = false    // ticks are driven manually below
	b.segmentThreshold = 100 // never elect; leftovers only
	b.ageThreshold = time.Second
	b.jitter = func() time.Duration { return 0 }
	ctx := t.Context()

	for range 2 {
		assert.NoError(t, b.Apply(ctx, "leftover", metadatadb.IntAdd{Key: "n", Delta: 1}))
	}
	n := b.namespace("leftover")
	lst, err := n.tick(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(lst.entries))

	// Simulate a compactor that committed a rollup covering both segments
	// and died before its deletes.
	var m mark
	for key, lm := range lst.entries {
		if !m.covers(lm, key) {
			m = mark{LM: lm, Key: key}
		}
	}
	body, err := json.Marshal(rollupBody{State: []byte(`{"n": 2}`), Mark: m})
	assert.NoError(t, err)
	_, err = client.PutObject(ctx, bucket, ".metadata/leftover/rollup.json",
		bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "application/json"})
	assert.NoError(t, err)

	// A newer write lets the leftovers age past the threshold.
	time.Sleep(1200 * time.Millisecond)
	assert.NoError(t, b.Apply(ctx, "leftover", metadatadb.IntAdd{Key: "n", Delta: 1}))

	lst, err = n.tick(b.ctx)
	assert.NoError(t, err)
	assert.NoError(t, n.ladder(b.ctx, lst))

	// The folded leftovers are deleted; only the young write remains, and
	// state reflects rollup + tail without double-applying.
	lst, err = n.list(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lst.entries))
	var v int64
	assert.NoError(t, b.Query(ctx, "leftover", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(3), v)
}

func TestSeedRace(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b1 := newBackend(t, bucket)
	b2 := newBackend(t, bucket)
	ctx := t.Context()

	assert.NoError(t, b1.Apply(ctx, "race", metadatadb.IntAdd{Key: "n", Delta: 1}))
	assert.NoError(t, b2.Apply(ctx, "race", metadatadb.IntAdd{Key: "n", Delta: 1}))

	// Both first ticks race to seed the rollup with If-None-Match:*; the
	// loser must adopt the winner and still complete its tick.
	wg := sync.WaitGroup{}
	for _, b := range []*Backend{b1, b2} {
		wg.Go(func() { assert.NoError(t, b.Flush(ctx, "race")) })
	}
	wg.Wait()

	for _, b := range []*Backend{b1, b2} {
		var v int64
		assert.NoError(t, b.Query(ctx, "race", metadatadb.IntGet{Key: "n"}, &v))
		assert.Equal(t, int64(2), v)
	}
}
