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
	metadatadbtest.SkipUnlessSoak(t)
	bucket := s3clienttest.Start(t)
	metadatadbtest.Soak(t, newBackend(t, bucket), metadatadbtest.SoakConfig{
		Duration:    5 * time.Second,
		Concurrency: 4,
		NumKeys:     10,
	})
}

func TestS3BackendSoakReplicas(t *testing.T) {
	metadatadbtest.SkipUnlessSoak(t)
	bucket := s3clienttest.Start(t)
	backends := make([]metadatadb.Backend, 3)
	for i := range backends {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
		b, err := New(ctx, s3client.ClientProvider(func() (*minio.Client, error) { return s3clienttest.Client(t), nil }), Config{
			Bucket:       bucket,
			SyncInterval: 500 * time.Millisecond, // real background ticks
		})
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, b.Close(context.Background())) })
		// Aggressive thresholds so compactions, elections, and probes fire
		// repeatedly during the run.
		b.ageThreshold = 2 * time.Second
		b.segmentThreshold = 8
		backends[i] = b
	}

	metadatadbtest.SoakReplicas(t, backends, metadatadbtest.SoakConfig{
		Duration:    10 * time.Second,
		Concurrency: 4,
		NumKeys:     20,
	})
}

func TestWireRoundTrip(t *testing.T) {
	ops := []metadatadb.Op{
		metadatadb.ScalarSet{Key: "s", Value: "hello"},
		metadatadb.ScalarDelete{Key: "s"},
		metadatadb.IntSet{Key: "i", Value: 1 << 60}, // int64 fields must round-trip exactly
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
		metadatadb.ScalarSet{Key: "big", Value: int64(1<<53 + 1)}, // any-typed values must round-trip exactly too
		metadatadb.MapSet{Key: "bigm", MapKey: int64(1<<53 + 3), Value: int64(1<<53 + 5)},
	}
	data, err := marshalSegment(ops)
	assert.NoError(t, err)
	decoded, err := unmarshalSegment(data)
	assert.NoError(t, err)
	assert.Equal(t, len(ops), len(decoded))

	want, got := map[string]any{}, map[string]any{}
	for _, o := range ops {
		metadatadb.ApplyOp(want, o)
	}
	for _, o := range decoded {
		metadatadb.ApplyOp(got, o)
	}
	// Compare via JSON: identical encodings, differing Go types (int64 on
	// the applied side, json.Number on the decoded side).
	wantJSON, err := json.Marshal(want)
	assert.NoError(t, err)
	gotJSON, err := json.Marshal(got)
	assert.NoError(t, err)
	assert.Equal(t, string(wantJSON), string(gotJSON))
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

	lst, err := b.namespace("gc").list(b.ctx)
	assert.NoError(t, err)
	assert.True(t, len(lst.entries) < writers, "expected group commit to coalesce: %d segments for %d writers", len(lst.entries), writers)

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

	for range 3 {
		lst, err := n.tick(b.ctx)
		assert.NoError(t, err)
		assert.NoError(t, n.ladder(b.ctx, lst))
	}

	assert.NotZero(t, n.rollup.mark.Key, "expected a compaction to have committed a rollup with a mark")
	var v int64
	assert.NoError(t, b.Query(ctx, "compact", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(4), v)

	lst, err := n.list(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(lst.entries))

	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, "compact"))
	assert.NoError(t, b2.Query(ctx, "compact", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, int64(4), v)
}

func TestLargeIntSurvivesRollup(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	b.initialTick = false // ticks are driven manually below
	b.segmentThreshold = 2
	b.ageThreshold = time.Second
	b.jitter = func() time.Duration { return 0 }
	ctx := t.Context()

	const big = int64(1<<53 + 1) // not representable as float64
	n := b.namespace("bigint")
	assert.NoError(t, b.Apply(ctx, "bigint", metadatadb.IntSet{Key: "n", Value: big}))
	assert.NoError(t, b.Apply(ctx, "bigint", metadatadb.IntMapSet{Key: "m", MapKey: "k", Value: big}))
	time.Sleep(1200 * time.Millisecond)
	assert.NoError(t, b.Apply(ctx, "bigint", metadatadb.IntAdd{Key: "n", Delta: 1}))

	for range 3 {
		lst, err := n.tick(b.ctx)
		assert.NoError(t, err)
		assert.NoError(t, n.ladder(b.ctx, lst))
	}
	assert.NotZero(t, n.rollup.mark.Key, "expected a compaction to have committed a rollup")

	// A cold replica rebuilding from the rollup must see the exact values.
	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, "bigint"))
	var v int64
	assert.NoError(t, b2.Query(ctx, "bigint", metadatadb.IntGet{Key: "n"}, &v))
	assert.Equal(t, big+1, v)
	assert.NoError(t, b2.Query(ctx, "bigint", metadatadb.IntMapGet{Key: "m", MapKey: "k"}, &v))
	assert.Equal(t, big, v)
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

	// The stall condition must hold two consecutive ticks to fire the probe.
	for range 2 {
		lst, err := n.tick(b.ctx)
		assert.NoError(t, err)
		assert.NoError(t, n.ladder(b.ctx, lst))
	}

	lst, err := n.list(b.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(lst.entries), "expected a probe segment to have been written")

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

	assert.NoError(t, b.Apply(ctx, "legacy", metadatadb.IntAdd{Key: "counter", Delta: 1}))
	assert.NoError(t, b.Flush(ctx, "legacy"))
	assert.NoError(t, b.Query(ctx, "legacy", metadatadb.IntGet{Key: "counter"}, &v))
	assert.Equal(t, int64(43), v)
}

func TestFlushNSSkipsInitialTick(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	assert.False(t, b.namespaceForFlush("flush-first").runInitialTick)
	assert.True(t, b.namespace("apply-first").runInitialTick)
}

func TestEmptyRootNamespace(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	ctx := t.Context()

	assert.NoError(t, b.Apply(ctx, "", metadatadb.MapSet{Key: "cache-etags", MapKey: "obj", Value: `"etag"`}))

	// The root namespace maps to the reserved .root directory.
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

	// Repeated to cover both select arms in apply's send.
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
	b := newBackend(t, bucket)

	for _, tt := range []struct {
		name string
		body string
	}{
		{"Garbage", "{not json"},
		{"Null", "null"},
		{"Scalar", `42`},
		{"Array", `[1, 2]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ns := "corrupt-" + strings.ToLower(tt.name)

			_, err := client.PutObject(ctx, bucket, ".metadata/"+ns+".json",
				strings.NewReader(tt.body), int64(len(tt.body)), minio.PutObjectOptions{})
			assert.NoError(t, err)

			assert.NoError(t, b.Flush(ctx, ns))

			var v int64
			assert.NoError(t, b.Query(ctx, ns, metadatadb.IntGet{Key: "counter"}, &v))
			assert.Equal(t, int64(0), v)

			assert.NoError(t, b.Apply(ctx, ns, metadatadb.IntAdd{Key: "counter", Delta: 1}))
			assert.NoError(t, b.Flush(ctx, ns))
			assert.NoError(t, b.Query(ctx, ns, metadatadb.IntGet{Key: "counter"}, &v))
			assert.Equal(t, int64(1), v)
		})
	}
}

func TestStructKeysSurviveWire(t *testing.T) {
	bucket := s3clienttest.Start(t)
	b := newBackend(t, bucket)
	ctx := t.Context()

	// Field order (Z before A) differs from sorted JSON key order, so this
	// fails if local and replayed key encodings disagree.
	type structKey struct {
		Z string `json:"z"`
		A string `json:"a"`
	}
	key := structKey{Z: "zed", A: "ay"}
	assert.NoError(t, b.Apply(ctx, "sk", metadatadb.MapSet{Key: "m", MapKey: key, Value: "v"}))

	check := func(backend *Backend) {
		t.Helper()
		var result struct {
			Value string
			OK    bool
		}
		assert.NoError(t, backend.Query(ctx, "sk", metadatadb.MapGet{Key: "m", MapKey: key}, &result))
		assert.True(t, result.OK)
		assert.Equal(t, "v", result.Value)
	}
	check(b)

	// A cold replica replays the key from the wire format.
	b2 := newBackend(t, bucket)
	assert.NoError(t, b2.Flush(ctx, "sk"))
	check(b2)

	// A delete with the original struct key must remove the replayed entry.
	assert.NoError(t, b2.Apply(ctx, "sk", metadatadb.MapDelete{Key: "m", MapKey: key}))
	var result struct {
		Value string
		OK    bool
	}
	assert.NoError(t, b2.Query(ctx, "sk", metadatadb.MapGet{Key: "m", MapKey: key}, &result))
	assert.False(t, result.OK)
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

	// Both first ticks race the If-None-Match:* seed; the loser must adopt
	// the winner and still complete its tick.
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
