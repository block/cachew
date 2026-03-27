package metadatadbtest

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// Suite runs a comprehensive test suite against a metadatadb.Backend implementation.
func Suite(t *testing.T, newBackend func(t *testing.T) metadatadb.Backend) {
	t.Run("Scalar", func(t *testing.T) {
		testScalar(t, newBackend(t))
	})
	t.Run("ScalarDelete", func(t *testing.T) {
		testScalarDelete(t, newBackend(t))
	})
	t.Run("ScalarStruct", func(t *testing.T) {
		testScalarStruct(t, newBackend(t))
	})
	t.Run("Int", func(t *testing.T) {
		testInt(t, newBackend(t))
	})
	t.Run("IntArithmetic", func(t *testing.T) {
		testIntArithmetic(t, newBackend(t))
	})
	t.Run("IntDivByZero", func(t *testing.T) {
		testIntDivByZero(t, newBackend(t))
	})
	t.Run("Set", func(t *testing.T) {
		testSet(t, newBackend(t))
	})
	t.Run("SetRemove", func(t *testing.T) {
		testSetRemove(t, newBackend(t))
	})
	t.Run("Map", func(t *testing.T) {
		testMap(t, newBackend(t))
	})
	t.Run("MapDelete", func(t *testing.T) {
		testMapDelete(t, newBackend(t))
	})
	t.Run("MapStruct", func(t *testing.T) {
		testMapStruct(t, newBackend(t))
	})
	t.Run("IntMap", func(t *testing.T) {
		testIntMap(t, newBackend(t))
	})
	t.Run("IntMapIncr", func(t *testing.T) {
		testIntMapIncr(t, newBackend(t))
	})
	t.Run("List", func(t *testing.T) {
		testList(t, newBackend(t))
	})
	t.Run("NamespaceIsolation", func(t *testing.T) {
		testNamespaceIsolation(t, newBackend(t))
	})
	t.Run("FlushPersists", func(t *testing.T) {
		testFlushPersists(t, newBackend(t))
	})
	t.Run("FlushRoundTrip", func(t *testing.T) {
		testFlushRoundTrip(t, newBackend(t))
	})
	t.Run("TwoStoresSync", func(t *testing.T) {
		testTwoStoresSync(t, newBackend(t))
	})
	t.Run("TokenMismatch", func(t *testing.T) {
		testTokenMismatch(t, newBackend(t))
	})
}

func testContext() context.Context {
	return logging.ContextWithLogger(context.Background(), slog.Default())
}

func newStore(t *testing.T, backend metadatadb.Backend) *metadatadb.Store {
	t.Helper()
	cfg := metadatadb.Config{SyncInterval: time.Hour, LockTTL: 5 * time.Second}
	s := metadatadb.New(testContext(), cfg, backend)
	t.Cleanup(func() { assert.NoError(t, s.Close()) })
	return s
}

func testScalar(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	sc := metadatadb.NewScalar[string](ns, "greeting")

	_, ok := sc.Get()
	assert.False(t, ok)

	sc.Set("hello")
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, "hello", v)

	sc.Set("world")
	v, ok = sc.Get()
	assert.True(t, ok)
	assert.Equal(t, "world", v)
}

func testScalarDelete(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	sc := metadatadb.NewScalar[int](ns, "counter")

	sc.Set(42)
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, v)

	sc.Delete()
	_, ok = sc.Get()
	assert.False(t, ok)
}

type testConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func testScalarStruct(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	sc := metadatadb.NewScalar[testConfig](ns, "config")

	sc.Set(testConfig{Host: "localhost", Port: 8080})
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, testConfig{Host: "localhost", Port: 8080}, v)
}

func testInt(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	i := metadatadb.NewInt(ns, "counter")

	assert.Equal(t, int64(0), i.Get())

	i.Set(10)
	assert.Equal(t, int64(10), i.Get())

	i.Add(5)
	assert.Equal(t, int64(15), i.Get())
}

func testIntArithmetic(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	i := metadatadb.NewInt(ns, "val")

	i.Set(100)
	i.Mul(3)
	assert.Equal(t, int64(300), i.Get())

	i.Div(4)
	assert.Equal(t, int64(75), i.Get())

	i.Add(-25)
	assert.Equal(t, int64(50), i.Get())
}

func testIntDivByZero(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	i := metadatadb.NewInt(ns, "val")

	i.Set(42)
	i.Div(0)
	assert.Equal(t, int64(42), i.Get())
}

func testSet(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	set := metadatadb.NewSet[string](ns, "repos")

	assert.Equal(t, []string(nil), set.Members())
	assert.False(t, set.Contains("a"))

	set.Add("a")
	set.Add("b")
	set.Add("a") // duplicate
	assert.False(t, set.Contains("c"))

	assert.Equal(t, []string{"a", "b"}, set.Members())
}

func testSetRemove(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	set := metadatadb.NewSet[string](ns, "tags")

	set.Add("x")
	set.Add("y")
	set.Remove("x")
	set.Remove("z") // non-existent is a no-op

	assert.Equal(t, []string{"y"}, set.Members())
}

func testMap(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	m := metadatadb.NewMap[string, string](ns, "labels")

	_, ok := m.Get("a")
	assert.False(t, ok)
	assert.Equal(t, []string(nil), m.Keys())

	m.Set("a", "alpha")
	m.Set("b", "beta")

	assert.Equal(t, map[string]string{"a": "alpha", "b": "beta"}, m.Entries())
}

func testMapDelete(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	m := metadatadb.NewMap[string, int](ns, "ports")

	m.Set("http", 80)
	m.Set("https", 443)
	m.Delete("http")

	assert.Equal(t, map[string]int{"https": 443}, m.Entries())
}

func testMapStruct(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	m := metadatadb.NewMap[string, testConfig](ns, "services")

	m.Set("web", testConfig{Host: "web.local", Port: 8080})

	v, ok := m.Get("web")
	assert.True(t, ok)
	assert.Equal(t, testConfig{Host: "web.local", Port: 8080}, v)
}

func testIntMap(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	m := metadatadb.NewIntMap[string](ns, "clones")

	assert.Equal(t, int64(0), m.Get("repo-a"))

	m.Set("repo-a", 10)
	m.Set("repo-b", 20)

	assert.Equal(t, map[string]int64{"repo-a": 10, "repo-b": 20}, m.Entries())

	m.Delete("repo-a")
	assert.Equal(t, map[string]int64{"repo-b": 20}, m.Entries())
}

func testIntMapIncr(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	m := metadatadb.NewIntMap[string](ns, "histogram")

	m.Add("repo-a", 1)
	m.Add("repo-a", 1)
	m.Add("repo-b", 5)
	m.Add("repo-a", 3)

	assert.Equal(t, map[string]int64{"repo-a": 5, "repo-b": 5}, m.Entries())
}

func testList(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := s.Namespace("test")
	l := metadatadb.NewList[string](ns, "log")

	assert.Equal(t, 0, l.Len())
	assert.Equal(t, []string(nil), l.Entries())

	l.Append("first")
	l.Append("second")
	l.Append("third")

	assert.Equal(t, 3, l.Len())
	assert.Equal(t, []string{"first", "second", "third"}, l.Entries())
}

func testNamespaceIsolation(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns1 := s.Namespace("ns1")
	ns2 := s.Namespace("ns2")

	sc1 := metadatadb.NewScalar[string](ns1, "key")
	sc2 := metadatadb.NewScalar[string](ns2, "key")

	sc1.Set("from-ns1")
	sc2.Set("from-ns2")

	v1, ok := sc1.Get()
	assert.True(t, ok)
	assert.Equal(t, "from-ns1", v1)

	v2, ok := sc2.Get()
	assert.True(t, ok)
	assert.Equal(t, "from-ns2", v2)
}

func testFlushPersists(t *testing.T, backend metadatadb.Backend) {
	ctx := testContext()
	s := newStore(t, backend)
	ns := s.Namespace("test")

	sc := metadatadb.NewScalar[string](ns, "key")
	sc.Set("value")
	assert.NoError(t, ns.Flush(ctx))

	// Create a new store against the same backend — should see the value.
	s2 := newStore(t, backend)
	ns2 := s2.Namespace("test")
	sc2 := metadatadb.NewScalar[string](ns2, "key")

	// Flush to load remote state.
	assert.NoError(t, ns2.Flush(ctx))

	v, ok := sc2.Get()
	assert.True(t, ok)
	assert.Equal(t, "value", v)
}

func testFlushRoundTrip(t *testing.T, backend metadatadb.Backend) {
	ctx := testContext()
	s := newStore(t, backend)
	ns := s.Namespace("test")

	// Write all data structure types, flush, then read back from a fresh store.
	metadatadb.NewScalar[string](ns, "sc").Set("hello")
	metadatadb.NewScalar[testConfig](ns, "cfg").Set(testConfig{Host: "h", Port: 1})
	i := metadatadb.NewInt(ns, "counter")
	i.Set(10)
	i.Add(5)
	set := metadatadb.NewSet[string](ns, "tags")
	set.Add("a")
	set.Add("b")
	m := metadatadb.NewMap[string, int](ns, "ports")
	m.Set("http", 80)
	im := metadatadb.NewIntMap[string](ns, "clones")
	im.Add("repo-a", 3)
	im.Add("repo-b", 7)
	l := metadatadb.NewList[string](ns, "log")
	l.Append("entry1")
	l.Append("entry2")

	assert.NoError(t, ns.Flush(ctx))

	// Fresh store, same backend.
	s2 := newStore(t, backend)
	ns2 := s2.Namespace("test")
	assert.NoError(t, ns2.Flush(ctx))

	sc, ok := metadatadb.NewScalar[string](ns2, "sc").Get()
	assert.True(t, ok)
	assert.Equal(t, "hello", sc)

	cfg, ok := metadatadb.NewScalar[testConfig](ns2, "cfg").Get()
	assert.True(t, ok)
	assert.Equal(t, testConfig{Host: "h", Port: 1}, cfg)

	assert.Equal(t, int64(15), metadatadb.NewInt(ns2, "counter").Get())
	assert.Equal(t, []string{"a", "b"}, metadatadb.NewSet[string](ns2, "tags").Members())
	assert.Equal(t, map[string]int{"http": 80}, metadatadb.NewMap[string, int](ns2, "ports").Entries())
	assert.Equal(t, map[string]int64{"repo-a": 3, "repo-b": 7}, metadatadb.NewIntMap[string](ns2, "clones").Entries())
	assert.Equal(t, []string{"entry1", "entry2"}, metadatadb.NewList[string](ns2, "log").Entries())
}

func testTwoStoresSync(t *testing.T, backend metadatadb.Backend) {
	ctx := testContext()
	s1 := newStore(t, backend)
	s2 := newStore(t, backend)

	ns1 := s1.Namespace("shared")
	ns2 := s2.Namespace("shared")

	// Store 1 writes and flushes.
	metadatadb.NewScalar[string](ns1, "owner").Set("store1")
	metadatadb.NewInt(ns1, "counter").Add(10)
	assert.NoError(t, ns1.Flush(ctx))

	// Store 2 loads.
	assert.NoError(t, ns2.Flush(ctx))

	v, ok := metadatadb.NewScalar[string](ns2, "owner").Get()
	assert.True(t, ok)
	assert.Equal(t, "store1", v)
	assert.Equal(t, int64(10), metadatadb.NewInt(ns2, "counter").Get())

	// Store 2 writes and flushes.
	metadatadb.NewInt(ns2, "counter").Add(5)
	assert.NoError(t, ns2.Flush(ctx))

	// Store 1 loads.
	assert.NoError(t, ns1.Flush(ctx))
	assert.Equal(t, int64(15), metadatadb.NewInt(ns1, "counter").Get())
}

func testTokenMismatch(t *testing.T, backend metadatadb.Backend) {
	ctx := testContext()

	// Directly exercise the Backend token semantics.
	_, token, err := backend.Load(ctx, "test")
	assert.NoError(t, err)
	assert.NoError(t, backend.Store(ctx, "test", json.RawMessage(`{"key":"v1"}`), token))

	// Two readers load the same version.
	_, token1, err := backend.Load(ctx, "test")
	assert.NoError(t, err)
	_, token2, err := backend.Load(ctx, "test")
	assert.NoError(t, err)
	assert.Equal(t, token1, token2)

	// First writer succeeds.
	assert.NoError(t, backend.Store(ctx, "test", json.RawMessage(`{"key":"v2"}`), token1))

	// Second writer fails — token is stale.
	err = backend.Store(ctx, "test", json.RawMessage(`{"key":"v3"}`), token2)
	assert.IsError(t, err, metadatadb.ErrInvalidToken)

	// Reload and retry succeeds.
	_, token3, err := backend.Load(ctx, "test")
	assert.NoError(t, err)
	assert.NoError(t, backend.Store(ctx, "test", json.RawMessage(`{"key":"v3"}`), token3))
}
