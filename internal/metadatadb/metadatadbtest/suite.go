package metadatadbtest

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

// Suite runs a comprehensive test suite against a metadatadb.Backend implementation.
// The factory must return n backends that share the same underlying storage.
func Suite(t *testing.T, newBackends func(t *testing.T, n int) []metadatadb.Backend) {
	one := func(t *testing.T) metadatadb.Backend { return newBackends(t, 1)[0] }

	t.Run("Scalar", func(t *testing.T) {
		testScalar(t, one(t))
	})
	t.Run("ScalarDelete", func(t *testing.T) {
		testScalarDelete(t, one(t))
	})
	t.Run("ScalarStruct", func(t *testing.T) {
		testScalarStruct(t, one(t))
	})
	t.Run("Int", func(t *testing.T) {
		testInt(t, one(t))
	})
	t.Run("IntArithmetic", func(t *testing.T) {
		testIntArithmetic(t, one(t))
	})
	t.Run("IntDivByZero", func(t *testing.T) {
		testIntDivByZero(t, one(t))
	})
	t.Run("Set", func(t *testing.T) {
		testSet(t, one(t))
	})
	t.Run("SetRemove", func(t *testing.T) {
		testSetRemove(t, one(t))
	})
	t.Run("Map", func(t *testing.T) {
		testMap(t, one(t))
	})
	t.Run("MapDelete", func(t *testing.T) {
		testMapDelete(t, one(t))
	})
	t.Run("MapStruct", func(t *testing.T) {
		testMapStruct(t, one(t))
	})
	t.Run("IntMap", func(t *testing.T) {
		testIntMap(t, one(t))
	})
	t.Run("IntMapIncr", func(t *testing.T) {
		testIntMapIncr(t, one(t))
	})
	t.Run("List", func(t *testing.T) {
		testList(t, one(t))
	})
	t.Run("NamespaceIsolation", func(t *testing.T) {
		testNamespaceIsolation(t, one(t))
	})
	t.Run("FlushPersists", func(t *testing.T) {
		testFlushPersists(t, one(t))
	})
	t.Run("WritePersists", func(t *testing.T) {
		backends := newBackends(t, 2)
		testWritePersists(t, backends[0], backends[1])
	})
	t.Run("FlushRoundTrip", func(t *testing.T) {
		testFlushRoundTrip(t, one(t))
	})
	t.Run("TwoStoresSync", func(t *testing.T) {
		testTwoStoresSync(t, one(t))
	})
	t.Run("TwoBackendsConcurrentOps", func(t *testing.T) {
		backends := newBackends(t, 2)
		testTwoBackendsConcurrentOps(t, backends[0], backends[1])
	})
}

func testContext() context.Context {
	return logging.ContextWithLogger(context.Background(), slog.Default())
}

func newStore(t *testing.T, backend metadatadb.Backend) *metadatadb.Store {
	t.Helper()
	s := metadatadb.New(testContext(), backend)
	t.Cleanup(func() { assert.NoError(t, s.Close(testContext())) })
	return s
}

func namespace(t *testing.T, s *metadatadb.Store, name string) *metadatadb.Namespace {
	t.Helper()
	prefix := strings.NewReplacer("/", "-").Replace(t.Name())
	return s.Namespace(prefix + "-" + name)
}

func testScalar(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	sc := metadatadb.NewScalar[string](ns, "greeting")

	_, ok := sc.Get()
	assert.False(t, ok)

	assert.NoError(t, sc.Set("hello"))
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, "hello", v)

	assert.NoError(t, sc.Set("world"))
	v, ok = sc.Get()
	assert.True(t, ok)
	assert.Equal(t, "world", v)
}

func testScalarDelete(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	sc := metadatadb.NewScalar[int](ns, "counter")

	assert.NoError(t, sc.Set(42))
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, v)

	assert.NoError(t, sc.Delete())
	_, ok = sc.Get()
	assert.False(t, ok)
}

type testConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func testScalarStruct(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	sc := metadatadb.NewScalar[testConfig](ns, "config")

	assert.NoError(t, sc.Set(testConfig{Host: "localhost", Port: 8080}))
	v, ok := sc.Get()
	assert.True(t, ok)
	assert.Equal(t, testConfig{Host: "localhost", Port: 8080}, v)
}

func testInt(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	i := metadatadb.NewInt(ns, "counter")

	assert.Equal(t, int64(0), i.Get())

	assert.NoError(t, i.Set(10))
	assert.Equal(t, int64(10), i.Get())

	assert.NoError(t, i.Add(5))
	assert.Equal(t, int64(15), i.Get())
}

func testIntArithmetic(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	i := metadatadb.NewInt(ns, "val")

	assert.NoError(t, i.Set(100))
	assert.NoError(t, i.Mul(3))
	assert.Equal(t, int64(300), i.Get())

	assert.NoError(t, i.Div(4))
	assert.Equal(t, int64(75), i.Get())

	assert.NoError(t, i.Add(-25))
	assert.Equal(t, int64(50), i.Get())
}

func testIntDivByZero(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	i := metadatadb.NewInt(ns, "val")

	assert.NoError(t, i.Set(42))
	assert.NoError(t, i.Div(0))
	assert.Equal(t, int64(42), i.Get())
}

func testSet(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	set := metadatadb.NewSet[string](ns, "repos")

	assert.Equal(t, []string(nil), set.Members())
	assert.False(t, set.Contains("a"))

	assert.NoError(t, set.Add("a"))
	assert.NoError(t, set.Add("b"))
	assert.NoError(t, set.Add("a"))
	assert.False(t, set.Contains("c"))

	assert.Equal(t, []string{"a", "b"}, set.Members())
}

func testSetRemove(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	set := metadatadb.NewSet[string](ns, "tags")

	assert.NoError(t, set.Add("x"))
	assert.NoError(t, set.Add("y"))
	assert.NoError(t, set.Remove("x"))
	assert.NoError(t, set.Remove("z"))

	assert.Equal(t, []string{"y"}, set.Members())
}

func testMap(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	m := metadatadb.NewMap[string, string](ns, "labels")

	_, ok := m.Get("a")
	assert.False(t, ok)
	assert.Equal(t, []string(nil), m.Keys())

	assert.NoError(t, m.Set("a", "alpha"))
	assert.NoError(t, m.Set("b", "beta"))

	assert.Equal(t, map[string]string{"a": "alpha", "b": "beta"}, m.Entries())
}

func testMapDelete(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	m := metadatadb.NewMap[string, int](ns, "ports")

	assert.NoError(t, m.Set("http", 80))
	assert.NoError(t, m.Set("https", 443))
	assert.NoError(t, m.Delete("http"))

	assert.Equal(t, map[string]int{"https": 443}, m.Entries())
}

func testMapStruct(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	m := metadatadb.NewMap[string, testConfig](ns, "services")

	assert.NoError(t, m.Set("web", testConfig{Host: "web.local", Port: 8080}))

	v, ok := m.Get("web")
	assert.True(t, ok)
	assert.Equal(t, testConfig{Host: "web.local", Port: 8080}, v)
}

func testIntMap(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	m := metadatadb.NewIntMap[string](ns, "clones")

	assert.Equal(t, int64(0), m.Get("repo-a"))

	assert.NoError(t, m.Set("repo-a", 10))
	assert.NoError(t, m.Set("repo-b", 20))

	assert.Equal(t, map[string]int64{"repo-a": 10, "repo-b": 20}, m.Entries())

	assert.NoError(t, m.Delete("repo-a"))
	assert.Equal(t, map[string]int64{"repo-b": 20}, m.Entries())
}

func testIntMapIncr(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	m := metadatadb.NewIntMap[string](ns, "histogram")

	assert.NoError(t, m.Add("repo-a", 1))
	assert.NoError(t, m.Add("repo-a", 1))
	assert.NoError(t, m.Add("repo-b", 5))
	assert.NoError(t, m.Add("repo-a", 3))

	assert.Equal(t, map[string]int64{"repo-a": 5, "repo-b": 5}, m.Entries())
}

func testList(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns := namespace(t, s, "test")
	l := metadatadb.NewList[string](ns, "log")

	assert.Equal(t, 0, l.Len())
	assert.Equal(t, []string(nil), l.Entries())

	assert.NoError(t, l.Append("first"))
	assert.NoError(t, l.Append("second"))
	assert.NoError(t, l.Append("third"))

	assert.Equal(t, 3, l.Len())
	assert.Equal(t, []string{"first", "second", "third"}, l.Entries())
}

func testNamespaceIsolation(t *testing.T, backend metadatadb.Backend) {
	s := newStore(t, backend)
	ns1 := namespace(t, s, "ns1")
	ns2 := namespace(t, s, "ns2")

	sc1 := metadatadb.NewScalar[string](ns1, "key")
	sc2 := metadatadb.NewScalar[string](ns2, "key")

	assert.NoError(t, sc1.Set("from-ns1"))
	assert.NoError(t, sc2.Set("from-ns2"))

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
	ns := namespace(t, s, "test")

	sc := metadatadb.NewScalar[string](ns, "key")
	assert.NoError(t, sc.Set("value"))
	assert.NoError(t, ns.Flush(ctx))

	// Create a new store against the same backend — should see the value.
	s2 := newStore(t, backend)
	ns2 := namespace(t, s2, "test")
	sc2 := metadatadb.NewScalar[string](ns2, "key")

	// Flush to load remote state.
	assert.NoError(t, ns2.Flush(ctx))

	v, ok := sc2.Get()
	assert.True(t, ok)
	assert.Equal(t, "value", v)
}

func testWritePersists(t *testing.T, b1, b2 metadatadb.Backend) {
	ctx := testContext()
	s1 := newStore(t, b1)
	ns1 := namespace(t, s1, "test")
	assert.NoError(t, metadatadb.NewScalar[string](ns1, "key").Set("value"))

	s2 := newStore(t, b2)
	ns2 := namespace(t, s2, "test")
	assert.NoError(t, ns2.Flush(ctx))

	v, ok := metadatadb.NewScalar[string](ns2, "key").Get()
	assert.True(t, ok)
	assert.Equal(t, "value", v)
}

func testFlushRoundTrip(t *testing.T, backend metadatadb.Backend) {
	ctx := testContext()
	s := newStore(t, backend)
	ns := namespace(t, s, "test")

	assert.NoError(t, metadatadb.NewScalar[string](ns, "sc").Set("hello"))
	assert.NoError(t, metadatadb.NewScalar[testConfig](ns, "cfg").Set(testConfig{Host: "h", Port: 1}))
	i := metadatadb.NewInt(ns, "counter")
	assert.NoError(t, i.Set(10))
	assert.NoError(t, i.Add(5))
	set := metadatadb.NewSet[string](ns, "tags")
	assert.NoError(t, set.Add("a"))
	assert.NoError(t, set.Add("b"))
	m := metadatadb.NewMap[string, int](ns, "ports")
	assert.NoError(t, m.Set("http", 80))
	im := metadatadb.NewIntMap[string](ns, "clones")
	assert.NoError(t, im.Add("repo-a", 3))
	assert.NoError(t, im.Add("repo-b", 7))
	l := metadatadb.NewList[string](ns, "log")
	assert.NoError(t, l.Append("entry1"))
	assert.NoError(t, l.Append("entry2"))

	assert.NoError(t, ns.Flush(ctx))

	// Fresh store, same backend.
	s2 := newStore(t, backend)
	ns2 := namespace(t, s2, "test")
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

	ns1 := namespace(t, s1, "shared")
	ns2 := namespace(t, s2, "shared")

	assert.NoError(t, metadatadb.NewScalar[string](ns1, "owner").Set("store1"))
	assert.NoError(t, metadatadb.NewInt(ns1, "counter").Add(10))
	assert.NoError(t, ns1.Flush(ctx))

	// Store 2 loads.
	assert.NoError(t, ns2.Flush(ctx))

	v, ok := metadatadb.NewScalar[string](ns2, "owner").Get()
	assert.True(t, ok)
	assert.Equal(t, "store1", v)
	assert.Equal(t, int64(10), metadatadb.NewInt(ns2, "counter").Get())

	assert.NoError(t, metadatadb.NewInt(ns2, "counter").Add(5))
	assert.NoError(t, ns2.Flush(ctx))

	// Store 1 loads.
	assert.NoError(t, ns1.Flush(ctx))
	assert.Equal(t, int64(15), metadatadb.NewInt(ns1, "counter").Get())
}

func testTwoBackendsConcurrentOps(t *testing.T, b1, b2 metadatadb.Backend) {
	ctx := testContext()
	s1 := metadatadb.New(ctx, b1)
	t.Cleanup(func() { assert.NoError(t, s1.Close(ctx)) })
	s2 := metadatadb.New(ctx, b2)
	t.Cleanup(func() { assert.NoError(t, s2.Close(ctx)) })

	ns1 := namespace(t, s1, "shared")
	ns2 := namespace(t, s2, "shared")

	assert.NoError(t, metadatadb.NewInt(ns1, "val").Add(10))
	assert.NoError(t, metadatadb.NewInt(ns2, "val").Add(-5))

	// Backend 1 flushes first — remote becomes 10.
	assert.NoError(t, ns1.Flush(ctx))

	// Backend 2 flushes — replays Add(-5) onto remote 10 = 5.
	assert.NoError(t, ns2.Flush(ctx))

	// Backend 1 reloads to pick up backend 2's contribution.
	assert.NoError(t, ns1.Flush(ctx))

	assert.Equal(t, int64(5), metadatadb.NewInt(ns1, "val").Get())
	assert.Equal(t, int64(5), metadatadb.NewInt(ns2, "val").Get())
}
