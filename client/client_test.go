package client_test

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

// fakeServer is a minimal /api/v1 backend for exercising the client over HTTP.
type fakeServer struct {
	mu      sync.Mutex
	objects map[string]fakeObject // keyed by "namespace/hex-key"
	stats   *client.Stats         // nil signals ErrStatsUnavailable
}

type fakeObject struct {
	body    []byte
	headers http.Header
}

func newFakeServer(stats *client.Stats) *httptest.Server {
	fs := &fakeServer{objects: make(map[string]fakeObject), stats: stats}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/object/{namespace}/{key}", fs.get)
	mux.HandleFunc("HEAD /api/v1/object/{namespace}/{key}", fs.stat)
	mux.HandleFunc("POST /api/v1/object/{namespace}/{key}", fs.put)
	mux.HandleFunc("DELETE /api/v1/object/{namespace}/{key}", fs.delete)
	mux.HandleFunc("GET /api/v1/namespaces", fs.namespaces)
	mux.HandleFunc("GET /api/v1/stats", fs.getStats)
	return httptest.NewServer(mux)
}

func (fs *fakeServer) key(r *http.Request) string {
	return r.PathValue("namespace") + "/" + r.PathValue("key")
}

func (fs *fakeServer) get(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	obj, ok := fs.objects[fs.key(r)]
	fs.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	maps.Copy(w.Header(), obj.headers)
	w.WriteHeader(http.StatusOK)
	w.Write(obj.body) //nolint:errcheck
}

func (fs *fakeServer) stat(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	obj, ok := fs.objects[fs.key(r)]
	fs.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	maps.Copy(w.Header(), obj.headers)
	w.WriteHeader(http.StatusOK)
}

func (fs *fakeServer) put(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	headers := make(http.Header)
	for k, v := range r.Header {
		if strings.EqualFold(k, "Content-Length") || strings.EqualFold(k, "User-Agent") ||
			strings.EqualFold(k, "Accept-Encoding") {
			continue
		}
		headers[k] = v
	}
	fs.mu.Lock()
	fs.objects[fs.key(r)] = fakeObject{body: body, headers: headers}
	fs.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (fs *fakeServer) delete(w http.ResponseWriter, r *http.Request) {
	fs.mu.Lock()
	_, ok := fs.objects[fs.key(r)]
	if ok {
		delete(fs.objects, fs.key(r))
	}
	fs.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (fs *fakeServer) namespaces(w http.ResponseWriter, _ *http.Request) {
	fs.mu.Lock()
	seen := make(map[string]struct{})
	for k := range fs.objects {
		ns := strings.SplitN(k, "/", 2)[0]
		seen[ns] = struct{}{}
	}
	fs.mu.Unlock()
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out) //nolint:errcheck
}

func (fs *fakeServer) getStats(w http.ResponseWriter, _ *http.Request) {
	if fs.stats == nil {
		http.Error(w, "stats unavailable", http.StatusNotImplemented)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(fs.stats) //nolint:errcheck
}

func TestObjectRoundTrip(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()

	ctx := t.Context()
	key := client.NewKey("hello")
	payload := []byte("hello world")

	wc, err := c.Create(ctx, key, http.Header{"Content-Type": {"text/plain"}}, 0)
	assert.NoError(t, err)
	_, err = wc.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, wc.Close())

	headers, err := c.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, "text/plain", headers.Get("Content-Type"))

	rc, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	got, err := io.ReadAll(rc)
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())
	assert.Equal(t, payload, got)
	assert.Equal(t, "text/plain", headers.Get("Content-Type"))

	assert.NoError(t, c.Delete(ctx, key))
	_, err = c.Stat(ctx, key)
	assert.Error(t, err)
	assert.True(t, isNotExist(err))
}

func isNotExist(err error) bool { return err != nil && os.IsNotExist(err) }

func TestHeaderFuncAppliesAuth(t *testing.T) {
	var seenAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenAuth = r.Header.Get("Authorization")
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := client.New(srv.URL, func() http.Header {
		return http.Header{"Authorization": {"Bearer token"}}
	})
	defer c.Close()

	_, err := c.Stat(t.Context(), client.NewKey("missing"))
	assert.Error(t, err)
	assert.Equal(t, "Bearer token", seenAuth)
}

func TestListNamespaces(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil)
	defer c.Close()
	ctx := t.Context()

	for _, ns := range []string{"alpha", "beta"} {
		nsClient := c.Namespace(client.Namespace(ns))
		wc, err := nsClient.Create(ctx, client.NewKey("x"), nil, 0)
		assert.NoError(t, err)
		_, err = wc.Write([]byte("x"))
		assert.NoError(t, err)
		assert.NoError(t, wc.Close())
	}

	namespaces, err := c.ListNamespaces(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta"}, namespaces)
}

func TestStatsUnavailable(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil)
	defer c.Close()

	_, err := c.Stats(t.Context())
	assert.Equal(t, client.ErrStatsUnavailable, err)
}

func TestStatsReturned(t *testing.T) {
	want := client.Stats{Objects: 3, Size: 1024, Capacity: 4096}
	srv := newFakeServer(&want)
	defer srv.Close()

	c := client.New(srv.URL, nil)
	defer c.Close()

	got, err := c.Stats(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSnapshotRoundTrip(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("snap")
	defer c.Close()
	ctx := t.Context()

	src := t.TempDir()
	assert.NoError(t, os.MkdirAll(filepath.Join(src, "sub"), 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("bravo"), 0o644))

	key := client.NewKey("snapshot")
	assert.NoError(t, c.Snapshot(ctx, key, src, client.SnapshotOptions{}))

	dst := filepath.Join(t.TempDir(), "out")
	assert.NoError(t, c.Restore(ctx, key, dst, client.RestoreOptions{}))

	a, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "alpha", string(a))

	b, err := os.ReadFile(filepath.Join(dst, "sub", "b.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "bravo", string(b))
}

func TestArchiveExtract(t *testing.T) {
	src := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(src, "x.txt"), []byte("x"), 0o644))
	assert.NoError(t, os.WriteFile(filepath.Join(src, "y.log"), []byte("y"), 0o644))

	var buf bytes.Buffer
	assert.NoError(t, client.Archive(t.Context(), &buf, src, []string{"."}, []string{"*.log"}, 0))

	dst := filepath.Join(t.TempDir(), "out")
	assert.NoError(t, client.Extract(t.Context(), &buf, dst, 0))

	entries, err := os.ReadDir(dst)
	assert.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.True(t, slices.Contains(names, "x.txt"))
	assert.False(t, slices.Contains(names, "y.log"))
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "RawString", input: "hello"},
		{name: "HexString", input: keyHex(client.NewKey("hello"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.ParseKey(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, client.NewKey("hello"), got)
		})
	}
}

func keyHex(k client.Key) string { return (&k).String() }

func TestParseNamespaceInvalid(t *testing.T) {
	_, err := client.ParseNamespace("_bad")
	assert.Error(t, err)
}
