package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

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

func fakeCheckConditionals(r *http.Request, etag string) int {
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if etag == "" || (ifMatch != "*" && ifMatch != etag) {
			return http.StatusPreconditionFailed
		}
	}
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if (ifNoneMatch == "*" && etag != "") || ifNoneMatch == etag {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				return http.StatusNotModified
			}
			return http.StatusPreconditionFailed
		}
	}
	return 0
}

func fakeETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
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
	if status := fakeCheckConditionals(r, obj.headers.Get("ETag")); status != 0 {
		w.WriteHeader(status)
		return
	}
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
	if status := fakeCheckConditionals(r, obj.headers.Get("ETag")); status != 0 {
		w.WriteHeader(status)
		return
	}
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
	headers.Set("ETag", fakeETag(body))
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

func TestSaveRestoreRoundTrip(t *testing.T) {
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
	assert.NoError(t, c.Save(ctx, key, src, []string{"."}))

	dst := filepath.Join(t.TempDir(), "out")
	hit, err := c.Restore(ctx, key, dst)
	assert.NoError(t, err)
	assert.True(t, hit)

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

func TestOpenIfNoneMatch(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()
	ctx := t.Context()

	key := client.NewKey("etag-test")
	payload := []byte("etag content")

	wc, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = wc.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, wc.Close())

	etag := fakeETag(payload)

	tests := []struct {
		name        string
		ifNoneMatch string
		wantErr     error
	}{
		{name: "Matching", ifNoneMatch: etag, wantErr: client.ErrNotModified},
		{name: "NonMatching", ifNoneMatch: `"wrong"`, wantErr: nil},
		{name: "Wildcard", ifNoneMatch: "*", wantErr: client.ErrNotModified},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, headers, err := c.Open(ctx, key, client.IfNoneMatch(tt.ifNoneMatch))
			if tt.wantErr != nil {
				assert.IsError(t, err, tt.wantErr)
				assert.NotZero(t, headers.Get("ETag"))
				assert.True(t, rc == nil)
			} else {
				assert.NoError(t, err)
				data, readErr := io.ReadAll(rc)
				assert.NoError(t, readErr)
				assert.NoError(t, rc.Close())
				assert.Equal(t, payload, data)
				_ = headers
			}
		})
	}
}

func TestStatIfNoneMatch(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()
	ctx := t.Context()

	key := client.NewKey("stat-etag")
	payload := []byte("stat content")

	wc, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = wc.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, wc.Close())

	etag := fakeETag(payload)

	headers, err := c.Stat(ctx, key, client.IfNoneMatch(etag))
	assert.IsError(t, err, client.ErrNotModified)
	assert.NotZero(t, headers.Get("ETag"))

	headers, err = c.Stat(ctx, key, client.IfNoneMatch(`"wrong"`))
	assert.NoError(t, err)
	assert.NotZero(t, headers.Get("ETag"))
}

func TestOpenIfMatch(t *testing.T) {
	srv := newFakeServer(nil)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()
	ctx := t.Context()

	key := client.NewKey("ifmatch-test")
	payload := []byte("ifmatch content")

	wc, err := c.Create(ctx, key, nil, 0)
	assert.NoError(t, err)
	_, err = wc.Write(payload)
	assert.NoError(t, err)
	assert.NoError(t, wc.Close())

	etag := fakeETag(payload)

	// Matching ETag should succeed
	rc, _, err := c.Open(ctx, key, client.IfMatch(etag))
	assert.NoError(t, err)
	assert.NoError(t, rc.Close())

	// Non-matching should fail with 412
	_, _, err = c.Open(ctx, key, client.IfMatch(`"wrong"`))
	assert.IsError(t, err, client.ErrPreconditionFailed)
}

func TestOpenRange(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/object/{namespace}/{key}", func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Range") {
		case "bytes=0-3":
			w.Header().Set("Content-Range", "bytes 0-3/10")
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(http.StatusPartialContent)
			w.Write([]byte("0123")) //nolint:errcheck
		case "bytes=50-60":
			w.Header().Set("Content-Range", "bytes */10")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			http.Error(w, "unexpected range", http.StatusBadRequest)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()
	ctx := t.Context()
	key := client.NewKey("range-test")

	rc, headers, err := c.Open(ctx, key, client.Range(0, 4))
	assert.NoError(t, err)
	data, readErr := io.ReadAll(rc)
	assert.NoError(t, readErr)
	assert.NoError(t, rc.Close())
	assert.Equal(t, "0123", string(data))
	assert.Equal(t, "bytes 0-3/10", headers.Get("Content-Range"))

	_, headers, err = c.Open(ctx, key, client.Range(50, 61))
	assert.IsError(t, err, client.ErrRangeNotSatisfiable)
	assert.Equal(t, "bytes */10", headers.Get("Content-Range"))
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

// roundTripperFunc adapts a function to http.RoundTripper for tests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestCreateClosePreservesStatusErrorOnCancelledCtx exercises the
// masked-403 race in writeCloser.Close: the server has responded with
// 403 (so wc.done holds *HTTPStatusError) but ctx is cancelled before
// Close runs. The fix prefers the typed status error over the local
// ctx-cancelled error so callers can still classify the failure as a
// permission denial.
func TestCreateClosePreservesStatusErrorOnCancelledCtx(t *testing.T) {
	// Controlled transport: signals via responseDone after the response
	// has been delivered to the client, so the test can cancel ctx with
	// a deterministic happens-before relative to wc.done being filled.
	responseDone := make(chan struct{})
	httpClient := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			// Respond 403 without reading the request body. The body is the
			// io.Pipe reader from Create; reading it would deadlock because
			// nothing writes to it until Close, and Close is blocked on
			// wc.done. The real cachew server's behaviour on auth-denial is
			// equivalent: it can write the response before/without consuming
			// the body — that's exactly what produces the broken-pipe
			// symptom in production.
			resp := &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     http.Header{},
				Request:    r,
			}
			close(responseDone)
			return resp, nil
		}),
	}
	c := client.NewWithHTTPClient("http://example.invalid", httpClient).Namespace("test")
	defer c.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	wc, err := c.Create(ctx, client.NewKey("k"), nil, 0)
	assert.NoError(t, err)

	// Wait until the transport has produced the 403 response. After this
	// point the goroutine inside Create has run io.Copy + Body.Close +
	// `wc.done <- &HTTPStatusError{403}`. Cancelling ctx now puts us in
	// exactly the state the masked-403 bug requires: ctx.Err() != nil at
	// Close time, *and* a typed status error sitting on wc.done.
	<-responseDone
	cancel()

	closeErr := wc.Close()
	var statusErr *client.HTTPStatusError
	assert.True(t, errors.As(closeErr, &statusErr),
		"Close error must contain *HTTPStatusError, got: %v", closeErr)
	assert.Equal(t, http.StatusForbidden, statusErr.StatusCode)
}

// TestCreateCloseIdempotent verifies that a second Close call (e.g. a
// deferred Close after an explicit Abort or a parallel cleanup goroutine)
// does not deadlock on the drained wc.done channel and returns the same
// error as the first.
func TestCreateCloseIdempotent(t *testing.T) {
	httpClient := &http.Client{
		Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     http.Header{},
				Request:    r,
			}, nil
		}),
	}
	c := client.NewWithHTTPClient("http://example.invalid", httpClient).Namespace("test")
	defer c.Close()

	wc, err := c.Create(t.Context(), client.NewKey("k"), nil, 0)
	assert.NoError(t, err)

	first := wc.Close()
	assert.NoError(t, first)

	// Second Close must return immediately. Run it in a goroutine with a
	// generous bound so a regression manifests as a clear timeout failure
	// rather than hanging the suite — without sync.Once the second caller
	// would block forever on <-wc.done after the channel was drained by
	// the first call.
	done := make(chan error, 1)
	go func() { done <- wc.Close() }()
	select {
	case second := <-done:
		assert.Equal(t, first, second, "second Close must return the same error as the first")
	case <-time.After(2 * time.Second):
		t.Fatal("second Close blocked; expected idempotent return")
	}
}
