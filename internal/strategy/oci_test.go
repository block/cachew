package strategy_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/opencontainers/go-digest"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

const (
	ociIndexType    = "application/vnd.oci.image.index.v1+json"
	ociManifestType = "application/vnd.oci.image.manifest.v1+json"
	ociTestHost     = "docker.cachew.example.com"
)

type fakeRequest struct {
	Method string
	URI    string
}

type fakeTokenRequest struct {
	Scope    string
	Username string
}

// fakeRegistry is a minimal OCI registry. Tag manifests serve an index or a
// single manifest depending on Accept.
type fakeRegistry struct {
	server *httptest.Server
	cdn    *httptest.Server

	mu             sync.Mutex
	version        string
	blobs          map[digest.Digest][]byte
	corrupt        bool
	headStatus     int
	requireToken   bool
	requests       []fakeRequest
	cdnRequests    []fakeRequest
	authorizations []string
	tokenRequests  []fakeTokenRequest
}

func newFakeRegistry(t *testing.T, blobs ...[]byte) *fakeRegistry {
	t.Helper()
	f := &fakeRegistry{version: "v1", blobs: map[digest.Digest][]byte{}}
	for _, blob := range blobs {
		f.blobs[digest.FromBytes(blob)] = blob
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

// redirectBlobs serves blobs from a separate CDN server, as Docker Hub does.
func (f *fakeRegistry) redirectBlobs(t *testing.T) {
	t.Helper()
	f.cdn = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.cdnRequests = append(f.cdnRequests, fakeRequest{Method: r.Method, URI: r.URL.RequestURI()})
		f.authorizations = append(f.authorizations, "cdn:"+r.Header.Get("Authorization"))
		blob, ok := f.blobs[digest.Digest(r.URL.Query().Get("digest"))]
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"cdn"`)
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader(string(blob)))
	}))
	t.Cleanup(f.cdn.Close)
}

func (f *fakeRegistry) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/token" {
		username, _, _ := r.BasicAuth()
		f.tokenRequests = append(f.tokenRequests, fakeTokenRequest{Scope: r.URL.Query().Get("scope"), Username: username})
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "upstream-token", "expires_in": 300})
		return
	}
	if f.requireToken && r.Header.Get("Authorization") != "Bearer upstream-token" {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fake"`, f.server.URL))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if r.URL.Path == "/v2/" {
		return
	}
	f.requests = append(f.requests, fakeRequest{Method: r.Method, URI: r.URL.RequestURI()})
	f.authorizations = append(f.authorizations, r.Header.Get("Authorization"))
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v2/"), "/")
	segment, reference := parts[len(parts)-2], parts[len(parts)-1]
	switch segment {
	case "blobs":
		f.serveBlob(w, r, reference)
	case "manifests":
		f.serveManifest(w, r, reference)
	default:
		_, _ = fmt.Fprintf(w, `{"%s":"%s"}`, segment, reference)
	}
}

func (f *fakeRegistry) serveBlob(w http.ResponseWriter, r *http.Request, reference string) {
	blob, ok := f.blobs[digest.Digest(reference)]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if f.cdn != nil {
		http.Redirect(w, r, f.cdn.URL+"/blob?digest="+reference, http.StatusTemporaryRedirect)
		return
	}
	if f.corrupt {
		blob = []byte("corrupt")
	}
	http.ServeContent(w, r, "", time.Time{}, strings.NewReader(string(blob)))
}

func (f *fakeRegistry) serveManifest(w http.ResponseWriter, r *http.Request, reference string) {
	body, contentType := []byte(fmt.Sprintf(`{"manifest":"%s"}`, f.version)), ociManifestType
	if strings.Contains(r.Header.Get("Accept"), ociIndexType) {
		body, contentType = []byte(fmt.Sprintf(`{"index":"%s"}`, f.version)), ociIndexType
	}
	if d, err := digest.Parse(reference); err == nil {
		blob, ok := f.blobs[d]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body = blob
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
	if r.Method == http.MethodHead && f.headStatus != 0 {
		w.WriteHeader(f.headStatus)
		return
	}
	_, _ = w.Write(body)
}

// read returns a fake's field under its lock, since fills record requests
// from their own goroutines.
func read[T any](f *fakeRegistry, field *T) T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return *field
}

func (f *fakeRegistry) set(fn func(f *fakeRegistry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func newOCITestMux(t *testing.T, f *fakeRegistry, config strategy.OCIConfig) *http.ServeMux {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	config.Registry = "example.io"
	config.Upstream = f.server.URL
	config.Hosts = []string{ociTestHost}
	config.TagTTL = cmp.Or(config.TagTTL, time.Nanosecond)
	config.BlobChunkSizeMB = cmp.Or(config.BlobChunkSizeMB, 32)
	config.BlobConcurrency = cmp.Or(config.BlobConcurrency, 4)
	config.Rewrites = []strategy.OCIRewrite{{Pattern: "^([^/]+)$", Replacement: "library/$1"}}
	mux := http.NewServeMux()
	_, err = strategy.NewOCI(ctx, config, c, mux, newTestSpools(t))
	assert.NoError(t, err)
	return mux
}

func serveOCI(t *testing.T, mux http.Handler, method, target string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	r := httptest.NewRequestWithContext(ctx, method, target, nil)
	for name, values := range header {
		r.Header[name] = values
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	return w
}

func TestOCIPing(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
	}{
		{name: "PathPrefix", method: http.MethodGet, target: "/oci/example.io/v2/"},
		{name: "PathPrefixHead", method: http.MethodHead, target: "/oci/example.io/v2/"},
		{name: "Host", method: http.MethodGet, target: "http://" + ociTestHost + "/v2/"},
		{name: "HostWithPort", method: http.MethodGet, target: "http://" + ociTestHost + ":8080/v2/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t)
			mux := newOCITestMux(t, f, strategy.OCIConfig{})
			w := serveOCI(t, mux, tt.method, tt.target, nil)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "registry/2.0", w.Header().Get("Docker-Distribution-Api-Version"))
			assert.Equal(t, 0, len(read(f, &f.requests)))
		})
	}
}

func TestOCIRejectsPushes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "StartUpload", method: http.MethodPost, path: "/v2/alpine/blobs/uploads/"},
		{name: "PatchUpload", method: http.MethodPatch, path: "/v2/alpine/blobs/uploads/abc"},
		{name: "PutManifest", method: http.MethodPut, path: "/v2/alpine/manifests/latest"},
		{name: "DeleteManifest", method: http.MethodDelete, path: "/v2/alpine/manifests/latest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t)
			mux := newOCITestMux(t, f, strategy.OCIConfig{})
			for _, target := range []string{"/oci/example.io" + tt.path, "http://" + ociTestHost + tt.path} {
				w := serveOCI(t, mux, tt.method, target, nil)
				assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
				assert.Equal(t, "GET, HEAD", w.Header().Get("Allow"))
			}
			assert.Equal(t, 0, len(read(f, &f.requests)))
		})
	}
}

func TestOCIPathParsing(t *testing.T) {
	blob := []byte("layer")
	blobDigest := digest.FromBytes(blob).String()
	tests := []struct {
		name         string
		path         string
		wantStatus   int
		wantUpstream []fakeRequest
	}{
		{name: "RewrittenTag", path: "/v2/alpine/manifests/latest", wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/library/alpine/manifests/latest"}}},
		{name: "NestedRepositoryNotRewritten", path: "/v2/org/team/app/manifests/v1.2", wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/org/team/app/manifests/v1.2"}}},
		{name: "RepositoryNamedLikeEndpoint", path: "/v2/org/manifests/manifests/v1", wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/org/manifests/manifests/v1"}}},
		{name: "ContainerdNamespaceDropped", path: "/v2/library/alpine/manifests/latest?ns=example.io",
			wantStatus:   http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/library/alpine/manifests/latest"}}},
		{name: "Blob", path: "/v2/alpine/blobs/" + blobDigest, wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/library/alpine/blobs/" + blobDigest}}},
		{name: "TagList", path: "/v2/alpine/tags/list?n=10&ns=example.io", wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet, URI: "/v2/library/alpine/tags/list?n=10"}}},
		{name: "Referrers", path: "/v2/alpine/referrers/" + blobDigest + "?artifactType=x", wantStatus: http.StatusOK,
			wantUpstream: []fakeRequest{{Method: http.MethodGet,
				URI: "/v2/library/alpine/referrers/" + blobDigest + "?artifactType=x"}}},
		{name: "UppercaseRepository", path: "/v2/Alpine/manifests/latest", wantStatus: http.StatusBadRequest},
		{name: "InvalidTag", path: "/v2/alpine/manifests/-latest", wantStatus: http.StatusBadRequest},
		{name: "InvalidBlobDigest", path: "/v2/alpine/blobs/latest", wantStatus: http.StatusBadRequest},
		{name: "MalformedDigest", path: "/v2/alpine/manifests/sha256:abc", wantStatus: http.StatusBadRequest},
		{name: "UnknownEndpoint", path: "/v2/alpine/uploads/latest", wantStatus: http.StatusNotFound},
		{name: "MissingRepository", path: "/v2/manifests/latest", wantStatus: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t, blob)
			mux := newOCITestMux(t, f, strategy.OCIConfig{})
			w := serveOCI(t, mux, http.MethodGet, "/oci/example.io"+tt.path, nil)
			assert.Equal(t, tt.wantStatus, w.Code)
			assert.Equal(t, tt.wantUpstream, read(f, &f.requests))
		})
	}
}

func TestOCIBlobCaching(t *testing.T) {
	blob := []byte("layer")
	target := "/oci/example.io/v2/alpine/blobs/" + digest.FromBytes(blob).String()
	tests := []struct {
		name          string
		corrupt       bool
		wantBody      string
		wantUpstreams int
	}{
		{name: "Verified", wantBody: "layer", wantUpstreams: 1},
		{name: "DigestMismatch", corrupt: true, wantBody: "corrupt", wantUpstreams: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t, blob)
			f.corrupt = tt.corrupt
			mux := newOCITestMux(t, f, strategy.OCIConfig{})
			for range 2 {
				w := serveOCI(t, mux, http.MethodGet, target, nil)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, tt.wantBody, w.Body.String())
			}
			assert.Equal(t, tt.wantUpstreams, len(read(f, &f.requests)))
		})
	}
}

func TestOCIManifestRevalidation(t *testing.T) {
	manifest := []byte(`{"pinned":true}`)
	manifestPath := "/oci/example.io/v2/alpine/manifests/" + digest.FromBytes(manifest).String()
	tagPath := "/oci/example.io/v2/alpine/manifests/latest"
	tests := []struct {
		name       string
		target     string
		headStatus int
		changed    bool
		wantBody   string
		wantMethod []string
	}{
		{name: "TagUnchanged", target: tagPath, wantBody: `{"manifest":"v1"}`,
			wantMethod: []string{http.MethodGet, http.MethodHead}},
		{name: "TagChanged", target: tagPath, changed: true, wantBody: `{"manifest":"v2"}`,
			wantMethod: []string{http.MethodGet, http.MethodHead, http.MethodGet}},
		{name: "TagRateLimited", target: tagPath, changed: true, headStatus: http.StatusTooManyRequests,
			wantBody: `{"manifest":"v1"}`, wantMethod: []string{http.MethodGet, http.MethodHead}},
		{name: "TagUpstreamUnavailable", target: tagPath, changed: true, headStatus: http.StatusBadGateway,
			wantBody: `{"manifest":"v1"}`, wantMethod: []string{http.MethodGet, http.MethodHead}},
		{name: "DigestImmutable", target: manifestPath, changed: true, wantBody: `{"pinned":true}`,
			wantMethod: []string{http.MethodGet}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t, manifest)
			mux := newOCITestMux(t, f, strategy.OCIConfig{TagTTL: time.Nanosecond})
			serveOCI(t, mux, http.MethodGet, tt.target, nil)
			f.set(func(f *fakeRegistry) {
				f.headStatus = tt.headStatus
				if tt.changed {
					f.version = "v2"
				}
			})

			w := serveOCI(t, mux, http.MethodGet, tt.target, nil)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, tt.wantBody, w.Body.String())
			assert.Equal(t, digest.FromString(tt.wantBody).String(), w.Header().Get("Docker-Content-Digest"))
			var methods []string
			for _, req := range read(f, &f.requests) {
				methods = append(methods, req.Method)
			}
			assert.Equal(t, tt.wantMethod, methods)
		})
	}
}

func TestOCIAcceptVariance(t *testing.T) {
	f := newFakeRegistry(t)
	mux := newOCITestMux(t, f, strategy.OCIConfig{TagTTL: time.Hour})
	steps := []struct {
		name          string
		accept        []string
		wantBody      string
		wantUpstreams int
	}{
		{name: "Index", accept: []string{ociIndexType + ", " + ociManifestType}, wantBody: `{"index":"v1"}`,
			wantUpstreams: 1},
		{name: "IndexReordered", accept: []string{ociManifestType + "," + ociIndexType, ociIndexType},
			wantBody: `{"index":"v1"}`, wantUpstreams: 1},
		{name: "ManifestOnly", accept: []string{ociManifestType}, wantBody: `{"manifest":"v1"}`, wantUpstreams: 2},
		{name: "ManifestOnlyAgain", accept: []string{" " + ociManifestType + " "}, wantBody: `{"manifest":"v1"}`,
			wantUpstreams: 2},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			w := serveOCI(t, mux, http.MethodGet, "/oci/example.io/v2/alpine/manifests/latest",
				http.Header{"Accept": step.accept})
			assert.Equal(t, step.wantBody, w.Body.String())
			assert.Equal(t, step.wantUpstreams, len(read(f, &f.requests)))
		})
	}
}

func TestOCIHead(t *testing.T) {
	target := "/oci/example.io/v2/alpine/manifests/latest"
	wantDigest := digest.FromString(`{"manifest":"v1"}`).String()
	tests := []struct {
		name         string
		methods      []string
		wantUpstream []string
	}{
		{name: "MissForwardsHead", methods: []string{http.MethodHead, http.MethodHead},
			wantUpstream: []string{http.MethodHead, http.MethodHead}},
		{name: "HitServedFromCache", methods: []string{http.MethodGet, http.MethodHead},
			wantUpstream: []string{http.MethodGet}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t)
			mux := newOCITestMux(t, f, strategy.OCIConfig{TagTTL: time.Hour})
			var w *httptest.ResponseRecorder
			for _, method := range tt.methods {
				w = serveOCI(t, mux, method, target, nil)
			}
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "", w.Body.String())
			assert.Equal(t, wantDigest, w.Header().Get("Docker-Content-Digest"))
			var methods []string
			for _, req := range read(f, &f.requests) {
				methods = append(methods, req.Method)
			}
			assert.Equal(t, tt.wantUpstream, methods)
		})
	}
}

func TestOCIBlobRedirectToCDN(t *testing.T) {
	blob := []byte(strings.Repeat("x", 2<<20+1))
	blobDigest := digest.FromBytes(blob).String()
	f := newFakeRegistry(t, blob)
	f.requireToken = true
	f.redirectBlobs(t)
	mux := newOCITestMux(t, f, strategy.OCIConfig{BlobChunkSizeMB: 1})

	for range 2 {
		w := serveOCI(t, mux, http.MethodGet, "/oci/example.io/v2/alpine/blobs/"+blobDigest, nil)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.True(t, w.Body.String() == string(blob), "body differs from upstream blob")
	}
	assert.Equal(t, []fakeRequest{{Method: http.MethodGet, URI: "/v2/library/alpine/blobs/" + blobDigest}}, read(f, &f.requests))
	cdnURI := "/blob?digest=" + blobDigest
	assert.Equal(t, []fakeRequest{
		{Method: http.MethodGet, URI: cdnURI}, {Method: http.MethodGet, URI: cdnURI}, {Method: http.MethodGet, URI: cdnURI},
	}, read(f, &f.cdnRequests))
	assert.Equal(t, []string{"Bearer upstream-token", "cdn:", "cdn:", "cdn:"}, read(f, &f.authorizations))
}

func TestOCITokenAuth(t *testing.T) {
	blob := []byte("layer")
	tests := []struct {
		name     string
		username string
		password string
	}{
		{name: "Anonymous"},
		{name: "Credentials", username: "user", password: "secret"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRegistry(t, blob)
			f.requireToken = true
			mux := newOCITestMux(t, f, strategy.OCIConfig{Username: tt.username, Password: tt.password})
			clientAuth := http.Header{"Authorization": {"Bearer client-token"}}
			for _, path := range []string{
				"/v2/alpine/manifests/latest",
				"/v2/alpine/blobs/" + digest.FromBytes(blob).String(),
				"/v2/org/app/tags/list",
			} {
				w := serveOCI(t, mux, http.MethodGet, "/oci/example.io"+path, clientAuth)
				assert.Equal(t, http.StatusOK, w.Code, path)
			}
			assert.Equal(t, []fakeTokenRequest{
				{Scope: "repository:library/alpine:pull", Username: tt.username},
				{Scope: "repository:org/app:pull", Username: tt.username},
			}, read(f, &f.tokenRequests))
			assert.Equal(t, []string{"Bearer upstream-token", "Bearer upstream-token", "Bearer upstream-token"},
				read(f, &f.authorizations))
		})
	}
}
