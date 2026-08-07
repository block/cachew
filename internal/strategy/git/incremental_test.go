package git_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

// fakeUpstreamMarker is the distinctive body returned by the fake upstream server
// in E2E fallback scenarios so we can assert the client received it.
const fakeUpstreamMarker = "FAKE-UPSTREAM-RESPONSE"

// countingFakeUpstream returns a fake upstream httptest.Server that counts every
// request and responds with fakeUpstreamMarker, plus its hit counter.
func countingFakeUpstream(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fakeUpstreamMarker))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// redirectToFakeTransport is an http.RoundTripper that redirects every request
// to fakeBaseURL (scheme+host), counting each redirect. It is used in E2E
// tests to intercept calls that serveReadyRepo would forward to upstream.
type redirectToFakeTransport struct {
	fakeBaseURL string
	inner       http.RoundTripper
	hits        *atomic.Int32
}

func (rt *redirectToFakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.hits.Add(1)
	req = req.Clone(req.Context())
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(rt.fakeBaseURL, "http://")
	req.Host = req.URL.Host
	return rt.inner.RoundTrip(req) //nolint:wrapcheck
}

// buildCompleteUploadPackBody produces a complete v0 upload-pack negotiation
// body: want lines, flush packet, then a "done" line. Without the trailing
// done, git upload-pack waits for more negotiation and the request hangs.
func buildCompleteUploadPackBody(oids ...string) []byte {
	var b strings.Builder
	for _, oid := range oids {
		b.WriteString(pkt("want " + oid + "\n"))
	}
	b.WriteString(flushPkt)
	b.WriteString(pkt("done\n"))
	return []byte(b.String())
}

// buildV0UploadPackBodyWithNULCaps produces a real protocol-v0 first-want line
// (capabilities after a NUL) plus done, matching gitprotocol-pack(5).
func buildV0UploadPackBodyWithNULCaps(oid string) []byte {
	first := "want " + oid + "\x00multi_ack side-band-64k agent=git/2.45\n"
	return []byte(pkt(first) + flushPkt + pkt("done\n"))
}

// newLoggingServer wraps handler in a middleware that injects the ctx logger
// into each request context before dispatching. This is required because
// handleRequest calls logging.FromContext which panics if no logger is present.
func newLoggingServer(ctx context.Context, handler http.Handler) *httptest.Server {
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(logging.ContextWithLogger(r.Context(), logging.FromContext(ctx)))
		handler.ServeHTTP(w, r)
	})
	return httptest.NewServer(wrapper)
}

// e2eUpstreamHost keeps checkRefsStale hermetic: git ls-remote against
// 127.0.0.1 fails instantly with connection-refused instead of real I/O.
const e2eUpstreamHost = "127.0.0.1"

// setupE2EStrategy creates a git strategy wired to a real http.ServeMux and
// returns the strategy plus its test server. mirrorRoot must already contain the
// mirror directory tree that RepoPathFromURL resolves the upstream URL to, built
// under e2eUpstreamHost.
func setupE2EStrategy(ctx context.Context, t *testing.T, mirrorRoot string, cfg git.Config) (*git.Strategy, *httptest.Server) {
	t.Helper()
	mux := http.NewServeMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:       mirrorRoot,
		FetchInterval:    24 * time.Hour, // prevent background auto-fetch during tests
		FetchTimeout:     30 * time.Second,
		RefCheckInterval: time.Nanosecond, // expire cooldown immediately so incremental can fetch
	}, nil)
	s, err := git.New(ctx, cfg, newTestScheduler(ctx, t), nil, mux, cm,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	t.Cleanup(func() { waitForReady(t, s) })
	srv := newLoggingServer(ctx, mux)
	t.Cleanup(srv.Close)
	return s, srv
}

func postUploadPack(ctx context.Context, serverURL, repoPath string, body []byte) (*http.Response, error) {
	url := fmt.Sprintf("%s/git/%s/%s/git-upload-pack", serverURL, e2eUpstreamHost, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	client := &http.Client{Transport: http.DefaultTransport}
	return client.Do(req)
}

// buildUploadPackBody produces a minimal v0 upload-pack body with the given
// want OIDs.
func buildUploadPackBody(oids ...string) []byte {
	var b strings.Builder
	for _, oid := range oids {
		b.WriteString(pkt("want " + oid + "\n"))
	}
	b.WriteString(flushPkt)
	return []byte(b.String())
}

// buildLsRefsBody produces a v2 ls-refs body: a ref advertisement request that
// carries no wants, so it reaches the incremental decision point without engaging it.
func buildLsRefsBody() []byte {
	return []byte(pkt("command=ls-refs\n") + delimPkt + pkt("peel\n") + flushPkt)
}

// Instrument names asserted by the incremental metric helpers below.
const (
	incrementalServesMetric   = "cachew.git.incremental_serves_total"
	incrementalDurationMetric = "cachew.git.incremental_fetch_duration_seconds"
	incrementalEligibleMetric = "cachew.git.incremental_eligible_total"
	operationsMetric          = "cachew.git.operations_total"
)

// setupMetricsReader points the global OTel meter provider at a manual reader
// for the duration of the test, restoring the previous provider afterwards.
// Tests in this package never run in parallel, so swapping the global is safe.
//
// Must be called before the strategy is constructed: newGitMetrics resolves its
// meter once, at construction time, and a meter from the default no-op provider
// silently discards everything recorded through it.
func setupMetricsReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	prev := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
	return reader
}

func collectMetrics(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(ctx, &rm))
	return rm
}

// metricAggregation returns the aggregation exported under name, or nil when the
// instrument recorded nothing. The SDK omits untouched instruments.
func metricAggregation(rm metricdata.ResourceMetrics, name string) metricdata.Aggregation {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	return nil
}

// counterPoints returns the int64 counter points under name whose attributes
// match want exactly.
func counterPoints(rm metricdata.ResourceMetrics, name string, want attribute.Set) []metricdata.DataPoint[int64] {
	sum, ok := metricAggregation(rm, name).(metricdata.Sum[int64])
	if !ok {
		return nil
	}
	var out []metricdata.DataPoint[int64]
	for _, dp := range sum.DataPoints {
		if dp.Attributes.Equals(&want) {
			out = append(out, dp)
		}
	}
	return out
}

// histogramPoints returns the float64 histogram points under name whose
// attributes match want exactly.
func histogramPoints(rm metricdata.ResourceMetrics, name string, want attribute.Set) []metricdata.HistogramDataPoint[float64] {
	hist, ok := metricAggregation(rm, name).(metricdata.Histogram[float64])
	if !ok {
		return nil
	}
	var out []metricdata.HistogramDataPoint[float64]
	for _, dp := range hist.DataPoints {
		if dp.Attributes.Equals(&want) {
			out = append(out, dp)
		}
	}
	return out
}

// metricPointCount reports how many points name exported across all attribute
// sets; zero when the instrument never recorded.
func metricPointCount(rm metricdata.ResourceMetrics, name string) int {
	switch d := metricAggregation(rm, name).(type) {
	case metricdata.Sum[int64]:
		return len(d.DataPoints)
	case metricdata.Histogram[float64]:
		return len(d.DataPoints)
	default:
		return 0
	}
}

// assertIncrementalServeMetrics asserts that a single engaged request against repo
// recorded one serve, one fetch duration and one engaged=true eligibility
// point. fetched says whether real fetch work happened, in which case the
// duration must be non-zero; a local_hit can legitimately round to zero.
func assertIncrementalServeMetrics(ctx context.Context, t *testing.T, reader *sdkmetric.ManualReader, outcome, repo string, fetched bool) {
	t.Helper()
	rm := collectMetrics(ctx, t, reader)
	serveAttrs := attribute.NewSet(
		attribute.String("outcome", outcome),
		attribute.String("repository", repo),
	)

	serves := counterPoints(rm, incrementalServesMetric, serveAttrs)
	assert.Equal(t, 1, len(serves), "want one %s point for outcome %q repository %q", incrementalServesMetric, outcome, repo)
	assert.Equal(t, int64(1), serves[0].Value)

	durations := histogramPoints(rm, incrementalDurationMetric, serveAttrs)
	assert.Equal(t, 1, len(durations), "want one %s point for outcome %q repository %q", incrementalDurationMetric, outcome, repo)
	assert.Equal(t, uint64(1), durations[0].Count)
	if fetched {
		assert.True(t, durations[0].Sum > 0, "%s should time the upstream fetch, got %v", incrementalDurationMetric, durations[0].Sum)
	}

	assertIncrementalEligible(t, rm, repo, true)
}

// assertIncrementalEligible asserts exactly one eligibility point for repo with the
// given engaged value.
func assertIncrementalEligible(t *testing.T, rm metricdata.ResourceMetrics, repo string, engaged bool) {
	t.Helper()
	eligible := counterPoints(rm, incrementalEligibleMetric, attribute.NewSet(
		attribute.Bool("engaged", engaged),
		attribute.String("repository", repo),
	))
	assert.Equal(t, 1, len(eligible), "want one %s point for engaged=%v repository %q", incrementalEligibleMetric, engaged, repo)
	assert.Equal(t, int64(1), eligible[0].Value)
}

func TestIncrementalParse(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})

	oid1 := sha1OID("aabbcc")
	oid2 := sha1OID("ddeeff")

	lsRefsBody := buildLsRefsBody()
	wantRefBody := []byte(pkt("command=fetch\n") + delimPkt + pkt("want-ref refs/heads/main\n") + flushPkt)
	mixedBody := []byte(pkt("command=fetch\n") + delimPkt + pkt("want-ref refs/heads/main\n") + pkt("want "+oid1+"\n") + flushPkt)
	wantBody := buildUploadPackBody(oid1, oid2)
	gzWantBody := gzipBody(wantBody)

	tests := []struct {
		name         string
		method       string
		path         string
		body         []byte
		extraHdr     map[string]string
		wantWants    []string
		wantWantRefs bool
	}{
		{
			name:      "GETReturnsNil",
			method:    http.MethodGet,
			path:      "org/repo/info/refs",
			wantWants: nil,
		},
		{
			name:      "POSTNonUploadPackPath",
			method:    http.MethodPost,
			path:      "org/repo/info/refs",
			body:      wantBody,
			wantWants: nil,
		},
		{
			name:      "POSTWithWants",
			method:    http.MethodPost,
			path:      "org/repo/git-upload-pack",
			body:      wantBody,
			wantWants: []string{oid1, oid2},
		},
		{
			name:      "LsRefsBody",
			method:    http.MethodPost,
			path:      "org/repo/git-upload-pack",
			body:      lsRefsBody,
			wantWants: nil,
		},
		{
			name:         "WantRefBody",
			method:       http.MethodPost,
			path:         "org/repo/git-upload-pack",
			body:         wantRefBody,
			wantWants:    nil,
			wantWantRefs: true,
		},
		{
			name:         "WantRefAndWantsBody",
			method:       http.MethodPost,
			path:         "org/repo/git-upload-pack",
			body:         mixedBody,
			wantWants:    []string{oid1},
			wantWantRefs: true,
		},
		{
			name:   "GzipBody",
			method: http.MethodPost,
			path:   "org/repo/git-upload-pack",
			body:   gzWantBody,
			extraHdr: map[string]string{
				"Content-Encoding": "gzip",
			},
			wantWants: []string{oid1, oid2},
		},
		{
			name:      "GarbageBodyReturnsNil",
			method:    http.MethodPost,
			path:      "org/repo/git-upload-pack",
			body:      []byte("not a pkt-line body"),
			wantWants: nil,
		},
		{
			name:      "NilBodyReturnsNil",
			method:    http.MethodPost,
			path:      "org/repo/git-upload-pack",
			body:      nil,
			wantWants: nil,
		},
		{
			name:      "EmptyBodyReturnsNil",
			method:    http.MethodPost,
			path:      "org/repo/git-upload-pack",
			body:      []byte{},
			wantWants: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/git/github.com/"+tt.path, bytes.NewReader(tt.body))
			req = req.WithContext(ctx)
			for k, v := range tt.extraHdr {
				req.Header.Set(k, v)
			}
			wants, wantRefs := git.IncrementalParse(req, tt.path, tt.body)
			assert.Equal(t, tt.wantWants, wants)
			assert.Equal(t, tt.wantWantRefs, wantRefs)
		})
	}
}

func TestIsUploadPackPost(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "POSTUploadPack", method: http.MethodPost, path: "org/repo/git-upload-pack", want: true},
		{name: "GETUploadPack", method: http.MethodGet, path: "org/repo/git-upload-pack", want: false},
		{name: "POSTInfoRefs", method: http.MethodPost, path: "org/repo/info/refs", want: false},
		{name: "GETInfoRefs", method: http.MethodGet, path: "org/repo/info/refs", want: false},
		{name: "POSTReceivePack", method: http.MethodPost, path: "org/repo/git-receive-pack", want: false},
		{name: "POSTRepoRoot", method: http.MethodPost, path: "org/repo", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/git/github.com/"+tt.path, nil)
			assert.Equal(t, tt.want, git.IsUploadPackPost(req, tt.path))
		})
	}
}

// setupMirrorWithUpstream creates:
//   - a non-bare upstream git repo at upstreamPath with one commit
//   - a bare mirror clone at mirrorPath
//
// Returns the HEAD SHA of the initial commit.
func setupMirrorWithUpstream(t *testing.T, upstreamPath, mirrorPath string) string {
	t.Helper()
	assert.NoError(t, os.MkdirAll(upstreamPath, 0o755))
	for _, args := range [][]string{
		{"git", "-c", "init.defaultBranch=main", "-C", upstreamPath, "init"},
		{"git", "-C", upstreamPath, "config", "user.email", "test@example.com"},
		{"git", "-C", upstreamPath, "config", "user.name", "Test"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	assert.NoError(t, os.WriteFile(filepath.Join(upstreamPath, "file.txt"), []byte("initial"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", upstreamPath, "add", "."},
		{"git", "-C", upstreamPath, "commit", "-m", "initial"},
		{"git", "clone", "--mirror", upstreamPath, mirrorPath},
		// Allow serving any SHA from the mirror (required for incremental pull-through).
		{"git", "-C", mirrorPath, "config", "uploadpack.allowAnySHA1InWant", "true"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	out, err := exec.Command("git", "-C", upstreamPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// pushCommitToUpstream adds a new commit to upstreamPath and returns the new HEAD SHA.
func pushCommitToUpstream(t *testing.T, upstreamPath string) string {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(upstreamPath, "file2.txt"), []byte("delta"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", upstreamPath, "add", "."},
		{"git", "-C", upstreamPath, "commit", "-m", "delta"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	// #nosec G204 - upstreamPath is a t.TempDir() path created by this test
	out, err := exec.Command("git", "-C", upstreamPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// pushPullRefToUpstream creates a commit reachable only via refs/pull/1/head,
// leaving refs/heads/* untouched. Returns the new commit SHA. This models the
// GitHub Actions PR-CI case where actions/checkout fetches the merge/head ref
// by exact SHA while branch tips are unchanged.
func pushPullRefToUpstream(t *testing.T, upstreamPath string) string {
	t.Helper()
	assert.NoError(t, os.WriteFile(filepath.Join(upstreamPath, "pr.txt"), []byte("pull-ref"), 0o600))
	for _, args := range [][]string{
		{"git", "-C", upstreamPath, "add", "."},
		{"git", "-C", upstreamPath, "commit", "-m", "pr commit"},
	} {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	// #nosec G204 - upstreamPath is a t.TempDir() path created by this test
	out, err := exec.Command("git", "-C", upstreamPath, "rev-parse", "HEAD").Output()
	assert.NoError(t, err)
	sha := strings.TrimSpace(string(out))
	// Point a pull ref at the new commit, then reset main back so heads are unchanged.
	// #nosec G204 - upstreamPath is a t.TempDir() path and sha is the rev-parse output from it
	out, err = exec.Command("git", "-C", upstreamPath, "update-ref", "refs/pull/1/head", sha).CombinedOutput()
	assert.NoError(t, err, string(out))
	// #nosec G204 - upstreamPath is a t.TempDir() path created by this test
	out, err = exec.Command("git", "-C", upstreamPath, "update-ref", "refs/heads/main", "HEAD~1").CombinedOutput()
	assert.NoError(t, err, string(out))
	// #nosec G204 - upstreamPath is a t.TempDir() path created by this test
	out, err = exec.Command("git", "-C", upstreamPath, "checkout", "-q", "main").CombinedOutput()
	assert.NoError(t, err, string(out))
	return sha
}

// stallUpstream makes every subsequent fetch from the mirror block inside
// upload-pack until the returned release func is called, so a test can act while
// a fetch is genuinely in flight. Returns the path of the marker file the
// stalled fetch creates on entry.
func stallUpstream(t *testing.T, mirrorPath string) (started string, release func()) {
	t.Helper()
	dir := t.TempDir()
	started = filepath.Join(dir, "started")
	releasePath := filepath.Join(dir, "release")
	script := filepath.Join(dir, "stall-upload-pack.sh")
	body := "#!/bin/sh\ntouch " + started + "\nwhile [ ! -f " + releasePath + " ]; do sleep 0.02; done\n" +
		"exec git upload-pack \"$@\"\n"
	assert.NoError(t, os.WriteFile(script, []byte(body), 0o600))
	assert.NoError(t, os.Chmod(script, 0o700))
	// #nosec G204 - mirrorPath and script are t.TempDir() paths created by this test
	out, err := exec.Command("git", "-C", mirrorPath, "config", "remote.origin.uploadpack", script).CombinedOutput()
	assert.NoError(t, err, string(out))
	return started, func() {
		assert.NoError(t, os.WriteFile(releasePath, nil, 0o600))
	}
}

// waitForFile blocks until path exists, failing the test if it never appears.
func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newTestStrategyForMirror creates a Strategy whose mirror root is mirrorRoot
// and returns the underlying manager.
func newTestStrategyForMirror(ctx context.Context, t *testing.T, mirrorRoot string, cfg git.Config) (*git.Strategy, *gitclone.Manager) {
	t.Helper()
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:       mirrorRoot,
		FetchInterval:    15 * time.Minute,
		FetchTimeout:     30 * time.Second,
		RefCheckInterval: time.Nanosecond, // expire cooldown immediately so incremental can fetch
	}, nil)
	s, err := git.New(ctx, cfg, newTestScheduler(ctx, t), nil, mux, cm,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	manager, err := cm()
	assert.NoError(t, err)
	return s, manager
}

// newTestStrategyForMirrorWithRefCheck is like newTestStrategyForMirror but
// lets the caller set RefCheckInterval. Used by the cooldown test.
func newTestStrategyForMirrorWithRefCheck(ctx context.Context, t *testing.T, mirrorRoot string, cfg git.Config, refCheckInterval time.Duration) (*git.Strategy, *gitclone.Manager) {
	t.Helper()
	mux := newTestMux()
	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot:       mirrorRoot,
		FetchInterval:    15 * time.Minute,
		FetchTimeout:     30 * time.Second,
		RefCheckInterval: refCheckInterval,
	}, nil)
	s, err := git.New(ctx, cfg, newTestScheduler(ctx, t), nil, mux, cm,
		func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)
	manager, err := cm()
	assert.NoError(t, err)
	return s, manager
}

func TestEnsureWantsAvailable(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})

	// The fake upstream URL must be one the Manager's RepoPathFromURL can parse
	// as a host+path, which means it must be an http/https URL. We create the
	// mirror at the expected path, then reconfigure its remote to point at the
	// local filesystem upstream so git fetch works without network access.
	const fakeUpstreamURL = "https://example.com/org/repo"
	// Manager resolves this to <mirrorRoot>/example.com/org/repo.
	relMirrorPath := filepath.Join("example.com", "org", "repo")

	t.Run("LocalHit", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{headSHA})
		assert.NoError(t, err)
		assert.Equal(t, "local_hit", outcome)
	})

	t.Run("FetchedAfterUpstreamAdvances", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		// Set up the initial mirror with the local upstream as origin so the
		// strategy's startup fetch (in warmExistingRepos) uses the local path and
		// does not fail.
		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		// Create the strategy and wait for warm-up. The startup fetch will
		// pull the initial state (one commit).
		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		// Advance upstream. The mirror does not know about this commit yet.
		newSHA := pushCommitToUpstream(t, upstreamPath)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		// Mirror does not have newSHA, so ensureWantsAvailable should fetch it.
		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{newSHA})
		assert.NoError(t, err)
		assert.Equal(t, "fetched", outcome)
	})

	t.Run("FallbackFetchFailed", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		// Set up the mirror with a valid upstream, then point its origin at a
		// non-existent path so the incremental fetch fails. The want is absent from
		// the mirror, so the fetch path runs and fails → fallback_fetch_failed.
		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)
		out, err := exec.Command("git", "-C", mirrorPath, "remote", "set-url", "origin", "/nonexistent/path").CombinedOutput()
		assert.NoError(t, err, string(out))

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 10 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		assert.Equal(t, "fallback_fetch_failed", outcome)
		assert.Error(t, err)
	})

	t.Run("FailureCooldownSkipsRetry", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)
		out, err := exec.Command("git", "-C", mirrorPath, "remote", "set-url", "origin", "/nonexistent/path").CombinedOutput()
		assert.NoError(t, err, string(out))

		const cooldown = 500 * time.Millisecond
		s, manager := newTestStrategyForMirrorWithRefCheck(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 10 * time.Second,
		}, cooldown)
		waitForReady(t, s)

		// Let the warm-up fetch's success cooldown expire so the first call
		// actually attempts a fetch (and fails against the broken origin).
		time.Sleep(cooldown + 50*time.Millisecond)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		assert.Equal(t, "fallback_fetch_failed", outcome)
		assert.Error(t, err)

		before := repo.LastFetch()
		outcome, err = git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		assert.NoError(t, err)
		assert.Equal(t, "fallback_missing", outcome, "failed attempt must cool down synchronous retries")
		assert.Equal(t, before, repo.LastFetch(), "cooldown must skip the fetch")
	})

	t.Run("CooldownSkipsFetch", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		// Long RefCheckInterval so the warm-up fetch puts us inside the cooldown.
		s, manager := newTestStrategyForMirrorWithRefCheck(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		}, time.Hour)
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		// An absent want within the cooldown must return fallback_missing without
		// issuing a git fetch, so LastFetch must be unchanged across the call. The
		// SHA is valid hex but absent from both mirror and upstream, so even a fetch
		// that did run could not bring it in.
		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		before := repo.LastFetch()
		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		assert.NoError(t, err)
		assert.Equal(t, "fallback_missing", outcome)
		assert.Equal(t, before, repo.LastFetch(), "cooldown must skip the fetch")
	})

	t.Run("CooldownSkippedWhenFetchInFlight", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		// Long RefCheckInterval keeps NeedsFetch false once the fetch below
		// stamps lastFetchAttempt. Tests the in-flight case, not the recent one.
		s, manager := newTestStrategyForMirrorWithRefCheck(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		}, time.Hour)
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		// Advance upstream so newSHA is missing. Stall upload-pack so the fetch
		// below stays running until released.
		newSHA := pushCommitToUpstream(t, upstreamPath)
		startedMarker, releaseFetch := stallUpstream(t, mirrorPath)

		fetchDone := make(chan error, 1)
		go func() {
			fetchDone <- repo.Fetch(ctx)
		}()
		waitForFile(t, startedMarker)

		// Fetch is in flight and lastFetchAttempt is fresh: the case the old
		// guard short-circuited on without checking FetchInFlight.
		assert.False(t, repo.NeedsFetch(time.Hour), "cooldown must be active")
		assert.True(t, repo.FetchInFlight(), "fetch must be in flight")

		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		start := time.Now()
		go func() {
			outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{newSHA})
			done <- result{outcome, err}
		}()

		// Let the request reach the coalescing wait before releasing. Otherwise a
		// broken guard could return fallback_missing before this fires.
		time.Sleep(50 * time.Millisecond)
		releaseFetch()
		assert.NoError(t, <-fetchDone, "background fetch must succeed")

		res := <-done
		elapsed := time.Since(start)
		assert.NoError(t, res.err)
		assert.Equal(t, "fetched", res.outcome,
			"a request arriving while a fetch is in flight must wait for it instead of falling back")
		assert.True(t, elapsed >= 50*time.Millisecond,
			"outcome must come from waiting on the in-flight fetch, not an instant fallback (elapsed %v)", elapsed)
	})

	t.Run("PullRefOnlyFetches", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		// Advance upstream under refs/pull/1/head only, leaving heads unchanged,
		// so a heads-only staleness check would report "fresh". The cooldown path
		// (not that check) must still fetch the PR commit.
		pullSHA := pushPullRefToUpstream(t, upstreamPath)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{pullSHA})
		assert.NoError(t, err)
		assert.Equal(t, "fetched", outcome, "want reachable only via refs/pull/* must be fetched")
		assert.True(t, repo.HasCommit(ctx, pullSHA), "mirror should contain the pull-ref commit")
	})

	t.Run("VerifiedFetchAfterCoalesce", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		// Advance upstream after warm-up so the mirror lacks newSHA.
		newSHA := pushCommitToUpstream(t, upstreamPath)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		// Hold the fetch semaphore with a non-fetch holder so the first
		// doFetchReporting coalesces (fetched=false). Releasing it lets the
		// coalescing call return, and the verified-fetch second attempt then
		// acquires the semaphore and runs a real git fetch.
		release := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				<-release
				return nil
			})
		}()
		time.Sleep(20 * time.Millisecond)

		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{newSHA})
			done <- result{outcome, err}
		}()

		close(release)
		assert.NoError(t, <-holderDone)
		res := <-done
		assert.NoError(t, res.err)
		assert.Equal(t, "fetched", res.outcome, "verified fetch should bring in the missing want")
		assert.True(t, repo.HasCommit(ctx, newSHA), "mirror should contain newSHA after verified fetch")
	})

	t.Run("ClientGonePrecancelled", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		newSHA := pushCommitToUpstream(t, upstreamPath)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		reqCtx, cancel := context.WithCancel(ctx)
		cancel()
		outcome, err := git.EnsureWantsAvailable(reqCtx, s, repo, []string{newSHA})
		assert.Equal(t, "client_gone", outcome)
		assert.Error(t, err)
	})

	t.Run("FallbackLocalError", func(t *testing.T) {
		tmpDir := t.TempDir()
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		// Not a git repo, so cat-file fails with a live context.
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		assert.Equal(t, "fallback_local_error", outcome)
		assert.Error(t, err)
	})

	t.Run("ClientGoneAfterCoalesce", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		newSHA := pushCommitToUpstream(t, upstreamPath)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		// Hold the semaphore so attempt 1 coalesces. Cancel the request
		// context while waiting; after release, ensureWantsAvailable must
		// report client_gone (not fallback_fetch_failed / local_error).
		release := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				<-release
				return nil
			})
		}()
		time.Sleep(20 * time.Millisecond)

		reqCtx, cancel := context.WithCancel(ctx)
		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, err := git.EnsureWantsAvailable(reqCtx, s, repo, []string{newSHA})
			done <- result{outcome, err}
		}()
		time.Sleep(20 * time.Millisecond)
		cancel()
		close(release)
		assert.NoError(t, <-holderDone)
		res := <-done
		assert.Equal(t, "client_gone_after_fetch", res.outcome)
		assert.Error(t, res.err)
	})

	t.Run("VerifiedFetchFails", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)
		out, err := exec.Command("git", "-C", mirrorPath, "remote", "set-url", "origin", "/nonexistent/path").CombinedOutput()
		assert.NoError(t, err, string(out))

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 10 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		release := make(chan struct{})
		acquired := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				close(acquired)
				<-release
				return nil
			})
		}()
		<-acquired

		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
			done <- result{outcome, err}
		}()
		close(release)
		assert.NoError(t, <-holderDone)
		res := <-done
		assert.Equal(t, "fallback_fetch_failed", res.outcome)
		assert.Error(t, res.err)
	})

	t.Run("VerifiedFetchStillMissing", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		release := make(chan struct{})
		acquired := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				close(acquired)
				<-release
				return nil
			})
		}()
		<-acquired

		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
			done <- result{outcome, err}
		}()
		close(release)
		assert.NoError(t, <-holderDone)
		res := <-done
		assert.Equal(t, "fallback_missing", res.outcome)
		assert.NoError(t, res.err)
	})

	t.Run("PeerFetchFailureCooldownSkipsVerifiedFetch", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)
		// #nosec G204 - mirrorPath is a t.TempDir() path and the remote URL is a literal
		out, err := exec.Command("git", "-C", mirrorPath, "remote", "set-url", "origin", "/nonexistent/path").CombinedOutput()
		assert.NoError(t, err, string(out))

		const cooldown = time.Second
		s, manager := newTestStrategyForMirrorWithRefCheck(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 10 * time.Second,
		}, cooldown)
		waitForReady(t, s)

		// Let the warm-up fetch's cooldown expire so the coalescing attempt runs.
		time.Sleep(cooldown + 50*time.Millisecond)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		release := make(chan struct{})
		acquired := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				close(acquired)
				<-release
				return nil
			})
		}()
		<-acquired

		// Queued on the semaphore ahead of the coalescing callers, so its failing
		// fetch stamps the cooldown while they are all still waiting.
		peerDone := make(chan error, 1)
		go func() { peerDone <- repo.FetchVerified(ctx) }()
		time.Sleep(50 * time.Millisecond)

		type result struct {
			outcome string
			err     error
		}
		const waiters = 4
		done := make(chan result, waiters)
		for range waiters {
			go func() {
				outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
				done <- result{outcome, err}
			}()
		}
		time.Sleep(100 * time.Millisecond)
		close(release)
		assert.NoError(t, <-holderDone)
		assert.Error(t, <-peerDone)

		for range waiters {
			res := <-done
			assert.NoError(t, res.err)
			assert.Equal(t, "fallback_missing", res.outcome, "peer fetch failure must cool down the verified retry")
		}
	})

	t.Run("ClientGoneAfterVerifiedFetch", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 30 * time.Second,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		startedMarker, releaseFetch := stallUpstream(t, mirrorPath)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		release := make(chan struct{})
		acquired := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				close(acquired)
				<-release
				return nil
			})
		}()
		<-acquired

		reqCtx, cancel := context.WithCancel(ctx)
		type result struct {
			outcome string
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, err := git.EnsureWantsAvailable(reqCtx, s, repo, []string{absentSHA})
			done <- result{outcome, err}
		}()
		time.Sleep(50 * time.Millisecond)
		close(release)
		assert.NoError(t, <-holderDone)

		// The coalescing attempt found no fetch to join, so the verified retry is
		// the one now stalled upstream. Disconnect the client mid-fetch.
		waitForFile(t, startedMarker)
		cancel()
		releaseFetch()

		res := <-done
		assert.Equal(t, "client_gone_after_fetch", res.outcome)
		assert.Error(t, res.err)
	})

	t.Run("FetchTimeout", func(t *testing.T) {
		tmpDir := t.TempDir()
		upstreamPath := filepath.Join(tmpDir, "upstream")
		mirrorRoot := filepath.Join(tmpDir, "mirrors")
		mirrorPath := filepath.Join(mirrorRoot, relMirrorPath)
		assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

		setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

		reader := setupMetricsReader(t)
		s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
			IncrementalPullthrough:  true,
			IncrementalFetchTimeout: 50 * time.Millisecond,
		})
		waitForReady(t, s)

		repo, err := manager.GetOrCreate(ctx, fakeUpstreamURL)
		assert.NoError(t, err)

		const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		release := make(chan struct{})
		acquired := make(chan struct{})
		holderDone := make(chan error, 1)
		go func() {
			holderDone <- repo.WithFetchExclusion(ctx, func() error {
				close(acquired)
				<-release
				return nil
			})
		}()
		<-acquired
		time.Sleep(100 * time.Millisecond)

		outcome, err := git.EnsureWantsAvailable(ctx, s, repo, []string{absentSHA})
		close(release)
		assert.NoError(t, <-holderDone)
		assert.Equal(t, "fallback_fetch_failed", outcome)
		assert.Error(t, err)

		// The wait for the semaphore timed out, so this request never issued a
		// fetch of its own; counting it as a failed fetch would blame upstream for
		// contention and inflate the fetch error rate.
		errored := counterPoints(collectMetrics(ctx, t, reader), operationsMetric, attribute.NewSet(
			attribute.String("operation", "fetch"),
			attribute.String("status", "error"),
			attribute.String("trigger", "incremental"),
		))
		assert.Equal(t, 0, len(errored), "coalesce wait that never fetched must not record a fetch error")
	})
}

func TestIncrementalOutcomeAfterNotOurRef(t *testing.T) {
	assert.Equal(t, "", git.IncrementalOutcomeAfterNotOurRef(""))
	assert.Equal(t, "fallback_not_our_ref", git.IncrementalOutcomeAfterNotOurRef("local_hit"))
	assert.Equal(t, "fallback_not_our_ref", git.IncrementalOutcomeAfterNotOurRef("fetched"))
}

func TestIncrementalUploadPackServedLocally(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	// Mirror at <mirrorRoot>/127.0.0.1/org/repo matches the upstream URL
	// https://127.0.0.1/org/repo. See e2eUpstreamHost for why the host matters.
	const host = "127.0.0.1"
	const repoPath = "org/localrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	body := buildCompleteUploadPackBody(headSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), "NAK"), "response body should contain NAK")
	assert.True(t, strings.Contains(string(respBody), "PACK"), "response body should contain PACK magic bytes")
	assert.Equal(t, int32(0), hits.Load(), "fake upstream should not be contacted when mirror is in sync")

	assertIncrementalServeMetrics(ctx, t, reader, "local_hit", host+"/"+repoPath, false)
}

func TestIncrementalUploadPackServedLocallyV0NULCaps(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/v0nulrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// Real v0 first-want carries capabilities after a NUL. Before the parser
	// fix this body was treated as a malformed want and incremental was skipped.
	body := buildV0UploadPackBodyWithNULCaps(headSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), "NAK"), "response body should contain NAK")
	assert.True(t, strings.Contains(string(respBody), "PACK"), "response body should contain PACK magic bytes")
	assert.Equal(t, int32(0), hits.Load(), "fake upstream should not be contacted for a v0 NUL-cap local hit")

	assertIncrementalServeMetrics(ctx, t, reader, "local_hit", host+"/"+repoPath, false)
}

func TestIncrementalUploadPackFetchesMissingDelta(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/deltarepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// Advance upstream after warm-up, so the mirror does not yet have newSHA.
	newSHA := pushCommitToUpstream(t, upstreamPath)

	// Sanity check: mirror does not have the new commit yet.
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	catFile := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA)
	assert.Error(t, catFile.Run(), "mirror should not have newSHA before the request")

	body := buildCompleteUploadPackBody(newSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), "NAK"), "response body should contain NAK")
	assert.True(t, strings.Contains(string(respBody), "PACK"), "response body should contain PACK magic bytes")

	// The proxy transport (hits) is only used by forwardToUpstream. The incremental
	// delta fetch goes through the mirror's origin remote (local filesystem),
	// so hits must remain zero.
	assert.Equal(t, int32(0), hits.Load(), "fake upstream proxy should not be contacted; delta fetch uses mirror's origin remote")

	// Confirm the mirror now contains the new commit, proving the delta fetch ran.
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	catFileAfter := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA)
	assert.NoError(t, catFileAfter.Run(), "mirror should contain newSHA after the incremental delta fetch")

	assertIncrementalServeMetrics(ctx, t, reader, "fetched", host+"/"+repoPath, true)
}

func TestIncrementalUploadPackFallsBackOnMissingWant(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/fallbackrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	body := buildCompleteUploadPackBody(absentSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.True(t, hits.Load() >= 1, "fake upstream should receive the forwarded POST")
	assert.True(t, strings.Contains(string(respBody), fakeUpstreamMarker),
		"client should receive the fake upstream marker body")

	assertIncrementalServeMetrics(ctx, t, reader, "fallback_missing", host+"/"+repoPath, true)
}

func TestIncrementalUploadPackFallsBackOnFetchFailed(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/fetchfailedrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 10 * time.Second,
	})
	waitForReady(t, s)

	// Point the mirror's origin at an unreachable path so the incremental fetch
	// fails. checkRefsStale uses the upstream URL (https://127.0.0.1/...) and
	// fails fast on connection-refused; RefCheckInterval is 1ns in the E2E
	// setup so the cooldown has expired and the fetch path runs.
	// #nosec G204 - mirrorPath is a t.TempDir() path and the remote URL is a literal
	out, err := exec.Command("git", "-C", mirrorPath, "remote", "set-url", "origin", "/nonexistent/path").CombinedOutput()
	assert.NoError(t, err, string(out))

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// A want the mirror does not have; the incremental fetch will fail to reach the
	// (now broken) origin, so the request must fall back to upstream.
	const absentSHA = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	body := buildCompleteUploadPackBody(absentSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.True(t, hits.Load() >= 1, "fake upstream should receive the forwarded POST when the incremental fetch fails")
	assert.True(t, strings.Contains(string(respBody), fakeUpstreamMarker),
		"client should receive the fake upstream marker body")

	assertIncrementalServeMetrics(ctx, t, reader, "fallback_fetch_failed", host+"/"+repoPath, true)
}

func TestIncrementalUploadPackConcurrentRequestsCoalesce(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/concurrentrepo"
	const numClients = 8
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// Advance upstream after warm-up, so the mirror does not yet have newSHA.
	newSHA := pushCommitToUpstream(t, upstreamPath)

	// Sanity check: mirror does not have the new commit yet.
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	catFile := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA)
	assert.Error(t, catFile.Run(), "mirror should not have newSHA before the requests")

	body := buildCompleteUploadPackBody(newSHA)

	var wg sync.WaitGroup
	errs := make([]error, numClients)
	statuses := make([]int, numClients)
	bodies := make([]string, numClients)
	for i := range numClients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, postErr := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
			if postErr != nil {
				errs[i] = postErr
				return
			}
			defer resp.Body.Close()
			b, readErr := io.ReadAll(resp.Body)
			errs[i] = readErr
			statuses[i] = resp.StatusCode
			bodies[i] = string(b)
		}(i)
	}
	wg.Wait()

	for i := range numClients {
		assert.NoError(t, errs[i], "client %d request body read", i)
		assert.Equal(t, http.StatusOK, statuses[i], "client %d status", i)
		assert.True(t, strings.Contains(bodies[i], "PACK"), "client %d response should contain PACK", i)
	}

	// All clients were served locally: the fake upstream proxy was never hit.
	assert.Equal(t, int32(0), hits.Load(), "no client should have been forwarded to upstream")

	// The mirror now contains the new commit, proving the delta fetch ran.
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	catFileAfter := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA)
	assert.NoError(t, catFileAfter.Run(), "mirror should contain newSHA after concurrent incremental fetch")

	// Exactly numClients incremental serve events were recorded, as a mix of
	// "fetched" (the client that ran the fetch) and "local_hit" (clients that
	// found the want already present from a concurrent client's fetch).
	// No fallback_* outcomes should appear.
	rm := collectMetrics(ctx, t, reader)
	fetchedAttrs := attribute.NewSet(
		attribute.String("outcome", "fetched"),
		attribute.String("repository", host+"/"+repoPath),
	)
	localHitAttrs := attribute.NewSet(
		attribute.String("outcome", "local_hit"),
		attribute.String("repository", host+"/"+repoPath),
	)
	fetchedPts := counterPoints(rm, incrementalServesMetric, fetchedAttrs)
	localHitPts := counterPoints(rm, incrementalServesMetric, localHitAttrs)
	var fetchedCount, localHitCount int64
	if len(fetchedPts) > 0 {
		fetchedCount = fetchedPts[0].Value
	}
	if len(localHitPts) > 0 {
		localHitCount = localHitPts[0].Value
	}
	assert.Equal(t, int64(numClients), fetchedCount+localHitCount,
		"all %d clients should be served via incremental (fetched=%d local_hit=%d)", numClients, fetchedCount, localHitCount)
	assert.True(t, fetchedCount >= 1, "at least one client must have triggered the fetch")
}

func TestIncrementalUploadPackServedLocallyGzipBody(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/gziprepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// gzip-encode the upload-pack body; incrementalParse must transparently
	// decompress it to extract the wants.
	body := gzipBody(buildCompleteUploadPackBody(headSHA))
	url := fmt.Sprintf("%s/git/%s/%s/git-upload-pack", cachewSrv.URL, e2eUpstreamHost, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.Header.Set("Content-Encoding", "gzip")
	client := &http.Client{Transport: http.DefaultTransport}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), "PACK"), "response body should contain PACK magic bytes")
	assert.Equal(t, int32(0), hits.Load(), "fake upstream should not be contacted when mirror is in sync")

	assertIncrementalServeMetrics(ctx, t, reader, "local_hit", host+"/"+repoPath, false)
}

func TestIncrementalUploadPackServedLocallyV2FetchBody(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/v2repo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	headSHA := setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// Build a protocol v2 command=fetch body with a want line. incrementalParse
	// must parse it and engage the incremental path.
	v2Body := buildBody(
		pkt("command=fetch\n"),
		delimPkt,
		pkt("want "+headSHA+"\n"),
		pkt("done\n"),
		flushPkt,
	)
	url := fmt.Sprintf("%s/git/%s/%s/git-upload-pack", cachewSrv.URL, e2eUpstreamHost, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(v2Body))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.Header.Set("Git-Protocol", "version=2")
	client := &http.Client{Transport: http.DefaultTransport}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), "PACK"), "response body should contain PACK magic bytes")
	assert.Equal(t, int32(0), hits.Load(), "fake upstream should not be contacted when mirror is in sync")

	assertIncrementalServeMetrics(ctx, t, reader, "local_hit", host+"/"+repoPath, false)
}

func TestIncrementalLsRefsRecordsNotEngaged(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/lsrefsrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, buildLsRefsBody())
	assert.NoError(t, err)
	defer resp.Body.Close()
	_, err = io.ReadAll(resp.Body)
	assert.NoError(t, err)

	rm := collectMetrics(ctx, t, reader)
	assertIncrementalEligible(t, rm, host+"/"+repoPath, false)
	assert.Equal(t, 0, metricPointCount(rm, incrementalServesMetric),
		"%s must only fire for engaged requests", incrementalServesMetric)
	assert.Equal(t, 0, metricPointCount(rm, incrementalDurationMetric),
		"%s must only fire for engaged requests", incrementalDurationMetric)
}

func TestIncrementalWantRefForwardedToUpstream(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/wantrefrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// A v2 want-ref body asks the mirror to resolve refs/heads/main by name.
	// The mirror holds that ref, so this would be a local hit if cachew tried;
	// it must be forwarded instead.
	wantRefBody := []byte(pkt("command=fetch\n") + delimPkt + pkt("want-ref refs/heads/main\n") + flushPkt)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, wantRefBody)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.True(t, hits.Load() >= 1, "fake upstream should receive the forwarded want-ref POST")
	assert.True(t, strings.Contains(string(respBody), fakeUpstreamMarker),
		"client should receive the fake upstream marker body")

	rm := collectMetrics(ctx, t, reader)
	assertIncrementalEligible(t, rm, host+"/"+repoPath, false)
	assert.Equal(t, 0, metricPointCount(rm, incrementalServesMetric),
		"%s must not fire for forwarded want-ref requests", incrementalServesMetric)
}

func TestIncrementalWantRefUnknownRefServedFromUpstream(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	t.Setenv("GIT_SSL_NO_VERIFY", "1")

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	run := func(args ...string) {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}

	workPath := filepath.Join(tmpDir, "work")
	assert.NoError(t, os.MkdirAll(workPath, 0o755))
	run("git", "-c", "init.defaultBranch=main", "-C", workPath, "init")
	run("git", "-C", workPath, "config", "user.email", "test@example.com")
	run("git", "-C", workPath, "config", "user.name", "Test")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file.txt"), []byte("initial"), 0o600))
	run("git", "-C", workPath, "add", ".")
	run("git", "-C", workPath, "commit", "-m", "initial")

	upstreamRoot := filepath.Join(tmpDir, "upstream-root")
	upstreamPath := filepath.Join(upstreamRoot, "org", "tag-repo")
	assert.NoError(t, os.MkdirAll(filepath.Dir(upstreamPath), 0o755))
	run("git", "clone", "--bare", workPath, upstreamPath)
	run("git", "-C", upstreamPath, "config", "uploadpack.allowRefInWant", "true")

	gitSrv := serveGitOverTLS(t, upstreamRoot)
	hostPort := strings.TrimPrefix(gitSrv.URL, "https://")
	upstreamURL := gitSrv.URL + "/org/tag-repo"

	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, hostPort, "org", "tag-repo")
	assert.NoError(t, os.MkdirAll(filepath.Dir(mirrorPath), 0o755))
	run("git", "clone", "--mirror", upstreamURL, mirrorPath)
	// The mirror deliberately keeps ref-in-want unadvertised (git's default);
	// upstream, configured above, is the only side that must accept want-ref.

	var hits atomic.Int32
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)
	s.SetHTTPTransport(&countingTLSTransport{
		inner: &http.Transport{TLSClientConfig: hermeticTLSConfig(gitSrv)},
		hits:  &hits,
	})

	client := &http.Client{Transport: http.DefaultTransport}

	// Push tag v9 to upstream without moving main, so mirror heads stay fresh.
	run("git", "-C", workPath, "tag", "v9")
	run("git", "-C", workPath, "push", upstreamPath, "refs/tags/v9:refs/tags/v9")

	wantRefBody := []byte(pkt("command=fetch\n") + delimPkt + pkt("want-ref refs/tags/v9\n") + pkt("done\n") + flushPkt)
	postURL := fmt.Sprintf("%s/git/%s/org/tag-repo/git-upload-pack", cachewSrv.URL, hostPort)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(wantRefBody))
	assert.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	postReq.Header.Set("Git-Protocol", "version=2")
	postResp, err := client.Do(postReq)
	assert.NoError(t, err)
	defer postResp.Body.Close()
	postRespBody, err := io.ReadAll(postResp.Body)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, postResp.StatusCode)
	assert.True(t, hits.Load() >= 1, "want-ref for a tag the mirror lacks must be forwarded to upstream")
	assert.True(t, len(postRespBody) > 0 && !strings.Contains(string(postRespBody), "unknown ref"),
		"forwarded response should come from upstream, not a local unknown-ref error; got %q", string(postRespBody[:min(80, len(postRespBody))]))
}

func TestIncrementalDisabledFallsBackViaNotOurRef(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/disabledrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  false, // the old path must be taken
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// Advance upstream, so the mirror is one commit behind.
	newSHA := pushCommitToUpstream(t, upstreamPath)

	body := buildCompleteUploadPackBody(newSHA)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, body)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.True(t, hits.Load() >= 1, "fake upstream should receive the forwarded POST when incremental is disabled")
	assert.True(t, strings.Contains(string(respBody), fakeUpstreamMarker),
		"client should receive the fake upstream marker body")

	// Mirror must NOT contain newSHA: no incremental fetch ran.
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	catFile := exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA)
	assert.Error(t, catFile.Run(), "mirror should not contain newSHA when incremental pull-through is disabled")

	rm := collectMetrics(ctx, t, reader)
	assertIncrementalEligible(t, rm, host+"/"+repoPath, true)
	assert.Equal(t, 0, metricPointCount(rm, incrementalServesMetric),
		"incremental disabled: no serve outcomes should be recorded")
}

// countingTLSTransport counts upstream round trips made through the strategy's
// reverse proxy.
type countingTLSTransport struct {
	inner http.RoundTripper
	hits  *atomic.Int32
}

func (rt *countingTLSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.hits.Add(1)
	return rt.inner.RoundTrip(req)
}

// serveGitOverTLS exposes root via git http-backend on a TLS httptest server,
// so hermetic tests can exercise paths that require ls-remote/fetch to succeed
// over HTTPS (the strategy hardcodes the https scheme for upstream URLs).
// Callers must set GIT_SSL_NO_VERIFY=1 (via t.Setenv) for git CLI operations
// against the server, and use hermeticTLSConfig(srv) for proxied requests.
func serveGitOverTLS(t *testing.T, root string) *httptest.Server {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	assert.NoError(t, err)
	handler := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env: []string{
			"GIT_PROJECT_ROOT=" + root,
			"GIT_HTTP_EXPORT_ALL=1",
			"PATH=" + os.Getenv("PATH"),
		},
	}
	srv := httptest.NewTLSServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// hermeticTLSConfig trusts srv's own leaf certificate instead of skipping
// verification, so proxied requests to a serveGitOverTLS server perform real
// certificate validation.
func hermeticTLSConfig(srv *httptest.Server) *tls.Config {
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func TestIncrementalStaleInfoRefsForwardedThenBackgroundFetch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}
	// The hermetic upstream uses a self-signed certificate; git CLI operations
	// (clone, ls-remote, background fetch) inherit this from the test process.
	t.Setenv("GIT_SSL_NO_VERIFY", "1")

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	run := func(args ...string) {
		out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
		assert.NoError(t, err, string(out))
	}
	revParse := func(dir string) string {
		out, err := exec.Command("git", "-C", dir, "rev-parse", "HEAD").Output()
		assert.NoError(t, err)
		return strings.TrimSpace(string(out))
	}

	// Work repo with one commit, bare-cloned into the served root.
	workPath := filepath.Join(tmpDir, "work")
	assert.NoError(t, os.MkdirAll(workPath, 0o755))
	run("git", "-c", "init.defaultBranch=main", "-C", workPath, "init")
	run("git", "-C", workPath, "config", "user.email", "test@example.com")
	run("git", "-C", workPath, "config", "user.name", "Test")
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file.txt"), []byte("initial content"), 0o600))
	run("git", "-C", workPath, "add", ".")
	run("git", "-C", workPath, "commit", "-m", "initial commit")
	headSHA := revParse(workPath)

	upstreamRoot := filepath.Join(tmpDir, "upstream-root")
	upstreamPath := filepath.Join(upstreamRoot, "org", "stale-repo")
	assert.NoError(t, os.MkdirAll(filepath.Dir(upstreamPath), 0o755))
	run("git", "clone", "--bare", workPath, upstreamPath)

	gitSrv := serveGitOverTLS(t, upstreamRoot)
	hostPort := strings.TrimPrefix(gitSrv.URL, "https://")
	upstreamURL := gitSrv.URL + "/org/stale-repo"

	// Mirror cloned over HTTPS so its origin matches the strategy's upstream URL.
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, hostPort, "org", "stale-repo")
	assert.NoError(t, os.MkdirAll(filepath.Dir(mirrorPath), 0o755))
	run("git", "clone", "--mirror", upstreamURL, mirrorPath)

	var hits atomic.Int32
	reader := setupMetricsReader(t)
	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)
	s.SetHTTPTransport(&countingTLSTransport{
		inner: &http.Transport{TLSClientConfig: hermeticTLSConfig(gitSrv)},
		hits:  &hits,
	})

	// SetHTTPTransport mutates http.DefaultClient, so test client requests must
	// use a dedicated transport to avoid polluting the proxy hit counter.
	client := &http.Client{Transport: http.DefaultTransport}

	// Advance upstream after warm-up so the mirror is stale.
	assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file2.txt"), []byte("delta content"), 0o600))
	run("git", "-C", workPath, "add", ".")
	run("git", "-C", workPath, "commit", "-m", "delta")
	run("git", "-C", workPath, "push", upstreamPath, "HEAD:main")
	newSHA := revParse(workPath)

	// Stale info/refs: forwarded to upstream so the client sees fresh refs,
	// and a background fetch is scheduled to catch the mirror up.
	infoRefsURL := fmt.Sprintf("%s/git/%s/org/stale-repo/info/refs?service=git-upload-pack", cachewSrv.URL, hostPort)
	infoReq, err := http.NewRequestWithContext(ctx, http.MethodGet, infoRefsURL, nil)
	assert.NoError(t, err)
	resp, err := client.Do(infoReq)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.True(t, strings.Contains(string(respBody), newSHA), "forwarded info/refs must carry the fresh upstream tip")
	assert.True(t, hits.Load() >= 1, "stale info/refs must be forwarded to upstream")

	// The background fetch triggered by the stale info/refs brings the delta in.
	deadline := time.Now().Add(30 * time.Second)
	// #nosec G204 - mirrorPath is a t.TempDir() path and newSHA is rev-parse output from the local upstream
	for exec.Command("git", "-C", mirrorPath, "cat-file", "-e", newSHA).Run() != nil {
		if time.Now().After(deadline) {
			t.Fatal("background fetch did not bring newSHA into the mirror")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// An upload-pack POST for objects the mirror has is served locally, with no
	// upstream round trip.
	before := hits.Load()
	body := buildCompleteUploadPackBody(headSHA)
	postURL := fmt.Sprintf("%s/git/%s/org/stale-repo/git-upload-pack", cachewSrv.URL, hostPort)
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(body))
	assert.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	postResp, err := client.Do(postReq)
	assert.NoError(t, err)
	defer postResp.Body.Close()
	postRespBody, err := io.ReadAll(postResp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, postResp.StatusCode)
	assert.True(t, strings.Contains(string(postRespBody), "PACK"), "response body should contain PACK magic bytes")
	assert.Equal(t, before, hits.Load(), "upload-pack for present wants must be served locally")

	assertIncrementalServeMetrics(ctx, t, reader, "local_hit", hostPort+"/org/stale-repo", false)
}

func TestIncrementalUploadPackRejectsOversizedBody(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/bigbodyrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	oversized := make([]byte, 2*git.UploadPackParseLimit+1)
	url := fmt.Sprintf("%s/git/%s/%s/git-upload-pack", cachewSrv.URL, e2eUpstreamHost, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(oversized))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	client := &http.Client{Transport: http.DefaultTransport}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func TestUploadPackOversizedBodyAcceptedWhenIncrementalDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/bigbodydisabledrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  false, // the body cap must not apply
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	oversized := make([]byte, 2*git.UploadPackParseLimit+1)
	url := fmt.Sprintf("%s/git/%s/%s/git-upload-pack", cachewSrv.URL, e2eUpstreamHost, repoPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(oversized))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	client := &http.Client{Transport: http.DefaultTransport}
	resp, err := client.Do(req)
	assert.NoError(t, err)
	defer resp.Body.Close()
	// The body is garbage, so the local backend's verdict is git's business; the
	// contract under test is only that cachew itself does not reject the size.
	assert.NotEqual(t, http.StatusRequestEntityTooLarge, resp.StatusCode,
		"the body cap must be gated on IncrementalPullthrough")
}

func TestWantRefForwardedIncrementalDisabled(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const host = "127.0.0.1"
	const repoPath = "org/wantrefdisabledrepo"
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorPath := filepath.Join(mirrorRoot, host, repoPath)
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	fakeSrv, hits := countingFakeUpstream(t)

	s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  false, // the want-ref guard must still hold
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	s.SetHTTPTransport(&redirectToFakeTransport{
		fakeBaseURL: fakeSrv.URL,
		inner:       http.DefaultTransport,
		hits:        hits,
	})

	// The mirror holds refs/heads/main, but it does not advertise ref-in-want and
	// resolving the ref name locally could serve a stale tip, so the request must
	// be forwarded even with incremental disabled.
	wantRefBody := []byte(pkt("command=fetch\n") + delimPkt + pkt("want-ref refs/heads/main\n") + flushPkt)
	resp, err := postUploadPack(ctx, cachewSrv.URL, repoPath, wantRefBody)
	assert.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.True(t, hits.Load() >= 1, "fake upstream should receive the forwarded want-ref POST")
	assert.True(t, strings.Contains(string(respBody), fakeUpstreamMarker),
		"client should receive the fake upstream marker body")
}

func TestSubmitFetchForceBypassesFetchCooldown(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const upstreamURL = "https://example.com/org/repo"
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "example.com", "org", "repo")
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	// A long cooldown leaves the warm-up fetch's timestamp inside the window,
	// which is the state a just-failed incremental fetch also leaves behind.
	const cooldown = time.Hour
	s, manager := newTestStrategyForMirrorWithRefCheck(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	}, cooldown)
	waitForReady(t, s)

	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	// Advance upstream after warm-up so convergence can only come from a fetch
	// that runs now.
	newSHA := pushCommitToUpstream(t, upstreamPath)
	assert.False(t, repo.NeedsFetch(cooldown), "warm-up fetch must leave the cooldown active")

	// The cooldown makes the unforced submission a no-op, so the mirror stays behind.
	git.SubmitFetch(s, repo)
	time.Sleep(500 * time.Millisecond)
	assert.False(t, repo.HasCommit(ctx, newSHA), "submitFetch must not fetch inside the cooldown")

	git.SubmitFetchForce(s, repo)
	deadline := time.Now().Add(30 * time.Second)
	for !repo.HasCommit(ctx, newSHA) {
		if time.Now().After(deadline) {
			t.Fatal("forced background fetch did not bring newSHA into the mirror")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestSubmitFetchForceFetchesPastSemaphoreHolder(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	const upstreamURL = "https://example.com/org/repo"
	upstreamPath := filepath.Join(tmpDir, "upstream")
	mirrorRoot := filepath.Join(tmpDir, "mirrors")
	mirrorPath := filepath.Join(mirrorRoot, "example.com", "org", "repo")
	assert.NoError(t, os.MkdirAll(mirrorPath, 0o755))

	setupMirrorWithUpstream(t, upstreamPath, mirrorPath)

	s, manager := newTestStrategyForMirror(ctx, t, mirrorRoot, git.Config{
		IncrementalPullthrough:  true,
		IncrementalFetchTimeout: 30 * time.Second,
	})
	waitForReady(t, s)

	repo, err := manager.GetOrCreate(ctx, upstreamURL)
	assert.NoError(t, err)

	// Advance upstream after warm-up so convergence can only come from the
	// recovery fetch.
	newSHA := pushCommitToUpstream(t, upstreamPath)

	// A non-fetch holder, a snapshot tar say, occupies the fetch semaphore while
	// the recovery job runs. A coalescing fetch would count the holder's work as
	// its own and leave the mirror exactly as cold as before.
	release := make(chan struct{})
	acquired := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- repo.WithFetchExclusion(ctx, func() error {
			close(acquired)
			<-release
			return nil
		})
	}()
	<-acquired

	git.SubmitFetchForce(s, repo)
	time.Sleep(time.Second)
	assert.False(t, repo.HasCommit(ctx, newSHA), "the holder blocks the fetch, so the mirror is still behind")

	close(release)
	assert.NoError(t, <-holderDone)

	deadline := time.Now().Add(30 * time.Second)
	for !repo.HasCommit(ctx, newSHA) {
		if time.Now().After(deadline) {
			t.Fatal("forced fetch did not bring newSHA into the mirror")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestShallowCloneDefaultProtocolSucceeds(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found in PATH")
	}

	for _, incremental := range []bool{true, false} {
		t.Run(fmt.Sprintf("IncrementalPullthrough=%v", incremental), func(t *testing.T) {
			t.Setenv("GIT_SSL_NO_VERIFY", "1")
			_, ctx := logging.Configure(context.Background(), logging.Config{})
			tmpDir := t.TempDir()

			run := func(args ...string) {
				out, err := exec.Command(args[0], args[1:]...).CombinedOutput()
				assert.NoError(t, err, string(out))
			}

			// Several commits so --depth 1 is genuinely shallower than the repo.
			workPath := filepath.Join(tmpDir, "work")
			assert.NoError(t, os.MkdirAll(workPath, 0o755))
			run("git", "-c", "init.defaultBranch=main", "-C", workPath, "init")
			run("git", "-C", workPath, "config", "user.email", "test@example.com")
			run("git", "-C", workPath, "config", "user.name", "Test")
			for i := range 5 {
				assert.NoError(t, os.WriteFile(filepath.Join(workPath, "file.txt"),
					fmt.Appendf(nil, "rev%d", i), 0o600))
				run("git", "-C", workPath, "add", "file.txt")
				run("git", "-C", workPath, "commit", "-m", fmt.Sprintf("commit%d", i))
			}

			upstreamRoot := filepath.Join(tmpDir, "upstream-root")
			upstreamPath := filepath.Join(upstreamRoot, "org", "shallow-repo")
			assert.NoError(t, os.MkdirAll(filepath.Dir(upstreamPath), 0o755))
			run("git", "clone", "--bare", workPath, upstreamPath)

			gitSrv := serveGitOverTLS(t, upstreamRoot)
			hostPort := strings.TrimPrefix(gitSrv.URL, "https://")
			upstreamURL := gitSrv.URL + "/org/shallow-repo"

			mirrorRoot := filepath.Join(tmpDir, "mirrors")
			mirrorPath := filepath.Join(mirrorRoot, hostPort, "org", "shallow-repo")
			assert.NoError(t, os.MkdirAll(filepath.Dir(mirrorPath), 0o755))
			run("git", "clone", "--mirror", upstreamURL, mirrorPath)

			var hits atomic.Int32
			s, cachewSrv := setupE2EStrategy(ctx, t, mirrorRoot, git.Config{
				IncrementalPullthrough:  incremental,
				IncrementalFetchTimeout: 30 * time.Second,
			})
			waitForReady(t, s)
			s.SetHTTPTransport(&countingTLSTransport{
				inner: &http.Transport{TLSClientConfig: hermeticTLSConfig(gitSrv)},
				hits:  &hits,
			})

			// No protocol.version override: the client negotiates v2, which is
			// the only configuration that reproduces the regression.
			cloneURL := fmt.Sprintf("%s/git/%s/org/shallow-repo", cachewSrv.URL, hostPort)
			dest := filepath.Join(tmpDir, "shallow")
			out, err := exec.Command("git", "clone", "--depth", "1", cloneURL, dest).CombinedOutput()
			assert.NoError(t, err, "shallow clone through cachew must succeed: %s", string(out))

			// A pack that parsed as intended yields a usable one-commit repo.
			depthOut, err := exec.Command("git", "-C", dest, "rev-list", "--count", "HEAD").CombinedOutput()
			assert.NoError(t, err, string(depthOut))
			assert.Equal(t, "1", strings.TrimSpace(string(depthOut)),
				"clone should be shallow with a single commit")
			fsckOut, err := exec.Command("git", "-C", dest, "fsck", "--connectivity-only").CombinedOutput()
			assert.NoError(t, err, string(fsckOut))
			revParse := func(dir string) string {
				revOut, revErr := exec.Command("git", "-C", dir, "rev-parse", "HEAD").CombinedOutput()
				assert.NoError(t, revErr, string(revOut))
				return strings.TrimSpace(string(revOut))
			}
			assert.Equal(t, revParse(workPath), revParse(dest), "clone should land on the upstream tip")

			// The mirror has every wanted object, so the clone must be served
			// locally rather than proxied upstream.
			assert.Equal(t, int32(0), hits.Load(), "shallow clone should be served from the mirror")
		})
	}
}
