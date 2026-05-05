package git //nolint:testpackage // white-box testing required for clock and retention injection

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
)

func newTestRepoCounts(t *testing.T, now func() time.Time) *RepoCounts {
	t.Helper()
	ctx := logging.ContextWithLogger(context.Background(), slog.Default())
	store := metadatadb.New(ctx, metadatadb.NewMemoryBackend())
	rc := NewRepoCounts(store.Namespace("git"))
	if now != nil {
		rc.now = now
	}
	return rc
}

func TestRepoCountsNilSafe(t *testing.T) {
	var rc *RepoCounts
	rc.IncrementClone("https://github.com/foo/bar")
	assert.Equal(t, 0, rc.Reap())
	assert.Zero(t, len(rc.TopRepos(0, 0)))
	assert.Zero(t, NewRepoCounts(nil))
}

func TestRepoCountsReapEmpty(t *testing.T) {
	rc := newTestRepoCounts(t, nil)
	assert.Equal(t, 0, rc.Reap(), "reap on an empty namespace should report zero deletions")
}

func TestRepoCountsIncrementAndAggregate(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc := newTestRepoCounts(t, func() time.Time { return clock })

	for range 5 {
		rc.IncrementClone("https://github.com/foo/popular")
	}
	for range 2 {
		rc.IncrementClone("https://github.com/foo/quiet")
	}
	// Bump the popular repo on a previous day too.
	clock = clock.AddDate(0, 0, -1)
	for range 3 {
		rc.IncrementClone("https://github.com/foo/popular")
	}
	clock = clock.AddDate(0, 0, 1)

	top := rc.TopRepos(0, 0)
	assert.Equal(t, []RepoCount{
		{Repo: "https://github.com/foo/popular", Count: 8},
		{Repo: "https://github.com/foo/quiet", Count: 2},
	}, top)
}

func TestRepoCountsWindowFilter(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc := newTestRepoCounts(t, func() time.Time { return clock })

	// 10 days ago: only "old" repo gets hits.
	clock = time.Date(2026, 4, 25, 12, 0, 0, 0, time.UTC)
	for range 4 {
		rc.IncrementClone("https://github.com/foo/old")
	}
	// Today: only "new" repo gets hits.
	clock = time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc.IncrementClone("https://github.com/foo/new")

	all := rc.TopRepos(0, 0)
	assert.Equal(t, 2, len(all), "no window includes both repos")

	last7 := rc.TopRepos(7, 0)
	assert.Equal(t, []RepoCount{{Repo: "https://github.com/foo/new", Count: 1}}, last7)
}

func TestRepoCountsLimit(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc := newTestRepoCounts(t, func() time.Time { return clock })

	for i, repo := range []string{"alpha", "bravo", "charlie", "delta"} {
		for j := 0; j <= i; j++ {
			rc.IncrementClone("https://github.com/foo/" + repo)
		}
	}

	top2 := rc.TopRepos(0, 2)
	assert.Equal(t, []RepoCount{
		{Repo: "https://github.com/foo/delta", Count: 4},
		{Repo: "https://github.com/foo/charlie", Count: 3},
	}, top2)
}

func TestRepoCountsReap(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc := newTestRepoCounts(t, func() time.Time { return clock })
	rc.retentionDays = 30

	// Old entry: 100 days ago.
	clock = time.Date(2026, 1, 25, 12, 0, 0, 0, time.UTC)
	rc.IncrementClone("https://github.com/foo/ancient")
	// Recent entry: 5 days ago.
	clock = time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	rc.IncrementClone("https://github.com/foo/fresh")

	clock = time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, 1, rc.Reap(), "one stale bucket should be deleted")

	remaining := rc.TopRepos(0, 0)
	assert.Equal(t, []RepoCount{{Repo: "https://github.com/foo/fresh", Count: 1}}, remaining)
}

func TestRepoCountsReapMalformedKeys(t *testing.T) {
	clock := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	rc := newTestRepoCounts(t, func() time.Time { return clock })

	// Inject a malformed key directly via Add.
	rc.counts.Add("not-a-real-key", 7)
	rc.IncrementClone("https://github.com/foo/valid")

	assert.Equal(t, 1, rc.Reap(), "the malformed key should be deleted, the valid one preserved")
	remaining := rc.TopRepos(0, 0)
	assert.Equal(t, []RepoCount{{Repo: "https://github.com/foo/valid", Count: 1}}, remaining)
}

func TestRepoCountsKeyRoundTrip(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	url := "https://github.com/squareup/some-repo"
	k := repoCountsKey(url, now)
	repo, day, ok := splitRepoCountsKey(k)
	assert.True(t, ok)
	assert.Equal(t, url, repo)
	assert.Equal(t, time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC), day)
}

func TestRequestIsClone(t *testing.T) {
	gzipBytes := func(t *testing.T, in []byte) []byte {
		t.Helper()
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write(in)
		assert.NoError(t, err)
		assert.NoError(t, gw.Close())
		return buf.Bytes()
	}

	v1Clone := []byte("0067want abc123 multi_ack_detailed no-done side-band-64k thin-pack ofs-delta agent=git/2.40\n0009done\n0000")
	v1Fetch := []byte("0067want abc123 multi_ack_detailed no-done side-band-64k thin-pack ofs-delta agent=git/2.40\n0032have def4560000000000000000000000000\n0009done\n0000")
	v2Clone := []byte("0014command=fetch\n0009want abc1230009done0000")
	v2Fetch := []byte("0014command=fetch\n0009want abc1230009have def4560009done0000")

	tests := []struct {
		name            string
		path            string
		method          string
		body            []byte
		contentEncoding string
		want            bool
	}{
		{name: "GETInfoRefs", path: "org/repo.git/info/refs", method: http.MethodGet, want: false},
		{name: "GETUploadPack", path: "org/repo.git/git-upload-pack", method: http.MethodGet, want: false},
		{name: "POSTUnknownPath", path: "org/repo.git/something-else", method: http.MethodPost, body: v1Clone, want: false},
		{name: "POSTLsRefs", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: []byte("0014command=ls-refs\n0000"), want: false},
		{name: "POSTV1Clone", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: v1Clone, want: true},
		{name: "POSTV1Fetch", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: v1Fetch, want: false},
		{name: "POSTV2Clone", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: v2Clone, want: true},
		{name: "POSTV2Fetch", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: v2Fetch, want: false},
		{name: "POSTEmptyBody", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: nil, want: true},
		{name: "POSTLsRefsGzipped", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: gzipBytes(t, []byte("0014command=ls-refs\n0000")), contentEncoding: "gzip", want: false},
		{name: "POSTV2CloneGzipped", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: gzipBytes(t, v2Clone), contentEncoding: "gzip", want: true},
		{name: "POSTV2FetchGzipped", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: gzipBytes(t, v2Fetch), contentEncoding: "gzip", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tt.body != nil {
				bodyReader = bytes.NewReader(tt.body)
			}
			r := httptest.NewRequest(tt.method, "/"+tt.path, bodyReader)
			if tt.contentEncoding != "" {
				r.Header.Set("Content-Encoding", tt.contentEncoding)
			}
			got, err := RequestIsClone(tt.path, r)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// The body must remain readable for downstream handlers.
			if tt.body != nil {
				replayed, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Equal(t, tt.body, replayed)
			}
		})
	}
}

// TestRequestIsCloneBoundsBodyRead guards against a regression where the
// entire body was buffered before the inspect cap was applied, allowing a
// large or hostile body to OOM the proxy.
func TestRequestIsCloneBoundsBodyRead(t *testing.T) {
	prefix := []byte("0067want abc123 multi_ack_detailed no-done side-band-64k thin-pack ofs-delta agent=git/2.40\n0009done\n0000")
	tail := bytes.Repeat([]byte("x"), 4*1024*1024) // 4MiB after the prefix
	cr := &countingReader{Reader: io.MultiReader(bytes.NewReader(prefix), bytes.NewReader(tail))}
	r := httptest.NewRequest(http.MethodPost, "/org/repo.git/git-upload-pack", cr)
	r.ContentLength = int64(len(prefix) + len(tail))

	got, err := RequestIsClone("org/repo.git/git-upload-pack", r)
	assert.NoError(t, err)
	assert.True(t, got)
	// Inspection must not have pulled the full body into memory.
	assert.True(t, cr.n <= int64(2*uploadPackBodyInspectLimit),
		"RequestIsClone read %d bytes upfront; expected <= %d", cr.n, 2*uploadPackBodyInspectLimit)

	// Downstream must still be able to consume the entire body.
	replayed, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Equal(t, len(prefix)+len(tail), len(replayed))
}

type countingReader struct {
	io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += int64(n)
	return n, err
}

// TestRequestIsCloneFetchWithManyWants guards against a regression where a
// fetch with a long list of "want" lines preceding any "have" line gets
// classified as a clone because the inspect window cuts off before the haves.
func TestRequestIsCloneFetchWithManyWants(t *testing.T) {
	var b strings.Builder
	b.WriteString("0014command=fetch\n")
	for range 500 {
		b.WriteString("0009want abc123\n")
	}
	b.WriteString("0009have def456\n")
	b.WriteString("0000")
	body := []byte(b.String())
	assert.True(t, len(body) > 1024, "body must exceed the prior inspect limit so this test is meaningful")
	r := httptest.NewRequest(http.MethodPost, "/org/repo.git/git-upload-pack", bytes.NewReader(body))
	got, err := RequestIsClone("org/repo.git/git-upload-pack", r)
	assert.NoError(t, err)
	assert.False(t, got, "fetch with many wants must still be classified as a fetch, not a clone")
}
