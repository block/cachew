package git_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func pkt(line string) string {
	return fmt.Sprintf("%04x%s", len(line)+4, line)
}

func (f *infoRefsFixture) requestUploadPack(t *testing.T, want string) *httptest.ResponseRecorder {
	t.Helper()
	body := pkt("want "+want+"\n") + "0000" + pkt("done\n")
	req := httptest.NewRequestWithContext(f.ctx, http.MethodPost,
		"/git/example.test/org/repo/git-upload-pack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", "org/repo/git-upload-pack")
	w := httptest.NewRecorder()
	f.uploadPack.ServeHTTP(w, req)
	return w
}

func TestUploadPackNotOurRefFetchesAndServesFromMirror(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	wantSHA := fixture.commitUpstream(t)

	w := fixture.requestUploadPack(t, wantSHA)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(0), fixture.transport.hits.Load())
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-git-upload-pack-result")
	assert.Contains(t, fixture.logs.String(), "Served from mirror after fetching object missing for 'not our ref'")
	assert.Equal(t, wantSHA, runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main"))
}

func TestUploadPackNotOurRefUnreachableUpstreamForwards(t *testing.T) {
	fixture := newInfoRefsFixture(t)
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)

	w := fixture.requestUploadPack(t, strings.Repeat("a", 40))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(1), fixture.transport.hits.Load())
	assert.Equal(t, "proxied-info-refs", w.Body.String())
	assert.Contains(t, fixture.logs.String(), "Falling back to upstream due to 'not our ref'")
}
