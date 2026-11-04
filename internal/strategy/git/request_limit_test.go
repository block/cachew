package git_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/strategy/git"
)

func uploadPackBody(want string, haves int) string {
	var b strings.Builder
	b.WriteString(pkt("want " + want + "\n"))
	b.WriteString("0000")
	for i := range haves {
		b.WriteString(pkt(fmt.Sprintf("have %040x\n", i+1)))
	}
	b.WriteString(pkt("done\n"))
	return b.String()
}

func (f *infoRefsFixture) post(t *testing.T, path, body string, chunked bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(f.ctx, http.MethodPost, "/git/example.test/"+path, strings.NewReader(body))
	if chunked {
		req.ContentLength = -1
	}
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	req.SetPathValue("host", "example.test")
	req.SetPathValue("path", path)
	w := httptest.NewRecorder()
	f.uploadPack.ServeHTTP(w, req)
	return w
}

func TestRequestBodyLimit(t *testing.T) {
	const limit = 1 << 20
	fixture := newInfoRefsFixtureWithConfig(t, git.Config{MaxRequestBodyMB: 1})
	setInfoRefsLsRemote(t, fixture.upstreamPath, false)
	head := runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "HEAD")
	oversized := uploadPackBody(head, limit/50)
	assert.True(t, len(oversized) > limit)

	tests := []struct {
		name    string
		path    string
		body    string
		chunked bool
		status  int
	}{
		{"UploadPackUnderLimit", "org/repo/git-upload-pack", uploadPackBody(head, 15000), true, http.StatusOK},
		{"UploadPackContentLength", "org/repo/git-upload-pack", oversized, false, http.StatusRequestEntityTooLarge},
		{"UploadPackChunked", "org/repo/git-upload-pack", oversized, true, http.StatusRequestEntityTooLarge},
		{"UploadPackChunkedWhileCloning", "org/other/git-upload-pack", oversized, true, http.StatusRequestEntityTooLarge},
		{"EnsureRefsChunked", "org/repo/ensure-refs", `{"commits":["` + strings.Repeat("a", limit) + `"]}`, true,
			http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := fixture.transport.hits.Load()
			w := fixture.post(t, tt.path, tt.body, tt.chunked)
			assert.Equal(t, tt.status, w.Code, w.Body.String())
			assert.Equal(t, before, fixture.transport.hits.Load())
		})
	}
}
