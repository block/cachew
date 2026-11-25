package git_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func recordBackendBitmaps(t *testing.T, upstream string) string {
	t.Helper()
	realGit, err := exec.LookPath("git")
	assert.NoError(t, err)
	dir := t.TempDir()
	log := filepath.Join(dir, "backend.log")
	script := fmt.Sprintf(`#!/bin/sh
if echo " $* " | grep -q ' ls-remote '; then
  exec %[1]q ls-remote %[2]q
fi
if [ "$1" = http-backend ]; then
  echo "bitmaps=$(%[1]q config --get pack.usebitmaps)" >> %[3]q
fi
exec %[1]q "$@"
`, realGit, upstream, log)
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return log
}

func TestUploadPackSkipsBitmapsOnlyForIncrementalFetches(t *testing.T) {
	tests := []struct {
		name      string
		protocol  string
		body      func(want, have string) string
		newWant   bool
		gzipped   bool
		backend   []string
		forwarded int32
	}{
		{
			name:     "v2 fetch with haves",
			protocol: "version=2",
			body: func(want, have string) string {
				return pkt("command=fetch\n") + "0001" + pkt("want "+want+"\n") + pkt("have "+have+"\n") + pkt("done\n") + "0000"
			},
			backend: []string{"bitmaps=false"},
		},
		{
			name: "v0 fetch with haves",
			body: func(want, have string) string {
				return pkt("want "+want+"\n") + "0000" + pkt("have "+have+"\n") + pkt("done\n")
			},
			backend: []string{"bitmaps=false"},
		},
		{
			name: "gzipped v0 fetch with haves",
			body: func(want, have string) string {
				return pkt("want "+want+"\n") + "0000" + pkt("have "+have+"\n") + pkt("done\n")
			},
			gzipped: true,
			backend: []string{"bitmaps=false"},
		},
		{
			name: "fetch replayed after not our ref",
			body: func(want, have string) string {
				return pkt("want "+want+"\n") + "0000" + pkt("have "+have+"\n") + pkt("done\n")
			},
			newWant: true,
			backend: []string{"bitmaps=false", "bitmaps=false"},
		},
		{
			name: "v0 clone",
			body: func(want, _ string) string {
				return pkt("want "+want+"\n") + "0000" + pkt("done\n")
			},
			backend: []string{"bitmaps="},
		},
		{
			name:     "v2 ls-refs",
			protocol: "version=2",
			body:     func(_, _ string) string { return pkt("command=ls-refs\n") + "0000" },
			backend:  []string{"bitmaps="},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newInfoRefsFixture(t)
			log := recordBackendBitmaps(t, fixture.upstreamPath)
			have := runInfoRefsGit(t, "-C", fixture.mirrorPath, "rev-parse", "refs/heads/main")
			want := have
			if test.newWant {
				want = fixture.commitUpstream(t)
			}
			body := []byte(test.body(want, have))
			if test.gzipped {
				var compressed bytes.Buffer
				gw := gzip.NewWriter(&compressed)
				_, err := gw.Write(body)
				assert.NoError(t, err)
				assert.NoError(t, gw.Close())
				body = compressed.Bytes()
			}
			req := httptest.NewRequestWithContext(fixture.ctx, http.MethodPost,
				"/git/example.test/org/repo/git-upload-pack", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
			if test.gzipped {
				req.Header.Set("Content-Encoding", "gzip")
			}
			if test.protocol != "" {
				req.Header.Set("Git-Protocol", test.protocol)
			}
			req.SetPathValue("host", "example.test")
			req.SetPathValue("path", "org/repo/git-upload-pack")
			w := httptest.NewRecorder()
			fixture.uploadPack.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, test.forwarded, fixture.transport.hits.Load())
			recorded, err := os.ReadFile(log)
			assert.NoError(t, err)
			assert.Equal(t, test.backend, strings.Fields(string(recorded)))
		})
	}
}
