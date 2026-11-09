package git_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/strategy/git"
)

func TestSpoolKeyForRequest(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		method   string
		body     string
		expected string
	}{
		{name: "InfoRefs", path: "org/repo.git/info/refs", method: http.MethodGet, expected: ""},
		{name: "UploadPackGET", path: "org/repo.git/git-upload-pack", method: http.MethodGet, expected: "upload-pack"},
		{name: "Unknown", path: "org/repo.git/something-else", method: http.MethodGet, expected: ""},
		{name: "Plain", path: "org/repo", method: http.MethodGet, expected: ""},
		{name: "UploadPackPOSTSameBody", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: "command=ls-refs\n", expected: ""},
		{name: "UploadPackPOSTDiffBody", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: "command=fetch\n", expected: ""},
	}

	// Compute expected keys for the two POST cases by running them first.
	var lsRefsKey, fetchKey string
	for i, tt := range tests {
		r := httptest.NewRequest(tt.method, "/"+tt.path, strings.NewReader(tt.body))
		key, err := git.SpoolKeyForRequest(tt.path, r)
		assert.NoError(t, err)
		switch tt.name {
		case "UploadPackPOSTSameBody":
			lsRefsKey = key
			tests[i].expected = key
		case "UploadPackPOSTDiffBody":
			fetchKey = key
			tests[i].expected = key
		}
	}
	// The two POST keys must differ (different bodies).
	assert.NotEqual(t, lsRefsKey, fetchKey)
	// Both must start with "upload-pack-" prefix.
	assert.Contains(t, lsRefsKey, "upload-pack-")
	assert.Contains(t, fetchKey, "upload-pack-")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, "/"+tt.path, strings.NewReader(tt.body))
			key, err := git.SpoolKeyForRequest(tt.path, r)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, key)
		})
	}
}
