package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractPathPrefix(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"", ""},
		{"/", ""},
		{"/git/github.com/org/repo/info/refs", "git"},
		{"/gomod/proxy.golang.org/x/mod/@latest", "gomod"},
		{"/hermit/packages/go-1.22.0.tar.gz", "hermit"},
		{"/api/v1/object/brew/some-key", "api/object"},
		{"/api/v1/stats", "api/stats"},
		{"/api/v1/namespaces", "api/namespaces"},
		{"/api/object/ns/key", "api/object"},
		{"/api", "api"},
		{"/api/", "api"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, extractPathPrefix(tt.path))
		})
	}
}
