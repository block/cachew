package cache_test

import (
	"io"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
)

func TestCheckIfNoneMatch(t *testing.T) {
	tests := []struct {
		name        string
		ifNoneMatch string
		etag        string
		expected    bool
	}{
		{"EmptyIfNoneMatch", "", `"abc"`, false},
		{"EmptyETag", `"abc"`, "", false},
		{"WildcardMatchesAny", "*", `"abc"`, true},
		{"WildcardEmptyETag", "*", "", false},
		{"ExactMatchStrong", `"abc"`, `"abc"`, true},
		{"NoMatch", `"abc"`, `"xyz"`, false},
		{"MultipleOneMatches", `"aaa", "bbb", "ccc"`, `"bbb"`, true},
		{"MultipleNoneMatch", `"aaa", "bbb", "ccc"`, `"zzz"`, false},
		{"WeakMatchesStrong", `W/"abc"`, `"abc"`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cache.CheckIfNoneMatch(tt.ifNoneMatch, tt.etag)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestCheckIfMatch(t *testing.T) {
	tests := []struct {
		name     string
		ifMatch  string
		etag     string
		expected bool
	}{
		{"EmptyIfMatch", "", `"abc"`, true},
		{"WildcardNonEmpty", "*", `"abc"`, true},
		{"WildcardEmptyETag", "*", "", false},
		{"ExactMatch", `"abc"`, `"abc"`, true},
		{"NoMatch", `"abc"`, `"xyz"`, false},
		{"MultipleOneMatches", `"aaa", "bbb", "ccc"`, `"bbb"`, true},
		{"WeakDoesNotMatchStrong", `W/"abc"`, `"abc"`, false},
		{"WeakDoesNotMatchWeak", `W/"abc"`, `W/"abc"`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cache.CheckIfMatch(tt.ifMatch, tt.etag)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestHashingWriter(t *testing.T) {
	t.Run("Format", func(t *testing.T) {
		hw := cache.NewHashingWriter(io.Discard)
		_, err := hw.Write([]byte("hello world"))
		assert.NoError(t, err)

		etag := hw.ETag()
		assert.True(t, len(etag) > len(`"sha256:"`), "ETag too short: %s", etag)
		assert.Equal(t, `"sha256:`, etag[:8])
		assert.Equal(t, `"`, etag[len(etag)-1:])
	})

	t.Run("Deterministic", func(t *testing.T) {
		data := []byte("deterministic content")

		hw1 := cache.NewHashingWriter(io.Discard)
		_, err := hw1.Write(data)
		assert.NoError(t, err)

		hw2 := cache.NewHashingWriter(io.Discard)
		_, err = hw2.Write(data)
		assert.NoError(t, err)

		assert.Equal(t, hw1.ETag(), hw2.ETag())
	})

	t.Run("DifferentData", func(t *testing.T) {
		hw1 := cache.NewHashingWriter(io.Discard)
		_, err := hw1.Write([]byte("data one"))
		assert.NoError(t, err)

		hw2 := cache.NewHashingWriter(io.Discard)
		_, err = hw2.Write([]byte("data two"))
		assert.NoError(t, err)

		assert.NotEqual(t, hw1.ETag(), hw2.ETag())
	})
}
