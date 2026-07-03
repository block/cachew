package cache_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/google/uuid"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
)

func TestValidateNamespace(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{name: "Simple", input: "git", valid: true},
		{name: "WithHyphen", input: "go-mod", valid: true},
		{name: "WithUnderscore", input: "go_mod", valid: true},
		{name: "WithNumbers", input: "v2cache", valid: true},
		{name: "UpperCase", input: "GitLFS", valid: true},
		{name: "Empty", input: "", valid: false},
		{name: "DotPrefix", input: ".metadata", valid: false},
		{name: "DotInMiddle", input: "go.mod", valid: false},
		{name: "Slash", input: "a/b", valid: false},
		{name: "Space", input: "a b", valid: false},
		{name: "HyphenPrefix", input: "-foo", valid: false},
		{name: "UnderscorePrefix", input: "_foo", valid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cache.ValidateNamespace(tt.input)
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestDefaultETagsAreUUIDsAndUnique(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	content := []byte("same content")
	etags := make([]string, 0, 2)
	for _, key := range []cache.Key{cache.NewKey("uuid-etag-1"), cache.NewKey("uuid-etag-2")} {
		w, err := c.Create(ctx, key, nil, time.Hour)
		assert.NoError(t, err)
		_, err = w.Write(content)
		assert.NoError(t, err)
		assert.NoError(t, w.Close())

		r, headers, err := c.Open(ctx, key)
		assert.NoError(t, err)
		_, err = io.Copy(io.Discard, r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())

		rawETag, err := cache.RawETagFromHeader(headers.Get(cache.ETagKey))
		assert.NoError(t, err)
		_, err = uuid.Parse(rawETag)
		assert.NoError(t, err)
		etags = append(etags, headers.Get(cache.ETagKey))
	}
	assert.NotEqual(t, etags[0], etags[1])
}

func TestCreateWithExplicitETag(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	key := cache.NewKey("explicit-etag")
	w, err := c.Create(ctx, key, nil, time.Hour, cache.WithETag("commit-sha_123"))
	assert.NoError(t, err)
	_, err = w.Write([]byte("content"))
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, headers, err := c.Open(ctx, key)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, `"commit-sha_123"`, headers.Get(cache.ETagKey))
}

func TestCreateRejectsInvalidETag(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()

	tests := []struct {
		name string
		etag string
	}{
		{name: "Empty", etag: ""},
		{name: "Quoted", etag: `"quoted"`},
		{name: "Space", etag: "has space"},
		{name: "Comma", etag: "has,comma"},
		{name: "Unicode", etag: "snowman-☃"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := c.Create(ctx, cache.NewKey("invalid-"+tt.name), nil, time.Hour, cache.WithETag(tt.etag))
			assert.Error(t, err)
		})
	}
}
