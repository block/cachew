package git_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/git"
)

func TestBundleHTTPEndpoint(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	cloneManager := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: tmpDir,
	})

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	_, err = git.New(ctx, git.Config{
		BundleInterval: 24 * time.Hour,
	}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cloneManager)
	assert.NoError(t, err)

	// Create a fake bundle in the cache
	upstreamURL := "https://github.com/org/repo"
	cacheKey := cache.NewKey(upstreamURL + ".bundle")
	bundleData := []byte("fake bundle data")

	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/x-git-bundle"}
	writer, err := memCache.Create(ctx, cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write(bundleData)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	// Test bundle endpoint exists
	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// Test successful bundle request
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/bundle", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/bundle")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "application/x-git-bundle", w.Header().Get("Content-Type"))
	assert.Equal(t, bundleData, w.Body.Bytes())

	// Test bundle not found
	req = httptest.NewRequest(http.MethodGet, "/git/github.com/org/nonexistent/bundle", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/nonexistent/bundle")
	w = httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
}

func TestBundleInterval(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	tests := []struct {
		name           string
		bundleInterval time.Duration
		expectDefault  bool
	}{
		{
			name:           "CustomInterval",
			bundleInterval: 1 * time.Hour,
			expectDefault:  false,
		},
		{
			name:           "DefaultInterval",
			bundleInterval: 0,
			expectDefault:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
			assert.NoError(t, err)
			mux := newTestMux()

			cloneManager := gitclone.NewManagerProvider(ctx, gitclone.Config{
				MirrorRoot: tmpDir,
			})

			s, err := git.New(ctx, git.Config{
				BundleInterval: tt.bundleInterval,
			}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cloneManager)
			assert.NoError(t, err)
			assert.NotZero(t, s)

			// Strategy should be created successfully regardless of bundle interval
		})
	}
}
