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

func TestSnapshotHTTPEndpoint(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
	assert.NoError(t, err)
	mux := newTestMux()

	cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
		MirrorRoot: tmpDir,
	}, nil)
	_, err = git.New(ctx, git.Config{
		SnapshotInterval: 24 * time.Hour,
	}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
	assert.NoError(t, err)

	// Create a fake snapshot in the cache
	upstreamURL := "https://github.com/org/repo"
	cacheKey := cache.NewKey(upstreamURL + ".snapshot")
	snapshotData := []byte("fake snapshot data")

	headers := make(map[string][]string)
	headers["Content-Type"] = []string{"application/zstd"}
	writer, err := memCache.Create(ctx, "", cacheKey, headers, 24*time.Hour)
	assert.NoError(t, err)
	_, err = writer.Write(snapshotData)
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	handler := mux.handlers["GET /git/{host}/{path...}"]
	assert.NotZero(t, handler)

	// Test successful snapshot request
	req := httptest.NewRequest(http.MethodGet, "/git/github.com/org/repo/snapshot", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/repo/snapshot")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "application/zstd", w.Header().Get("Content-Type"))
	assert.Equal(t, snapshotData, w.Body.Bytes())

	// Test snapshot not found
	req = httptest.NewRequest(http.MethodGet, "/git/github.com/org/nonexistent/snapshot", nil)
	req = req.WithContext(ctx)
	req.SetPathValue("host", "github.com")
	req.SetPathValue("path", "org/nonexistent/snapshot")
	w = httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, 404, w.Code)
}

func TestSnapshotInterval(t *testing.T) {
	_, ctx := logging.Configure(context.Background(), logging.Config{})
	tmpDir := t.TempDir()

	tests := []struct {
		name             string
		snapshotInterval time.Duration
	}{
		{
			name:             "CustomInterval",
			snapshotInterval: 1 * time.Hour,
		},
		{
			name:             "DefaultInterval",
			snapshotInterval: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memCache, err := cache.NewMemory(ctx, cache.MemoryConfig{})
			assert.NoError(t, err)
			mux := newTestMux()

			cm := gitclone.NewManagerProvider(ctx, gitclone.Config{
				MirrorRoot: tmpDir,
			}, nil)
			s, err := git.New(ctx, git.Config{
				SnapshotInterval: tt.snapshotInterval,
			}, jobscheduler.New(ctx, jobscheduler.Config{}), memCache, mux, cm, func() (*githubapp.TokenManager, error) { return nil, nil }) //nolint:nilnil
			assert.NoError(t, err)
			assert.NotZero(t, s)
		})
	}
}
