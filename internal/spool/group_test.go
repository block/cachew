package spool_test

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
)

const key = "object"

func TestConcurrentRequestsShareOneFill(t *testing.T) {
	ctx, registry, _ := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.ReleaseCompleted)
	release := make(chan struct{})
	var fills atomic.Int32
	fill := func(_ context.Context, w *spool.Writer) error {
		fills.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "first ")
		<-release
		_, err := io.WriteString(w, "second")
		return err
	}

	const readers = 5
	recorders := make([]*httptest.ResponseRecorder, readers)
	results := make([]spool.Result, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i := range readers {
		recorders[i] = httptest.NewRecorder()
		wg.Go(func() {
			req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
			results[i], errs[i] = group.Serve(recorders[i], req, key, fill)
		})
	}
	waitForReaders(t, group, readers)
	close(release)
	wg.Wait()

	assert.Equal(t, int32(1), fills.Load())
	filled := 0
	for i := range readers {
		assert.NoError(t, errs[i])
		assert.Equal(t, http.StatusOK, recorders[i].Code)
		assert.Equal(t, "text/plain", recorders[i].Header().Get("Content-Type"))
		assert.Equal(t, "first second", recorders[i].Body.String())
		assert.Equal(t, int64(len("first second")), results[i].Bytes)
		if results[i].Filled {
			filled++
		}
	}
	assert.Equal(t, 1, filled)
}

func TestFillOutlivesItsRequests(t *testing.T) {
	ctx, registry, _ := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.ReleaseCompleted)
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer c.Close()
	cacheKey := cache.NewKey(key)
	release := make(chan struct{})
	fillDone := make(chan error, 1)
	fill := func(ctx context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		cw, err := c.Create(ctx, cacheKey, http.Header{}, time.Hour)
		assert.NoError(t, err)
		assert.NoError(t, w.Store(cw))
		_, _ = io.WriteString(w, "first ")
		<-release
		_, err = io.WriteString(w, "second")
		fillDone <- ctx.Err()
		return err
	}

	leaderCtx, cancelLeader := context.WithCancel(ctx)
	leaderErr := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(leaderCtx, http.MethodGet, "/object", nil)
		_, err := group.Serve(httptest.NewRecorder(), req, key, fill)
		leaderErr <- err
	}()
	waitForReaders(t, group, 1)
	followerCtx, cancelFollower := context.WithCancel(ctx)
	followerErr := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(followerCtx, http.MethodGet, "/object", nil)
		_, err := group.Serve(httptest.NewRecorder(), req, key, fill)
		followerErr <- err
	}()
	waitForReaders(t, group, 2)

	cancelLeader()
	assert.IsError(t, <-leaderErr, context.Canceled)
	cancelFollower()
	assert.IsError(t, <-followerErr, context.Canceled)
	close(release)
	assert.NoError(t, <-fillDone)
	waitFor(t, func() bool { return group.Len() == 0 })

	r, _, err := c.Open(ctx, cacheKey)
	assert.NoError(t, err)
	defer r.Close()
	body, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, "first second", string(body))
}

func TestRequestsFollowPendingCacheWrite(t *testing.T) {
	ctx, registry, dir := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.ReleaseCompleted)
	disk, err := cache.NewDisk(ctx, cache.DiskConfig{Root: t.TempDir(), MaxTTL: time.Hour})
	assert.NoError(t, err)
	defer disk.Close()
	release := make(chan struct{})
	fill := func(ctx context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		cw, err := disk.Create(ctx, cache.NewKey(key), http.Header{}, time.Hour)
		assert.NoError(t, err)
		assert.NoError(t, w.Store(cw))
		_, _ = io.WriteString(w, "first ")
		<-release
		_, err = io.WriteString(w, "second")
		return err
	}

	recorder := httptest.NewRecorder()
	served := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
		_, err := group.Serve(recorder, req, key, fill)
		served <- err
	}()
	waitFor(t, func() bool { return group.Written(key) > 0 })
	files, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
	close(release)

	assert.NoError(t, <-served)
	assert.Equal(t, "first second", recorder.Body.String())
}

func TestCacheWriteFailureDoesNotFailResponse(t *testing.T) {
	ctx, registry, _ := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.ReleaseCompleted)
	fill := func(_ context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		assert.NoError(t, w.Store(&failingCacheWriter{}))
		for range 3 {
			if _, err := io.WriteString(w, "chunk "); err != nil {
				return err
			}
		}
		return nil
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	_, err := group.Serve(recorder, req, key, fill)
	assert.NoError(t, err)
	assert.Equal(t, "chunk chunk chunk ", recorder.Body.String())
}

func TestSequentialRequests(t *testing.T) {
	tests := []struct {
		name        string
		retention   spool.Retention
		errorWindow time.Duration
		status      int
		header      http.Header
		wantFills   int32
	}{
		{name: "SuccessReleased", retention: spool.ReleaseCompleted, status: http.StatusOK, wantFills: 2},
		{name: "SuccessRetained", retention: spool.RetainCompleted, status: http.StatusOK, wantFills: 1},
		{name: "NotFoundWithinErrorWindow", errorWindow: time.Hour, status: http.StatusNotFound, wantFills: 1},
		{name: "NotFoundWithoutErrorWindow", status: http.StatusNotFound, wantFills: 2},
		{name: "ServerErrorNotReusable", errorWindow: time.Hour, status: http.StatusInternalServerError, wantFills: 2},
		{
			name:        "ServerErrorWithMaxAge",
			errorWindow: time.Hour,
			status:      http.StatusServiceUnavailable,
			header:      http.Header{"Cache-Control": {"max-age=60"}},
			wantFills:   1,
		},
		{
			name:        "NoStoreNotFound",
			errorWindow: time.Hour,
			status:      http.StatusNotFound,
			header:      http.Header{"Cache-Control": {"no-store"}},
			wantFills:   2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, registry, _ := newTestRegistry(t, spool.Config{ErrorWindow: tt.errorWindow})
			group := registry.NewGroup(tt.retention)
			var fills atomic.Int32
			fill := func(_ context.Context, w *spool.Writer) error {
				fills.Add(1)
				maps.Copy(w.Header(), tt.header)
				w.WriteHeader(tt.status)
				_, err := io.WriteString(w, "body")
				return err
			}
			for range 2 {
				recorder := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
				result, err := group.Serve(recorder, req, key, fill)
				assert.NoError(t, err)
				assert.Equal(t, tt.status, result.Status)
				assert.Equal(t, tt.status, recorder.Code)
				assert.Equal(t, "body", recorder.Body.String())
			}
			assert.Equal(t, tt.wantFills, fills.Load())
		})
	}
}

func TestUnsharedResponseIsNotServedToFollowers(t *testing.T) {
	ctx, registry, _ := newTestRegistry(t, spool.Config{ErrorWindow: time.Hour})
	group := registry.NewGroup(spool.ReleaseCompleted)
	release := make(chan struct{})
	fill := func(_ context.Context, w *spool.Writer) error {
		<-release
		w.WriteHeader(http.StatusInternalServerError)
		_, err := io.WriteString(w, "failed")
		return err
	}

	leader := httptest.NewRecorder()
	leaderErr := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
		_, err := group.Serve(leader, req, key, fill)
		leaderErr <- err
	}()
	waitForReaders(t, group, 1)
	follower := httptest.NewRecorder()
	followerErr := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
		_, err := group.Serve(follower, req, key, fill)
		followerErr <- err
	}()
	waitForReaders(t, group, 2)
	close(release)

	assert.NoError(t, <-leaderErr)
	assert.Equal(t, http.StatusInternalServerError, leader.Code)
	assert.Equal(t, "failed", leader.Body.String())
	assert.IsError(t, <-followerErr, spool.ErrNotShared)
	assert.False(t, follower.Flushed)
	assert.Equal(t, 0, follower.Body.Len())
}

func TestFailedFills(t *testing.T) {
	fillErr := errors.New("upstream failed")
	tests := []struct {
		name       string
		config     spool.Config
		fill       spool.FillFunc
		wantStatus int
		wantBody   string
		wantErr    error
	}{
		{
			name:    "FailsBeforeHeaders",
			fill:    func(context.Context, *spool.Writer) error { return fillErr },
			wantErr: spool.ErrNotShared,
		},
		{
			name: "FailsAfterHeaders",
			fill: func(_ context.Context, w *spool.Writer) error {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "partial")
				return fillErr
			},
			wantStatus: http.StatusOK,
			wantBody:   "partial",
			wantErr:    fillErr,
		},
		{
			name:   "ExceedsFillTimeout",
			config: spool.Config{FillTimeout: 10 * time.Millisecond},
			fill: func(ctx context.Context, w *spool.Writer) error {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "partial")
				<-ctx.Done()
				return ctx.Err()
			},
			wantStatus: http.StatusOK,
			wantBody:   "partial",
			wantErr:    context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, registry, _ := newTestRegistry(t, tt.config)
			group := registry.NewGroup(spool.ReleaseCompleted)
			recorder := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
			result, err := group.Serve(recorder, req, key, tt.fill)
			assert.IsError(t, err, tt.wantErr)
			assert.Equal(t, tt.wantStatus, result.Status)
			assert.Equal(t, tt.wantBody, recorder.Body.String())
		})
	}
}

func TestRegistryCloseCancelsFills(t *testing.T) {
	ctx, registry, _ := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.ReleaseCompleted)
	fill := func(ctx context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "partial")
		<-ctx.Done()
		return ctx.Err()
	}

	served := make(chan error, 1)
	go func() {
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
		_, err := group.Serve(httptest.NewRecorder(), req, key, fill)
		served <- err
	}()
	waitFor(t, func() bool { return group.Written(key) > 0 })
	registry.Close()
	assert.IsError(t, <-served, context.Canceled)
}

func TestGroupCloseDropsRetainedResponses(t *testing.T) {
	ctx, registry, dir := newTestRegistry(t, spool.Config{})
	group := registry.NewGroup(spool.RetainCompleted)
	fill := func(_ context.Context, w *spool.Writer) error {
		_, err := io.WriteString(w, "body")
		return err
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	_, err := group.Serve(httptest.NewRecorder(), req, key, fill)
	assert.NoError(t, err)
	files, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(files))

	group.Close()
	_, err = group.Serve(httptest.NewRecorder(), req, key, fill)
	assert.IsError(t, err, spool.ErrNotShared)
	files, err = os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
}

func TestNewRemovesStaleSpoolFiles(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "stale"+spool.FileSuffix), nil, 0o600))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "unrelated"), nil, 0o600))
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	registry, err := spool.New(ctx, spool.Config{Dir: dir})
	assert.NoError(t, err)
	defer registry.Close()
	files, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, "unrelated", files[0].Name())
}

func newTestRegistry(t *testing.T, config spool.Config) (context.Context, *spool.Registry, string) {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	config.Dir = t.TempDir()
	registry, err := spool.New(ctx, config)
	assert.NoError(t, err)
	t.Cleanup(registry.Close)
	return ctx, registry, config.Dir
}

func waitForReaders(t *testing.T, group *spool.Group, readers int) {
	t.Helper()
	waitFor(t, func() bool { return group.Readers(key) == readers })
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}

type failingCacheWriter struct{ writes int }

func (w *failingCacheWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errors.New("cache full")
	}
	return len(p), nil
}

func (w *failingCacheWriter) Close() error      { return nil }
func (w *failingCacheWriter) Abort(error) error { return nil }
