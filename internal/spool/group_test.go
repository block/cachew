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
		assert.NoError(t, err)
		fillDone <- ctx.Err()
		return w.Commit()
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
		if _, err := io.WriteString(w, "second"); err != nil {
			return err
		}
		return w.Commit()
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
	commitErr := make(chan error, 1)
	fill := func(_ context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		assert.NoError(t, w.Store(&failingCacheWriter{}))
		for range 3 {
			if _, err := io.WriteString(w, "chunk "); err != nil {
				return err
			}
		}
		commitErr <- w.Commit()
		return nil
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	_, err := group.Serve(recorder, req, key, fill)
	assert.NoError(t, err)
	assert.Equal(t, "chunk chunk chunk ", recorder.Body.String())
	assert.IsError(t, <-commitErr, errCacheFull)
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

func TestErrorReuseChecksFollowerRequest(t *testing.T) {
	tests := []struct {
		name           string
		header         http.Header
		leaderHeader   http.Header
		followerHeader http.Header
		wantFills      int32
	}{
		{name: "AnonymousFollower", wantFills: 1},
		{name: "AuthorizedFollower", followerHeader: http.Header{"Authorization": {"Bearer x"}}, wantFills: 2},
		{name: "NoCacheFollower", followerHeader: http.Header{"Cache-Control": {"no-cache"}}, wantFills: 2},
		{name: "PragmaNoCacheFollower", followerHeader: http.Header{"Pragma": {"no-cache"}}, wantFills: 2},
		{
			name:           "AuthorizedFollowerOfPublicResponse",
			header:         http.Header{"Cache-Control": {"public"}},
			followerHeader: http.Header{"Authorization": {"Bearer x"}},
			wantFills:      1,
		},
		{
			name:           "SameVariedHeader",
			header:         http.Header{"Vary": {"Accept"}},
			leaderHeader:   http.Header{"Accept": {"a"}},
			followerHeader: http.Header{"Accept": {"a"}},
			wantFills:      1,
		},
		{
			name:           "DifferentVariedHeader",
			header:         http.Header{"Vary": {"Accept"}},
			leaderHeader:   http.Header{"Accept": {"a"}},
			followerHeader: http.Header{"Accept": {"b"}},
			wantFills:      2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, registry, _ := newTestRegistry(t, spool.Config{ErrorWindow: time.Hour})
			group := registry.NewGroup(spool.ReleaseCompleted)
			var fills atomic.Int32
			fill := func(_ context.Context, w *spool.Writer) error {
				fills.Add(1)
				maps.Copy(w.Header(), tt.header)
				w.WriteHeader(http.StatusNotFound)
				return nil
			}
			for _, header := range []http.Header{tt.leaderHeader, tt.followerHeader} {
				req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
				maps.Copy(req.Header, header)
				_, err := group.Serve(httptest.NewRecorder(), req, key, fill)
				if errors.Is(err, spool.ErrNotShared) {
					_, err = group.Serve(httptest.NewRecorder(), req, key+"-own", fill)
				}
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantFills, fills.Load())
		})
	}
}

func TestErrorReuseExpiresFromReceipt(t *testing.T) {
	tests := []struct {
		name        string
		errorWindow time.Duration
		finishFirst bool
		wantFills   int32
	}{
		{name: "JoinsWithinLifetime", errorWindow: time.Hour, wantFills: 1},
		{name: "JoinsDuringFillAfterLifetime", errorWindow: 20 * time.Millisecond, wantFills: 2},
		{name: "ArrivesAfterSlowFillOutlivedLifetime", errorWindow: 20 * time.Millisecond, finishFirst: true, wantFills: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, registry, _ := newTestRegistry(t, spool.Config{ErrorWindow: tt.errorWindow})
			group := registry.NewGroup(spool.ReleaseCompleted)
			t.Cleanup(group.Close)
			release := make(chan struct{})
			var fills atomic.Int32
			fill := func(_ context.Context, w *spool.Writer) error {
				first := fills.Add(1) == 1
				w.WriteHeader(http.StatusNotFound)
				_, err := io.WriteString(w, "missing")
				if first {
					<-release
				}
				return err
			}
			serve := func(recorder *httptest.ResponseRecorder, done chan<- error) {
				req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
				_, err := group.Serve(recorder, req, key, fill)
				done <- err
			}

			firstErr := make(chan error, 1)
			go serve(httptest.NewRecorder(), firstErr)
			waitFor(t, func() bool { return group.Written(key) > 0 })
			time.Sleep(50 * time.Millisecond)
			if tt.finishFirst {
				close(release)
				assert.NoError(t, <-firstErr)
			}
			second := httptest.NewRecorder()
			secondErr := make(chan error, 1)
			go serve(second, secondErr)
			waitFor(t, func() bool { return fills.Load() == 2 || group.Readers(key) == 2 })
			if !tt.finishFirst {
				close(release)
				assert.NoError(t, <-firstErr)
			}

			assert.NoError(t, <-secondErr)
			assert.Equal(t, http.StatusNotFound, second.Code)
			assert.Equal(t, "missing", second.Body.String())
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
			name: "SucceedsWithoutCommit",
			fill: func(_ context.Context, w *spool.Writer) error {
				w.WriteHeader(http.StatusOK)
				if err := w.Store(&failingCacheWriter{}); err != nil {
					return err
				}
				_, err := io.WriteString(w, "body")
				return err
			},
			wantStatus: http.StatusOK,
			wantBody:   "body",
			wantErr:    spool.ErrUncommitted,
		},
		{
			name: "AbortsAfterHeaders",
			fill: func(_ context.Context, w *spool.Writer) error {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "partial")
				panic(http.ErrAbortHandler)
			},
			wantStatus: http.StatusOK,
			wantBody:   "partial",
			wantErr:    http.ErrAbortHandler,
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
	registry, err := spool.New(ctx, spool.Config{Dir: dir}, func(err error) { t.Error(err) })
	assert.NoError(t, err)
	defer registry.Close()
	files, err := os.ReadDir(dir)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, "unrelated", files[0].Name())
}

func TestReleaseErrorsGoToOwner(t *testing.T) {
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	releaseErrs := make(chan error, 1)
	registry, err := spool.New(ctx, spool.Config{Dir: t.TempDir()}, func(err error) { releaseErrs <- err })
	assert.NoError(t, err)
	defer registry.Close()
	group := registry.NewGroup(spool.ReleaseCompleted)
	fill := func(_ context.Context, w *spool.Writer) error {
		w.WriteHeader(http.StatusOK)
		if err := w.Store(&pendingCacheWriter{}); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "body"); err != nil {
			return err
		}
		return w.Commit()
	}

	recorder := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/object", nil)
	_, err = group.Serve(recorder, req, key, fill)
	assert.NoError(t, err)
	assert.Equal(t, "body", recorder.Body.String())
	assert.IsError(t, <-releaseErrs, errReaderClose)
}

func newTestRegistry(t *testing.T, config spool.Config) (context.Context, *spool.Registry, string) {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	config.Dir = t.TempDir()
	registry, err := spool.New(ctx, config, func(err error) { t.Error(err) })
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

var errCacheFull = errors.New("cache full")

type failingCacheWriter struct{ writes int }

func (w *failingCacheWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errCacheFull
	}
	return len(p), nil
}

func (w *failingCacheWriter) Close() error      { return nil }
func (w *failingCacheWriter) Abort(error) error { return nil }

var errReaderClose = errors.New("reader close failed")

// pendingCacheWriter supports pending reads whose reader fails to close.
type pendingCacheWriter struct {
	mu   sync.Mutex
	data []byte
}

func (w *pendingCacheWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data = append(w.data, p...)
	return len(p), nil
}

func (w *pendingCacheWriter) Close() error      { return nil }
func (w *pendingCacheWriter) Abort(error) error { return nil }

func (w *pendingCacheWriter) OpenPending() (cache.ReadAtCloser, error) {
	return pendingCacheReader{w}, nil
}

type pendingCacheReader struct{ w *pendingCacheWriter }

func (r pendingCacheReader) ReadAt(p []byte, off int64) (int, error) {
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	n := copy(p, r.w.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r pendingCacheReader) Close() error { return errReaderClose }
