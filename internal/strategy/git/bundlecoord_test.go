package git //nolint:testpackage // These tests need access to build ownership.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

type bundlePublishGate struct {
	cache.Cache
	started chan context.Context
	release chan struct{}
	creates atomic.Int32
	fail    atomic.Bool
}

func (c *bundlePublishGate) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) (cache.Writer, error) {
	c.creates.Add(1)
	c.started <- ctx
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if c.fail.Load() {
		return nil, errors.New("cache unavailable")
	}
	return c.Cache.Create(ctx, key, headers, ttl, opts...)
}

func newBundleBuildTest(t *testing.T) (*Strategy, *gitclone.Repository, string, *bundlePublishGate) {
	t.Helper()
	ctx := logging.ContextWithLogger(t.Context(), slog.Default())
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, nil)
	assert.NoError(t, err)
	repo, err := manager.GetOrCreate(ctx, "https://example.com/org/repo")
	assert.NoError(t, err)
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.com")
		out, err := cmd.CombinedOutput()
		assert.NoError(t, err, string(out))
		return strings.TrimSpace(string(out))
	}
	run("init", repo.Path())
	run("-C", repo.Path(), "commit", "--allow-empty", "-m", "base")
	base := run("-C", repo.Path(), "rev-parse", "HEAD")
	run("-C", repo.Path(), "commit", "--allow-empty", "-m", "next")
	repo.MarkReady()
	mem, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	gate := &bundlePublishGate{Cache: mem, started: make(chan context.Context, 32), release: make(chan struct{})}
	s := &Strategy{ctx: ctx, cache: gate, cloneManager: manager, metrics: newGitMetrics(), config: Config{BundleCacheTTL: time.Hour}}
	return s, repo, base, gate
}

func serveTestBundle(ctx context.Context, s *Strategy, base string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/git/example.com/org/repo/snapshot.bundle?base="+base, nil)
	w := httptest.NewRecorder()
	s.handleBundleRequest(w, r, "example.com", "org/repo/snapshot.bundle")
	return w
}

func awaitBundleBuild(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bundle build did not stop")
	}
}

type blockedBundleResponse struct {
	*httptest.ResponseRecorder
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedBundleResponse) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.started)
		<-w.release
	})
	return w.ResponseRecorder.Write(p)
}

func TestBundleVerifiedEmptyResultIsShared(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, repo, _, gate := newBundleBuildTest(t)
		close(gate.release)
		s.cache = gate.Cache
		base, err := mirrorHead(s.ctx, repo.Path())
		assert.NoError(t, err)
		output, err := exec.CommandContext(s.ctx, "git", "-C", repo.Path(), "remote", "add", "origin", repo.Path()).CombinedOutput()
		assert.NoError(t, err, string(output))
		realGit, err := exec.LookPath("git")
		assert.NoError(t, err)
		bin := t.TempDir()
		countFile := filepath.Join(bin, "fetch-count")
		script := fmt.Sprintf("#!/bin/sh\nfor arg in \"$@\"; do\n  if [ \"$arg\" = fetch ]; then echo fetch >> %q; fi\ndone\nexec %q \"$@\"\n", countFile, realGit)
		assert.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o700))
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		entered := make(chan struct{})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		t.Cleanup(unblock)
		lockDone := make(chan error, 1)
		go func() {
			lockDone <- repo.WithFetchExclusion(s.ctx, func() error {
				close(entered)
				<-release
				return nil
			})
		}()
		<-entered
		const requests = 16
		results := make(chan *httptest.ResponseRecorder, requests)
		for range requests {
			go func() { results <- serveTestBundle(s.ctx, s, base) }()
		}
		synctest.Wait()
		unblock()
		assert.NoError(t, <-lockDone)
		for range requests {
			response := <-results
			assert.Equal(t, http.StatusNoContent, response.Code)
		}
		counts, err := os.ReadFile(countFile)
		assert.NoError(t, err)
		assert.Equal(t, 1, strings.Count(string(counts), "fetch\n"))
		assert.Equal(t, http.StatusNoContent, serveTestBundle(s.ctx, s, base).Code)
		counts, err = os.ReadFile(countFile)
		assert.NoError(t, err)
		assert.Equal(t, 2, strings.Count(string(counts), "fetch\n"))
	})
}

func TestBundleBuildCoalescing(t *testing.T) {
	for _, background := range []bool{false, true} {
		for _, backend := range []string{"memory", "noop", "failure"} {
			t.Run(fmt.Sprintf("background=%t/%s", background, backend), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					testBundleBuildCoalescing(t, background, backend)
				})
			})
		}
	}
}

func testBundleBuildCoalescing(t *testing.T, background bool, backend string) {
	t.Helper()
	s, repo, base, gate := newBundleBuildTest(t)
	if backend == "noop" {
		gate.Cache = cache.NoOpCache()
	}
	gate.fail.Store(backend == "failure")
	unblockCache := sync.OnceFunc(func() { close(gate.release) })
	defer unblockCache()
	entered, generate := make(chan struct{}), make(chan struct{})
	unblockGeneration := sync.OnceFunc(func() { close(generate) })
	defer unblockGeneration()
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- repo.WithFetchExclusion(s.ctx, func() error {
			close(entered)
			<-generate
			return nil
		})
	}()
	<-entered
	requestCtx, cancelRequest := context.WithCancel(s.ctx)
	defer cancelRequest()
	w := &blockedBundleResponse{ResponseRecorder: httptest.NewRecorder(), started: make(chan struct{}), release: make(chan struct{})}
	unblockClient := sync.OnceFunc(func() { close(w.release) })
	defer unblockClient()
	clientDone := make(chan struct{})
	serveSlowClient := func() {
		defer close(clientDone)
		r := httptest.NewRequestWithContext(requestCtx, http.MethodGet, "/snapshot.bundle?base="+base, nil)
		s.handleBundleRequest(w, r, "example.com", "org/repo/snapshot.bundle")
	}
	if background {
		s.pregenerateBundle(requestCtx, repo, repo.UpstreamURL(), base)
	} else {
		go serveSlowClient()
	}
	synctest.Wait()
	key := bundleCacheKey(repo.UpstreamURL(), base)
	entry, ok := s.bundleBuilds.Load(key)
	assert.True(t, ok)
	build := entry.(*bundleBuild)
	const requests = 8
	results := make(chan *httptest.ResponseRecorder, requests)
	for range requests {
		go func() { results <- serveTestBundle(s.ctx, s, base) }()
		s.pregenerateBundle(s.ctx, repo, repo.UpstreamURL(), base)
	}
	refs := int64(requests + 1)
	if background {
		go serveSlowClient()
		refs++
	}
	synctest.Wait()
	assert.Equal(t, refs, build.refs.Load())
	unblockGeneration()
	assert.NoError(t, <-lockDone)
	publishCtx := <-gate.started
	awaitBundleBuild(t, w.started)
	var body string
	for range requests {
		response := <-results
		assert.Equal(t, http.StatusOK, response.Code)
		if body == "" {
			body = response.Body.String()
		}
		assert.Equal(t, body, response.Body.String())
	}
	cancelRequest()
	assert.NoError(t, publishCtx.Err())
	assert.NoError(t, repo.WithFetchExclusion(s.ctx, func() error { return nil }))
	unblockCache()
	synctest.Wait()
	_, active := s.bundleBuilds.Load(key)
	assert.False(t, active)
	assert.Equal(t, int32(1), gate.creates.Load())
	_, err := build.file.Stat()
	assert.NoError(t, err)
	unblockClient()
	awaitBundleBuild(t, clientDone)
	synctest.Wait()
	assert.Equal(t, body, w.Body.String())
	_, err = build.file.Stat()
	assert.IsError(t, err, os.ErrClosed)
	if backend == "memory" {
		reader, _, err := gate.Cache.Open(s.ctx, key)
		assert.NoError(t, err)
		defer reader.Close()
		cached, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, body, string(cached))
	}
	bundlePath := filepath.Join(t.TempDir(), "result.bundle")
	assert.NoError(t, os.WriteFile(bundlePath, []byte(body), 0o600))
	output, err := exec.CommandContext(s.ctx, "git", "-C", repo.Path(), "bundle", "verify", bundlePath).CombinedOutput()
	assert.NoError(t, err, string(output))
}

func TestBundleBuildFailureAllowsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, repo, base, gate := newBundleBuildTest(t)
		gate.fail.Store(true)
		s.pregenerateBundle(s.ctx, repo, repo.UpstreamURL(), base)
		<-gate.started
		close(gate.release)
		synctest.Wait()
		_, err := gate.Stat(s.ctx, bundleCacheKey(repo.UpstreamURL(), base))
		assert.IsError(t, err, os.ErrNotExist)
		gate.fail.Store(false)
		response := serveTestBundle(s.ctx, s, base)
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Contains(t, response.Body.String(), "# v2 git bundle")
		synctest.Wait()
		assert.Equal(t, int32(2), gate.creates.Load())
	})
}

func TestBundleWaitDeadlineSurvivesBuildFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := logging.ContextWithLogger(t.Context(), slog.Default())
		mem, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
		assert.NoError(t, err)
		s := &Strategy{ctx: ctx, cache: mem, metrics: newGitMetrics()}
		base := strings.Repeat("a", 40)
		key := bundleCacheKey("https://example.com/org/repo", base)
		first := newBundleBuild()
		s.bundleBuilds.Store(key, first)
		result := make(chan *httptest.ResponseRecorder, 1)
		start := time.Now()
		go func() { result <- serveTestBundle(ctx, s, base) }()
		synctest.Wait()
		time.Sleep(4 * time.Minute)
		s.bundleBuilds.Store(key, newBundleBuild())
		close(first.done)
		response := <-result
		assert.Equal(t, http.StatusServiceUnavailable, response.Code)
		assert.Equal(t, 5*time.Minute, time.Since(start))
	})
}

func TestBundleRetryPublicationKeepsOriginalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, repo, base, gate := newBundleBuildTest(t)
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Minute)
		defer cancel()
		key := bundleCacheKey(repo.UpstreamURL(), base)
		first := newBundleBuild()
		s.bundleBuilds.Store(key, first)
		result := make(chan *httptest.ResponseRecorder, 1)
		start := time.Now()
		go func() { result <- serveTestBundle(ctx, s, base) }()
		synctest.Wait()
		time.Sleep(4 * time.Minute)
		s.bundleBuilds.Delete(key)
		close(first.done)
		response := <-result
		assert.Equal(t, 4*time.Minute, time.Since(start))
		deadline, ok := (<-gate.started).Deadline()
		assert.True(t, ok)
		assert.Equal(t, start.Add(5*time.Minute), deadline)
		assert.Equal(t, http.StatusOK, response.Code)
		assert.Contains(t, response.Body.String(), "# v2 git bundle")
		time.Sleep(time.Minute)
		synctest.Wait()
		_, active := s.bundleBuilds.Load(key)
		assert.False(t, active)
	})
}

func TestBundleBuildCancellationReleasesFetchLock(t *testing.T) {
	for _, background := range []bool{false, true} {
		t.Run(fmt.Sprintf("background=%t", background), func(t *testing.T) {
			s, repo, base, gate := newBundleBuildTest(t)
			close(gate.release)
			realGit, err := exec.LookPath("git")
			assert.NoError(t, err)
			bin := t.TempDir()
			started := filepath.Join(bin, "started")
			script := fmt.Sprintf("#!/bin/sh\nif [ \"$3\" = bundle ]; then\n  sleep 2 &\n  touch %q\n  wait\nfi\nexec %q \"$@\"\n", started, realGit)
			assert.NoError(t, os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0o700))
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			ctx, cancel := context.WithCancel(s.ctx)
			defer cancel()
			var done <-chan struct{}
			if background {
				requestCtx := s.ctx
				s.ctx = ctx
				s.pregenerateBundle(requestCtx, repo, repo.UpstreamURL(), base)
				entry, ok := s.bundleBuilds.Load(bundleCacheKey(repo.UpstreamURL(), base))
				assert.True(t, ok)
				done = entry.(*bundleBuild).done
			} else {
				completed := make(chan struct{})
				done = completed
				go func() {
					defer close(completed)
					file, err := s.createBundle(ctx, repo, base)
					if file != nil {
						_ = file.Close()
					}
					assert.Error(t, err)
				}()
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(started); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("git bundle did not start")
				}
				time.Sleep(5 * time.Millisecond)
			}
			start := time.Now()
			cancel()
			awaitBundleBuild(t, done)
			assert.True(t, time.Since(start) < time.Second, "git child process kept the output pipe open")
			lockCtx, unlock := context.WithTimeout(t.Context(), time.Second)
			defer unlock()
			assert.NoError(t, repo.WithFetchExclusion(lockCtx, func() error { return nil }))
			assert.Equal(t, int32(0), gate.creates.Load())
		})
	}
}
