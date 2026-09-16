package git //nolint:testpackage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestUploadPackLimiterUnlimited(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{})
	var releases []func()
	for range 32 {
		release, result, waited := l.acquire(t.Context(), "https://github.com/org/repo")
		assert.Equal(t, acquireOK, result)
		assert.Equal(t, time.Duration(0), waited)
		releases = append(releases, release)
	}
	for _, release := range releases {
		release()
	}
}

func TestUploadPackLimiterGlobalRejectsWhenFull(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{UploadPackCloneConcurrency: 1})
	release, result, _ := l.acquire(t.Context(), "https://github.com/org/a")
	assert.Equal(t, acquireOK, result)
	defer release()

	_, result, _ = l.acquire(t.Context(), "https://github.com/org/b")
	assert.Equal(t, acquireOverloaded, result)
}

func TestUploadPackLimiterPerRepoIndependent(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{UploadPackClonePerRepoConcurrency: 1})
	releaseA, result, _ := l.acquire(t.Context(), "https://github.com/org/a")
	assert.Equal(t, acquireOK, result)
	defer releaseA()

	releaseB, result, _ := l.acquire(t.Context(), "https://github.com/org/b")
	assert.Equal(t, acquireOK, result)
	defer releaseB()

	_, result, _ = l.acquire(t.Context(), "https://github.com/org/a")
	assert.Equal(t, acquireOverloaded, result)
}

func TestUploadPackLimiterQueueThenAdmit(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{
		UploadPackCloneConcurrency:  1,
		UploadPackCloneQueueTimeout: time.Second,
	})
	release, result, _ := l.acquire(t.Context(), "https://github.com/org/repo")
	assert.Equal(t, acquireOK, result)

	var got acquireResult
	var waited time.Duration
	done := make(chan struct{})
	go func() {
		defer close(done)
		var queuedRelease func()
		queuedRelease, got, waited = l.acquire(t.Context(), "https://github.com/org/other")
		if got == acquireOK {
			queuedRelease()
		}
	}()

	time.Sleep(50 * time.Millisecond)
	release()
	<-done
	assert.Equal(t, acquireOK, got)
	assert.True(t, waited >= 40*time.Millisecond)
}

func TestUploadPackLimiterQueueTimeoutRejects(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{
		UploadPackCloneConcurrency:  1,
		UploadPackCloneQueueTimeout: 30 * time.Millisecond,
	})
	release, result, _ := l.acquire(t.Context(), "https://github.com/org/repo")
	assert.Equal(t, acquireOK, result)
	defer release()

	start := time.Now()
	_, result, _ = l.acquire(t.Context(), "https://github.com/org/other")
	assert.Equal(t, acquireOverloaded, result)
	assert.True(t, time.Since(start) >= 30*time.Millisecond)
}

func TestUploadPackLimiterCanceled(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{
		UploadPackCloneConcurrency:  1,
		UploadPackCloneQueueTimeout: time.Second,
	})
	release, result, _ := l.acquire(t.Context(), "https://github.com/org/repo")
	assert.Equal(t, acquireOK, result)
	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, result, _ = l.acquire(ctx, "https://github.com/org/other")
	assert.Equal(t, acquireCanceled, result)
}

func TestUploadPackLimiterReleasesGlobalIfPerRepoRejects(t *testing.T) {
	t.Parallel()
	l := newUploadPackLimiter(Config{
		UploadPackCloneConcurrency:        1,
		UploadPackClonePerRepoConcurrency: 1,
	})
	release, result, _ := l.acquire(t.Context(), "https://github.com/org/a")
	assert.Equal(t, acquireOK, result)

	_, result, _ = l.acquire(t.Context(), "https://github.com/org/a")
	assert.Equal(t, acquireOverloaded, result)

	release()
	releaseB, result, _ := l.acquire(t.Context(), "https://github.com/org/b")
	assert.Equal(t, acquireOK, result)
	releaseB()
}

func TestUploadPackLimiterConcurrentRespectsGlobal(t *testing.T) {
	t.Parallel()
	const limit = 3
	l := newUploadPackLimiter(Config{UploadPackCloneConcurrency: limit})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			release, result, _ := l.acquire(t.Context(), "https://github.com/org/repo")
			if result != acquireOK {
				return
			}
			n := inFlight.Add(1)
			for {
				cur := maxInFlight.Load()
				if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			inFlight.Add(-1)
			release()
		})
	}
	wg.Wait()
	assert.True(t, maxInFlight.Load() <= limit)
	assert.True(t, maxInFlight.Load() > 0)
}
