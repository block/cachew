package git_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/strategy/git"
)

func TestClientCloneTrackerBasic(t *testing.T) {
	tracker := git.NewClientCloneTracker(2)

	r1, ok := tracker.TryAcquire("10.0.0.1")
	assert.True(t, ok)

	r2, ok := tracker.TryAcquire("10.0.0.1")
	assert.True(t, ok)

	// Third should be rejected.
	_, ok = tracker.TryAcquire("10.0.0.1")
	assert.False(t, ok)

	// Different client should be fine.
	r3, ok := tracker.TryAcquire("10.0.0.2")
	assert.True(t, ok)

	// Release one slot for 10.0.0.1.
	r1()

	r4, ok := tracker.TryAcquire("10.0.0.1")
	assert.True(t, ok)

	r2()
	r3()
	r4()
}

func TestClientCloneTrackerReleaseIdempotent(t *testing.T) {
	tracker := git.NewClientCloneTracker(1)

	release, ok := tracker.TryAcquire("10.0.0.1")
	assert.True(t, ok)

	// Double-release should not corrupt the counter.
	release()
	release()

	r2, ok := tracker.TryAcquire("10.0.0.1")
	assert.True(t, ok)
	r2()
}

func TestClientCloneTrackerConcurrent(t *testing.T) {
	tracker := git.NewClientCloneTracker(3)

	// Hold 3 slots so all subsequent acquires must be rejected.
	releases := make([]func(), 3)
	for i := range 3 {
		r, ok := tracker.TryAcquire("10.0.0.1")
		assert.True(t, ok)
		releases[i] = r
	}

	var rejected atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, ok := tracker.TryAcquire("10.0.0.1")
			if !ok {
				rejected.Add(1)
			}
		})
	}

	wg.Wait()
	assert.Equal(t, int32(20), rejected.Load(), "all attempts should be rejected while slots are held")

	for _, r := range releases {
		r()
	}
}

func TestClientIP(t *testing.T) {
	tests := []struct {
		remoteAddr string
		want       string
	}{
		{"192.168.1.1:12345", "192.168.1.1"},
		{"[::1]:8080", "::1"},
		{"192.168.1.1", "192.168.1.1"},
	}
	for _, tt := range tests {
		r := &http.Request{RemoteAddr: tt.remoteAddr}
		assert.Equal(t, tt.want, git.ClientIP(r), "ClientIP(%q)", tt.remoteAddr)
	}
}
