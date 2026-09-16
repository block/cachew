package git

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/block/cachew/internal/logging"
)

type acquireResult int

const (
	acquireOK acquireResult = iota
	acquireOverloaded
	acquireCanceled
)

func (r acquireResult) String() string {
	switch r {
	case acquireOK:
		return "admitted"
	case acquireOverloaded:
		return "rejected"
	case acquireCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// uploadPackLimiter bounds concurrent clone-shaped git-upload-pack responses.
// Incremental fetches are never passed through this type.
type uploadPackLimiter struct {
	global       chan struct{} // nil if unlimited
	perRepoLimit int
	perRepo      sync.Map // string -> chan struct{}
	queueTimeout time.Duration
	retryAfter   time.Duration
}

func newUploadPackLimiter(cfg Config) *uploadPackLimiter {
	l := &uploadPackLimiter{
		perRepoLimit: cfg.UploadPackClonePerRepoConcurrency,
		queueTimeout: cfg.UploadPackCloneQueueTimeout,
		retryAfter:   cfg.UploadPackCloneRetryAfter,
	}
	if l.retryAfter <= 0 {
		l.retryAfter = 30 * time.Second
	}
	if cfg.UploadPackCloneConcurrency > 0 {
		l.global = newTokenSem(cfg.UploadPackCloneConcurrency)
	}
	return l
}

func newTokenSem(n int) chan struct{} {
	ch := make(chan struct{}, n)
	for range n {
		ch <- struct{}{}
	}
	return ch
}

func (l *uploadPackLimiter) enabled() bool {
	return l != nil && (l.global != nil || l.perRepoLimit > 0)
}

func (l *uploadPackLimiter) repoSem(repo string) chan struct{} {
	if v, ok := l.perRepo.Load(repo); ok {
		return v.(chan struct{})
	}
	ch := newTokenSem(l.perRepoLimit)
	actual, _ := l.perRepo.LoadOrStore(repo, ch)
	return actual.(chan struct{})
}

func (l *uploadPackLimiter) acquire(ctx context.Context, repo string) (func(), acquireResult, time.Duration) {
	release := func() {}
	if !l.enabled() {
		return release, acquireOK, 0
	}
	start := time.Now()
	var deadline time.Time
	if l.queueTimeout > 0 {
		deadline = start.Add(l.queueTimeout)
	}

	if l.global != nil {
		if result := waitToken(ctx, l.global, deadline); result != acquireOK {
			return release, result, time.Since(start)
		}
	}

	var repoCh chan struct{}
	if l.perRepoLimit > 0 {
		repoCh = l.repoSem(repo)
		if result := waitToken(ctx, repoCh, deadline); result != acquireOK {
			if l.global != nil {
				l.global <- struct{}{}
			}
			return release, result, time.Since(start)
		}
	}

	return func() {
		if repoCh != nil {
			repoCh <- struct{}{}
		}
		if l.global != nil {
			l.global <- struct{}{}
		}
	}, acquireOK, time.Since(start)
}

func waitToken(ctx context.Context, ch chan struct{}, deadline time.Time) acquireResult {
	if ctx.Err() != nil {
		return acquireCanceled
	}
	if deadline.IsZero() {
		select {
		case <-ch:
			return acquireOK
		default:
			return acquireOverloaded
		}
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		select {
		case <-ch:
			return acquireOK
		default:
			return acquireOverloaded
		}
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-ch:
		return acquireOK
	case <-ctx.Done():
		return acquireCanceled
	case <-timer.C:
		return acquireOverloaded
	}
}

func (s *Strategy) gateCloneUploadPack(w http.ResponseWriter, r *http.Request, repo string) (func(), bool) {
	release, result, waited := s.uploadPackLimiter.acquire(r.Context(), repo)
	s.metrics.recordUploadPackCloneGate(r.Context(), result.String(), repo, waited)
	switch result {
	case acquireOK:
		if waited > 0 {
			logging.FromContext(r.Context()).InfoContext(r.Context(),
				"Admitted clone-shaped upload-pack after queue wait",
				"upstream", repo, "waited", waited)
		}
		return release, true
	case acquireCanceled:
		return func() {}, false
	case acquireOverloaded:
		s.rejectCloneUploadPack(w, r, repo)
		return func() {}, false
	default:
		s.rejectCloneUploadPack(w, r, repo)
		return func() {}, false
	}
}

func (s *Strategy) rejectCloneUploadPack(w http.ResponseWriter, r *http.Request, repo string) {
	retryAfter := 30 * time.Second
	if s.uploadPackLimiter != nil && s.uploadPackLimiter.retryAfter > 0 {
		retryAfter = s.uploadPackLimiter.retryAfter
	}
	sec := max(int(retryAfter.Seconds()), 1)
	logging.FromContext(r.Context()).WarnContext(r.Context(),
		"Rejecting clone-shaped upload-pack due to concurrency limit",
		"upstream", repo, "retry_after", retryAfter)
	w.Header().Set("Retry-After", strconv.Itoa(sec))
	http.Error(w, "too many concurrent git clones; retry later", http.StatusServiceUnavailable)
}
