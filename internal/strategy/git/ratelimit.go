package git

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

// ClientCloneTracker limits how many concurrent clone operations a single
// client IP can trigger. When a client exceeds the limit, the request is
// rejected with 429 Too Many Requests.
type ClientCloneTracker struct {
	mu       sync.Mutex
	inflight map[string]*atomic.Int32
	limit    int
}

// NewClientCloneTracker creates a tracker that allows at most limit concurrent
// clone operations per client IP.
func NewClientCloneTracker(limit int) *ClientCloneTracker {
	return &ClientCloneTracker{
		inflight: make(map[string]*atomic.Int32),
		limit:    limit,
	}
}

// TryAcquire attempts to reserve a clone slot for the given client IP.
// Returns true and a release function if the client is under the limit.
// Returns false if the client has reached the limit.
func (t *ClientCloneTracker) TryAcquire(clientIP string) (release func(), ok bool) {
	t.mu.Lock()
	counter, exists := t.inflight[clientIP]
	if !exists {
		counter = &atomic.Int32{}
		t.inflight[clientIP] = counter
	}
	t.mu.Unlock()

	if int(counter.Load()) >= t.limit {
		return nil, false
	}
	counter.Add(1)

	var once sync.Once
	return func() {
		once.Do(func() {
			if counter.Add(-1) <= 0 {
				t.mu.Lock()
				// Re-check under lock to avoid racing with another acquire.
				if counter.Load() <= 0 {
					delete(t.inflight, clientIP)
				}
				t.mu.Unlock()
			}
		})
	}, true
}

// ClientIP extracts the IP address from an HTTP request, stripping the port.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
