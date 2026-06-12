package git //nolint:testpackage // white-box test for unexported range helpers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/cache"
)

func TestParseSingleByteRange(t *testing.T) {
	tests := []struct {
		header     string
		start, end int64
		ok         bool
	}{
		{"bytes=0-99", 0, 99, true},
		{"bytes=100-199", 100, 199, true},
		{"bytes=5-5", 5, 5, true},
		{"", 0, 0, false},
		{"bytes=0-", 0, 0, false},           // open-ended rejected
		{"bytes=-100", 0, 0, false},         // suffix rejected
		{"bytes=0-99,200-299", 0, 0, false}, // multi-range rejected
		{"bytes=99-0", 0, 0, false},         // end < start rejected
		{"items=0-99", 0, 0, false},         // wrong unit
		{"bytes=a-b", 0, 0, false},          // non-numeric
	}
	for _, tt := range tests {
		start, end, ok := parseSingleByteRange(tt.header)
		assert.Equal(t, tt.ok, ok, "ok for %q", tt.header)
		if tt.ok {
			assert.Equal(t, tt.start, start, "start for %q", tt.header)
			assert.Equal(t, tt.end, end, "end for %q", tt.header)
		}
	}
}

// failPinCache fails the test if the pinned-range handler reaches the cache,
// proving the oversized-range guard rejects the request before any read.
type failPinCache struct{ t *testing.T }

func (p failPinCache) Pin(context.Context, cache.Key) (cache.PinnedObject, error) {
	p.t.Fatal("Pin must not be called")
	return cache.PinnedObject{}, nil //nolint:nilnil // unreachable; t.Fatal above
}

func (p failPinCache) OpenPinnedRange(context.Context, cache.Key, string, int64, int64) (io.ReadCloser, int64, error) {
	p.t.Fatal("OpenPinnedRange must not be called for an oversized range")
	return nil, 0, nil
}

func TestServePinnedRangeRejectsOversizedRange(t *testing.T) {
	s := &Strategy{}
	req := httptest.NewRequest(http.MethodGet, "/git/x/snapshot.tar.zst", nil)
	// Length = maxPinRangeBytes + 1, one byte over the cap.
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", int64(maxPinRangeBytes)))
	w := httptest.NewRecorder()

	s.servePinnedRange(w, req, failPinCache{t}, cache.NewKey("k"), "repo", "x", time.Now())

	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, w.Code)
}

func TestStrongIfMatchETag(t *testing.T) {
	tests := []struct {
		header string
		etag   string
		ok     bool
	}{
		{`"abc123"`, "abc123", true},
		{`  "abc123"  `, "abc123", true},
		{"", "", false},
		{"*", "", false},
		{`W/"abc123"`, "", false},   // weak validator rejected
		{`"abc", "def"`, "", false}, // multi-value rejected
		{"abc123", "", false},       // unquoted rejected
	}
	for _, tt := range tests {
		etag, ok := strongIfMatchETag(tt.header)
		assert.Equal(t, tt.ok, ok, "ok for %q", tt.header)
		if tt.ok {
			assert.Equal(t, tt.etag, etag, "etag for %q", tt.header)
		}
	}
}
