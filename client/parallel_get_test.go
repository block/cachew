package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

// Compile-time assertion that the concrete Client satisfies the narrow
// interface ParallelGet drives.
var _ client.RangeReader = (*client.Client)(nil)

// bufferAt is an in-memory io.WriterAt that extends like a file, zero-filling
// any gap, so tests can assert reassembly without touching disk.
type bufferAt struct {
	mu  sync.Mutex
	buf []byte
}

func (b *bufferAt) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if end := int(off) + len(p); end > len(b.buf) {
		b.buf = append(b.buf, make([]byte, end-len(b.buf))...)
	}
	copy(b.buf[off:], p)
	return len(p), nil
}

// rangeFlipReader serves correct byte ranges but ignores If-Match and reports
// a different ETag for any chunk past the first, simulating an object
// rewritten mid-download behind a server that does not honour preconditions.
// It exercises the client-side response-ETag guard.
type rangeFlipReader struct {
	data      []byte
	firstETag string
	restETag  string
}

func (f *rangeFlipReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	size := int64(len(f.data))
	start, length, outcome := client.NewRequestOptions(opts...).ResolveRange(size, f.firstETag)
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}

	etag := f.firstETag
	if start > 0 {
		etag = f.restETag
	}
	headers.Set(client.ETagKey, etag)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(f.data[start : start+length])), headers, nil
}

func TestParallelGetETagMismatch(t *testing.T) {
	c := &rangeFlipReader{data: make([]byte, 1000), firstETag: `"v1"`, restETag: `"v2"`}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object changed during read")
}

// ifMatchFlipReader honours If-Match like a conforming server whose object is
// rewritten after the discovery request, so every pinned chunk fails its
// precondition with a bodiless rejection.
type ifMatchFlipReader struct {
	data      []byte
	firstETag string
	restETag  string
}

func (f *ifMatchFlipReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	o := client.NewRequestOptions(opts...)
	etag := f.firstETag
	if o.IfMatch != "" {
		etag = f.restETag
	}
	if err := o.Check(etag); err != nil {
		return nil, nil, err
	}
	size := int64(len(f.data))
	start, length, outcome := o.ResolveRange(size, etag)
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	headers.Set(client.ETagKey, etag)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(f.data[start : start+length])), headers, nil
}

func TestParallelGetPreconditionFailedOnRewrite(t *testing.T) {
	c := &ifMatchFlipReader{data: make([]byte, 1000), firstETag: `"v1"`, restETag: `"v2"`}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.IsError(t, err, client.ErrPreconditionFailed)
	assert.Contains(t, err.Error(), "object changed during read")
}

// noETagReader serves byte ranges but never sets an ETag, modelling a legacy
// entry or a RangeReader implementation that omits it.
type noETagReader struct {
	data []byte
}

func (n *noETagReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	size := int64(len(n.data))
	start, length, outcome := client.NewRequestOptions(opts...).ResolveRange(size, "")
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(n.data[start : start+length])), headers, nil
}

func TestParallelGetNoETagMultiChunk(t *testing.T) {
	// A multi-chunk object with no ETag can't be pinned, so it falls back to a
	// single full read (backwards compatible with objects stored before ETags).
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	c := &noETagReader{data: data}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
}

func TestParallelGetNoETagSingleChunk(t *testing.T) {
	// A no-ETag object delivered entirely by the discovery request is a single
	// revision, so it succeeds without pinning.
	data := []byte("0123456789")
	c := &noETagReader{data: data}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
}

// changingSizeReader serves a multi-chunk body with no ETag on the ranged
// discovery request, then a differently sized body on the subsequent full
// (non-range) read, modelling an object rewritten between the two requests.
type changingSizeReader struct {
	discovery []byte
	rewritten []byte
}

func (c *changingSizeReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	o := client.NewRequestOptions(opts...)
	headers := http.Header{}
	if o.Range == "" {
		headers.Set("Content-Length", strconv.FormatInt(int64(len(c.rewritten)), 10))
		return io.NopCloser(bytes.NewReader(c.rewritten)), headers, nil
	}
	size := int64(len(c.discovery))
	start, length, outcome := o.ResolveRange(size, "")
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(c.discovery[start : start+length])), headers, nil
}

// openRecord captures the fetch-shaping options of a single Open call: the
// Range requested ("" for a full read) and the If-Match validator it was
// pinned to.
type openRecord struct {
	Range   string
	IfMatch string
}

// recordingReader serves byte ranges and records an openRecord for every Open
// call, so tests can assert how the object was fetched.
type recordingReader struct {
	data []byte
	etag string

	mu    sync.Mutex
	opens []openRecord
}

func (r *recordingReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	o := client.NewRequestOptions(opts...)
	r.mu.Lock()
	r.opens = append(r.opens, openRecord{Range: o.Range, IfMatch: o.IfMatch})
	r.mu.Unlock()

	size := int64(len(r.data))
	start, length, outcome := o.ResolveRange(size, r.etag)
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	if r.etag != "" {
		headers.Set(client.ETagKey, r.etag)
	}
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
}

func TestParallelGetSingleWorkerFullRead(t *testing.T) {
	// A concurrency of 1 gains nothing from chunking, so it must issue a single
	// non-ranged read rather than discovering and serialising ranged GETs.
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	c := &recordingReader{data: data, etag: `"v1"`}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 1)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
	assert.Equal(t, []openRecord{{}}, c.opens)
}

func TestParallelGetPinsChunksWithIfMatch(t *testing.T) {
	// Every chunk after discovery must carry the discovery ETag as an If-Match
	// precondition, so a version change is rejected server-side without a body.
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	c := &recordingReader{data: data, etag: `"v1"`}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)

	expected := []openRecord{{Range: "bytes=0-99"}}
	for seq := 1; seq < 10; seq++ {
		expected = append(expected, openRecord{Range: fmt.Sprintf("bytes=%d-%d", seq*100, seq*100+99), IfMatch: `"v1"`})
	}
	slices.SortFunc(c.opens, func(a, b openRecord) int { return strings.Compare(a.Range, b.Range) })
	assert.Equal(t, expected, c.opens)
}

func TestParallelGetNoETagSizeChangedBetweenRequests(t *testing.T) {
	// A no-ETag multi-chunk object falls back to a single full read. If it is
	// rewritten to a different size between discovery and that read, the
	// discovery total must not be used to validate the full body: the full read
	// is itself a consistent revision and should be accepted in its entirety.
	c := &changingSizeReader{discovery: make([]byte, 1000), rewritten: []byte("changed")}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, c.rewritten, dst.buf)
}
