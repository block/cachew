package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// Compile-time assertion that the concrete Client satisfies the narrow
// interface ParallelGet drives.
var _ client.RangeReader = (*client.Client)(nil)

func patternBytes(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

// collect runs ParallelGet into an in-memory WriterAt and returns the
// reassembled bytes.
func collect(c client.RangeReader, key client.Key, chunkSize int64, concurrency int) ([]byte, error) {
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, key, dst, chunkSize, concurrency)
	return dst.buf, err
}

// rangeFlipReader serves correct byte ranges but reports a different ETag for
// any chunk past the first, simulating an object rewritten mid-download.
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
	_, err := collect(c, client.NewKey("k"), 100, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "object changed during read")
}

// ifMatchFlipReader honours If-Match against firstETag on the discovery request
// but reports restETag (and a 412) for any pinned chunk, modelling a server that
// enforces If-Match on an object rewritten mid-download.
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
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), dst, 100, 4)
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
	data := patternBytes(1000)
	c := &noETagReader{data: data}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetNoETagSingleChunk(t *testing.T) {
	data := []byte("0123456789")
	c := &noETagReader{data: data}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
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

func TestParallelGetNoETagSizeChangedBetweenRequests(t *testing.T) {
	c := &changingSizeReader{discovery: make([]byte, 1000), rewritten: []byte("changed")}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, c.rewritten, got)
}

type openRecord struct {
	Range   string
	IfMatch string
}

// recordingReader serves byte ranges and records the Range and If-Match options
// of every Open call (both "" for a full, non-ranged read), so tests can assert
// how the object was fetched and how chunks were pinned.
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

func TestParallelGetReassembly(t *testing.T) {
	data := patternBytes(10_000)
	c := &recordingReader{data: data, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetSingleWorkerFullRead(t *testing.T) {
	data := patternBytes(1000)
	c := &recordingReader{data: data, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 100, 1)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, []openRecord{{}}, c.opens)
}

func TestParallelGetPinsChunksWithIfMatch(t *testing.T) {
	data := patternBytes(1000)
	c := &recordingReader{data: data, etag: `"v1"`}
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)

	expected := []openRecord{{Range: "bytes=0-99"}}
	for seq := 1; seq < 10; seq++ {
		expected = append(expected, openRecord{Range: fmt.Sprintf("bytes=%d-%d", seq*100, seq*100+99), IfMatch: `"v1"`})
	}
	slices.SortFunc(c.opens, func(a, b openRecord) int { return strings.Compare(a.Range, b.Range) })
	assert.Equal(t, expected, c.opens)
}

func TestParallelGetEmptyObject(t *testing.T) {
	c := &recordingReader{data: nil, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))
}

func TestParallelGetServerIgnoresRange(t *testing.T) {
	data := patternBytes(1000)
	c := &ignoreRangeReader{data: data}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetOutOfOrderCompletion(t *testing.T) {
	data := patternBytes(10_000)
	c := &reorderReader{data: data, etag: `"v1"`, chunkSize: 1000}
	got, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetPropagatesOpenError(t *testing.T) {
	c := &failingChunkReader{data: patternBytes(10_000), etag: `"v1"`, failAtStart: 5000}
	_, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestParallelGetRejectsOverlongChunk(t *testing.T) {
	c := &fullBodyOnChunkReader{data: patternBytes(10_000), etag: `"v1"`}
	_, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "more than the expected 1000 bytes")
}

func TestParallelGetRejectsMidflightRangeIgnore(t *testing.T) {
	c := &midflightRangeIgnoreReader{data: patternBytes(10_000), etag: `"v1"`}
	_, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server ignored range")
}

// bufferAt is an in-memory io.WriterAt that extends like a file, zero-filling
// gaps, so tests can assert reassembly without touching disk.
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

// fullBodyOnChunkReader honours the discovery range (start 0) with a proper 206
// but ignores the range on any later chunk, returning the entire object with the
// same ETag — modelling a backend that degrades to full responses mid-download.
type fullBodyOnChunkReader struct {
	data []byte
	etag string
}

func (r *fullBodyOnChunkReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	size := int64(len(r.data))
	start, length, outcome := client.NewRequestOptions(opts...).ResolveRange(size, r.etag)
	headers := http.Header{}
	headers.Set(client.ETagKey, r.etag)
	if outcome == client.RangePartial && start == 0 {
		headers.Set("Content-Length", strconv.FormatInt(length, 10))
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
		return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
	}
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	headers.Set("Content-Length", strconv.FormatInt(size, 10))
	return io.NopCloser(bytes.NewReader(r.data)), headers, nil
}

// midflightRangeIgnoreReader ranges the discovery chunk but answers subsequent
// chunk requests with the full body and no Content-Range, modelling a replica
// that stops honouring ranges mid-download.
type midflightRangeIgnoreReader struct {
	data []byte
	etag string
}

func (r *midflightRangeIgnoreReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	size := int64(len(r.data))
	start, length, outcome := client.NewRequestOptions(opts...).ResolveRange(size, r.etag)
	headers := http.Header{}
	headers.Set(client.ETagKey, r.etag)
	if outcome == client.RangePartial && start == 0 {
		headers.Set("Content-Length", strconv.FormatInt(length, 10))
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
		return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
	}
	headers.Set("Content-Length", strconv.FormatInt(size, 10))
	return io.NopCloser(bytes.NewReader(r.data)), headers, nil
}

// ignoreRangeReader returns the whole object with no Content-Range regardless of
// the requested range, modelling a backend that doesn't honour ranges.
type ignoreRangeReader struct{ data []byte }

func (r *ignoreRangeReader) Open(_ context.Context, _ client.Key, _ ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	headers := http.Header{}
	headers.Set("Content-Length", strconv.Itoa(len(r.data)))
	return io.NopCloser(bytes.NewReader(r.data)), headers, nil
}

// reorderReader serves correct byte ranges but delays earlier offsets longer
// than later ones, so within the in-flight window chunks complete out of order
// and the writer must buffer and reorder them.
type reorderReader struct {
	data      []byte
	etag      string
	chunkSize int64
}

func (r *reorderReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	size := int64(len(r.data))
	o := client.NewRequestOptions(opts...)
	start, length, outcome := o.ResolveRange(size, r.etag)
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	// Earlier chunks within a window sleep longer, so higher offsets finish
	// first and the writer is forced to reorder.
	if outcome == client.RangePartial {
		chunks := (size - start) / r.chunkSize
		time.Sleep(time.Duration(chunks) * time.Millisecond)
	}
	headers.Set(client.ETagKey, r.etag)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
}

// failingChunkReader serves ranges normally but errors when the requested range
// starts at failAtStart, modelling a mid-download fetch failure.
type failingChunkReader struct {
	data        []byte
	etag        string
	failAtStart int64

	opens atomic.Int64
}

func (r *failingChunkReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	r.opens.Add(1)
	size := int64(len(r.data))
	o := client.NewRequestOptions(opts...)
	start, length, outcome := o.ResolveRange(size, r.etag)
	if outcome == client.RangePartial && start == r.failAtStart {
		return nil, nil, errors.New("boom")
	}
	headers := http.Header{}
	if outcome == client.RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, client.ErrRangeNotSatisfiable
	}
	headers.Set(client.ETagKey, r.etag)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	if outcome == client.RangePartial {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
}

func TestParallelGetClient(t *testing.T) {
	content := patternBytes(1000)
	etag := fakeETag(content)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/object/{namespace}/{key}", func(w http.ResponseWriter, r *http.Request) {
		if ifMatch := r.Header.Get("If-Match"); ifMatch != "" && ifMatch != etag {
			w.Header().Set(client.ETagKey, etag)
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}

		w.Header().Set(client.ETagKey, etag)
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
			return
		}

		start, end, ok := parseTestByteRange(rangeHeader)
		assert.True(t, ok)
		if start >= int64(len(content)) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(content)))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		end = min(end, int64(len(content)-1))
		body := content[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := client.New(srv.URL, nil).Namespace("test")
	defer c.Close()
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("parallel-client"), dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, content, dst.buf)
}

func parseTestByteRange(header string) (start, end int64, ok bool) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, false
	}
	startSpec, endSpec, ok := strings.Cut(spec, "-")
	if !ok || endSpec == "" {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(startSpec, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end, err = strconv.ParseInt(endSpec, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return start, end, true
}
