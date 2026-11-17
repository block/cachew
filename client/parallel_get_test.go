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

var _ client.RangeReader = (*client.Client)(nil)

func patternBytes(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func collect(c client.RangeReader, key client.Key, chunkSize int64, concurrency int) ([]byte, error) {
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, key, dst, chunkSize, concurrency)
	return dst.buf, err
}

// rangeFlipReader reports a different ETag for chunks past the first, simulating a mid-download rewrite.
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

// ifMatchFlipReader fails pinned chunks with a 412, modelling If-Match enforcement on a mid-download rewrite.
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

// noETagReader serves byte ranges without an ETag, modelling a legacy entry or a RangeReader that omits it.
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

// changingSizeReader serves an ETag-less ranged body, then a different full body, modelling a rewrite between them.
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

// recordingReader serves byte ranges and records each Open's Range and If-Match, both empty for a full read.
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

func TestParallelGetReaderReassembly(t *testing.T) {
	data := patternBytes(10_000)
	c := &recordingReader{data: data, etag: `"v1"`}
	r, err := client.ParallelGetReader(context.Background(), c, client.NewKey("k"), 1000, 4)
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
	assert.Equal(t, data, got)
}

func TestParallelGetReaderZeroConcurrencySingleStream(t *testing.T) {
	data := patternBytes(1000)
	c := &recordingReader{data: data, etag: `"v1"`}
	r, err := client.ParallelGetReader(context.Background(), c, client.NewKey("k"), 100, 0)
	assert.NoError(t, err)
	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())
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

// faultyReader is a recordingReader that injects one queued fault per open at a given start offset.
//
// A fault is an HTTP status, or a body cut after some bytes with an error (io.EOF for a clean early end).
type faultyReader struct {
	recordingReader

	faultsMu sync.Mutex
	statuses map[int64][]int
	cuts     map[int64][]bodyCut
}

type bodyCut struct {
	after int64
	err   error
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func (f *faultyReader) Open(ctx context.Context, key client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	body, headers, err := f.recordingReader.Open(ctx, key, opts...)
	if err != nil {
		return nil, headers, err
	}
	start, _, _ := client.NewRequestOptions(opts...).ResolveRange(int64(len(f.data)), f.etag)
	f.faultsMu.Lock()
	defer f.faultsMu.Unlock()
	if statuses := f.statuses[start]; len(statuses) > 0 {
		f.statuses[start] = statuses[1:]
		return nil, nil, errors.Join(&client.HTTPStatusError{StatusCode: statuses[0]}, body.Close())
	}
	if cuts := f.cuts[start]; len(cuts) > 0 {
		f.cuts[start] = cuts[1:]
		return io.NopCloser(io.MultiReader(io.LimitReader(body, cuts[0].after), errReader{cuts[0].err})), headers, nil
	}
	return body, headers, nil
}

func (f *faultyReader) ranges() []openRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	opens := slices.Clone(f.opens)
	slices.SortFunc(opens, func(a, b openRecord) int { return strings.Compare(a.Range, b.Range) })
	return opens
}

func TestParallelGetResumesBrokenChunksFromTheirOffset(t *testing.T) {
	data := patternBytes(1000)
	c := &faultyReader{
		recordingReader: recordingReader{data: data, etag: `"v1"`},
		cuts: map[int64][]bodyCut{
			0:   {{after: 10, err: io.EOF}},
			300: {{after: 40, err: io.ErrUnexpectedEOF}},
			340: {{after: 5, err: io.ErrUnexpectedEOF}},
		},
	}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)

	opens := c.ranges()
	assert.SliceContains(t, opens, openRecord{Range: "bytes=10-99", IfMatch: `"v1"`})
	assert.SliceContains(t, opens, openRecord{Range: "bytes=340-399", IfMatch: `"v1"`})
	assert.SliceContains(t, opens, openRecord{Range: "bytes=345-399", IfMatch: `"v1"`})
	assert.Equal(t, 13, len(opens))
}

func TestParallelGetResumesSingleChunkObject(t *testing.T) {
	data := patternBytes(80)
	c := &faultyReader{
		recordingReader: recordingReader{data: data, etag: `"v1"`},
		cuts:            map[int64][]bodyCut{0: {{after: 30, err: io.ErrUnexpectedEOF}}},
	}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, []openRecord{{Range: "bytes=0-99"}, {Range: "bytes=30-79", IfMatch: `"v1"`}}, c.ranges())
}

func TestParallelGetRetriesServerErrors(t *testing.T) {
	data := patternBytes(1000)
	c := &faultyReader{
		recordingReader: recordingReader{data: data, etag: `"v1"`},
		statuses:        map[int64][]int{0: {http.StatusServiceUnavailable}, 500: {http.StatusBadGateway, http.StatusBadGateway}},
	}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, 13, len(c.ranges()))
}

func TestParallelGetGivesUpOnPersistentlyBrokenChunk(t *testing.T) {
	cut := bodyCut{after: 1, err: io.ErrUnexpectedEOF}
	c := &faultyReader{
		recordingReader: recordingReader{data: patternBytes(1000), etag: `"v1"`},
		cuts:            map[int64][]bodyCut{500: {cut}, 501: {cut}, 502: {cut}, 503: {cut}, 504: {cut}},
	}
	_, err := collect(c, client.NewKey("k"), 100, 4)
	assert.IsError(t, err, io.ErrUnexpectedEOF)
	assert.Contains(t, err.Error(), "write chunk at offset 503")
	assert.Equal(t, []bodyCut{cut}, c.cuts[504])
}

func TestParallelGetDoesNotRetryClientErrors(t *testing.T) {
	c := &faultyReader{
		recordingReader: recordingReader{data: patternBytes(1000), etag: `"v1"`},
		statuses:        map[int64][]int{500: {http.StatusForbidden}},
	}
	_, err := collect(c, client.NewKey("k"), 100, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected status code: 403")
	opens := 0
	for _, open := range c.ranges() {
		if open.Range == "bytes=500-599" {
			opens++
		}
	}
	assert.Equal(t, 1, opens)
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

// bufferAt is an in-memory io.WriterAt that extends like a file, zero-filling gaps.
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

// fullBodyOnChunkReader ranges the discovery chunk but returns the whole object, with Content-Range, for later ones.
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

// midflightRangeIgnoreReader ranges the discovery chunk but answers later ones with the full body and no range.
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

// ignoreRangeReader always returns the whole object with no Content-Range.
type ignoreRangeReader struct{ data []byte }

func (r *ignoreRangeReader) Open(_ context.Context, _ client.Key, _ ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	headers := http.Header{}
	headers.Set("Content-Length", strconv.Itoa(len(r.data)))
	return io.NopCloser(bytes.NewReader(r.data)), headers, nil
}

// reorderReader delays earlier offsets longer than later ones so chunks complete out of order.
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

// failingChunkReader errors when a requested range starts at failAtStart, modelling a mid-download failure.
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
