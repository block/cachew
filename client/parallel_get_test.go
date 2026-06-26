package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
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

// collect runs ParallelGet into a StreamSink and returns the reassembled bytes,
// reading the sink concurrently as the engine requires. The download error (if
// any) takes precedence over the read error.
func collect(c client.RangeReader, key client.Key, chunkSize int64, concurrency int) ([]byte, error) {
	sink := client.NewStreamSink(chunkSize, concurrency)
	type result struct {
		data []byte
		err  error
	}
	rc := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(sink)
		rc <- result{data: data, err: err}
	}()
	err := client.ParallelGet(context.Background(), c, key, sink, chunkSize, concurrency)
	sink.Done(err)
	res := <-rc
	if err != nil {
		return res.data, err
	}
	return res.data, res.err
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
	data := patternBytes(1000)
	c := &noETagReader{data: data}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetNoETagSingleChunk(t *testing.T) {
	// A no-ETag object delivered entirely by the discovery request is a single
	// revision, so it succeeds without pinning.
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
	// A no-ETag multi-chunk object falls back to a single full read. If it is
	// rewritten to a different size between discovery and that read, the
	// discovery total must not be used to validate the full body: the full read
	// is itself a consistent revision and should be accepted in its entirety.
	c := &changingSizeReader{discovery: make([]byte, 1000), rewritten: []byte("changed")}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, c.rewritten, got)
}

// recordingReader serves byte ranges and records the Range option of every
// Open call ("" for a full, non-ranged read), so tests can assert how the
// object was fetched.
type recordingReader struct {
	data []byte
	etag string

	mu    sync.Mutex
	opens []string
}

func (r *recordingReader) Open(_ context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	o := client.NewRequestOptions(opts...)
	r.mu.Lock()
	r.opens = append(r.opens, o.Range)
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
	// A multi-chunk object must be emitted to the writer as the original,
	// in-order byte stream despite being fetched concurrently.
	data := patternBytes(10_000)
	c := &recordingReader{data: data, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetSingleWorkerFullRead(t *testing.T) {
	// A concurrency of 1 gains nothing from chunking, so it must issue a single
	// non-ranged read rather than discovering and serialising ranged GETs.
	data := patternBytes(1000)
	c := &recordingReader{data: data, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 100, 1)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, []string{""}, c.opens)
}

func TestParallelGetEmptyObject(t *testing.T) {
	c := &recordingReader{data: nil, etag: `"v1"`}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(got))
}

func TestParallelGetServerIgnoresRange(t *testing.T) {
	// A backend that ignores the range header delivers the whole object on the
	// discovery request; it must be streamed in full.
	data := patternBytes(1000)
	c := &ignoreRangeReader{data: data}
	got, err := collect(c, client.NewKey("k"), 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetOutOfOrderCompletion(t *testing.T) {
	// Chunks deliberately complete in reverse order within the in-flight window;
	// the writer must still emit a correctly ordered stream.
	data := patternBytes(10_000)
	c := &reorderReader{data: data, etag: `"v1"`, chunkSize: 1000}
	got, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetPropagatesOpenError(t *testing.T) {
	// An error opening a non-first chunk must surface and cancel the download.
	c := &failingChunkReader{data: patternBytes(10_000), etag: `"v1"`, failAtStart: 5000}
	_, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestParallelGetRejectsOverlongChunk(t *testing.T) {
	// A backend that honours the discovery range but ignores a later chunk's
	// range — returning the whole object with the same ETag — must be detected
	// rather than emitting truncated, mis-aligned bytes.
	c := &fullBodyOnChunkReader{data: patternBytes(10_000), etag: `"v1"`}
	_, err := collect(c, client.NewKey("k"), 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "more than the expected 1000 bytes")
}

func TestParallelGetWriterAtReassembly(t *testing.T) {
	// A DiskSink scatters chunks to their offsets via concurrent WriteAt; a
	// multi-chunk object must still reassemble correctly.
	data := patternBytes(10_000)
	c := &recordingReader{data: data, etag: `"v1"`}
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), client.DiskSink{W: dst}, 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
}

func TestParallelGetWriterAtOutOfOrder(t *testing.T) {
	// Chunks complete out of order; DiskSink places each at its offset, so the
	// result is correct with no reordering needed.
	data := patternBytes(10_000)
	c := &reorderReader{data: data, etag: `"v1"`, chunkSize: 1000}
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), client.DiskSink{W: dst}, 1000, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
}

func TestParallelGetWriterAtRejectsOverlongChunk(t *testing.T) {
	// The overlong-chunk guard must hold on the DiskSink path too.
	c := &fullBodyOnChunkReader{data: patternBytes(10_000), etag: `"v1"`}
	dst := &bufferAt{}
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), client.DiskSink{W: dst}, 1000, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "more than the expected 1000 bytes")
}

// bufferAt is an in-memory io.WriterAt that extends like a file, zero-filling
// gaps, so tests can exercise DiskSink's concurrent WriteAt path without
// touching disk.
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
