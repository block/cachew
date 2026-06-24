package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
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
