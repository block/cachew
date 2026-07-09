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
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

var _ client.RangeReader = (*client.Client)(nil)

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
	data := []byte("0123456789")
	c := &noETagReader{data: data}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, data, dst.buf)
}

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

type openRecord struct {
	Range   string
	IfMatch string
}

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
	c := &changingSizeReader{discovery: make([]byte, 1000), rewritten: []byte("changed")}
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("k"), &dst, 100, 4)
	assert.NoError(t, err)
	assert.Equal(t, c.rewritten, dst.buf)
}

func TestParallelGetStream(t *testing.T) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 251)
	}

	tests := []struct {
		name        string
		reader      client.RangeReader
		concurrency int
		want        []byte
		wantErrIs   error
		wantErrText string
	}{
		{name: "MultiChunk", reader: &recordingReader{data: data, etag: `"v1"`}, concurrency: 4, want: data},
		{name: "SingleWorker", reader: &recordingReader{data: data, etag: `"v1"`}, concurrency: 1, want: data},
		{name: "NoETagFallback", reader: &noETagReader{data: data}, concurrency: 4, want: data},
		{name: "Empty", reader: &recordingReader{etag: `"v1"`}, concurrency: 4},
		{name: "ETagMismatch", reader: &rangeFlipReader{data: data, firstETag: `"v1"`, restETag: `"v2"`},
			concurrency: 4, wantErrText: "object changed during read"},
		{name: "PreconditionFailed", reader: &ifMatchFlipReader{data: data, firstETag: `"v1"`, restETag: `"v2"`},
			concurrency: 4, wantErrIs: client.ErrPreconditionFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := client.ParallelGetStream(context.Background(), tt.reader, client.NewKey("k"), &out, 100, tt.concurrency, t.TempDir())
			switch {
			case tt.wantErrIs != nil:
				assert.IsError(t, err, tt.wantErrIs)
			case tt.wantErrText != "":
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrText)
			default:
				assert.NoError(t, err)
				assert.Equal(t, tt.want, out.Bytes())
			}
		})
	}
}

type failingWriter struct{ err error }

func (f *failingWriter) Write([]byte) (int, error) { return 0, f.err }

func TestParallelGetStreamWriterError(t *testing.T) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	c := &recordingReader{data: data, etag: `"v1"`}
	sinkErr := errors.New("sink failed")
	err := client.ParallelGetStream(context.Background(), c, client.NewKey("k"), &failingWriter{err: sinkErr}, 100, 4, t.TempDir())
	assert.IsError(t, err, sinkErr)
}

func TestParallelGetClient(t *testing.T) {
	content := make([]byte, 1000)
	for i := range content {
		content[i] = byte(i % 251)
	}
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
	var dst bufferAt
	err := client.ParallelGet(context.Background(), c, client.NewKey("parallel-client"), &dst, 100, 4)
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
