package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
)

func testPattern(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func TestStreamBufferRoundTrip(t *testing.T) {
	data := testPattern(1000)
	buf := newStreamBuffer(16, 2) // window 64, much smaller than the object
	read := make(chan []byte, 1)
	go func() {
		got, err := io.ReadAll(buf)
		assert.NoError(t, err)
		read <- got
	}()
	n, err := buf.WriteAt(data, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	buf.closeWrite(nil)
	assert.Equal(t, data, <-read)
}

func TestStreamBufferOutOfOrderChunks(t *testing.T) {
	data := testPattern(64)
	buf := newStreamBuffer(16, 2)
	for _, off := range []int64{48, 16, 32, 0} {
		_, err := buf.WriteAt(data[off:off+16], off)
		assert.NoError(t, err)
	}
	buf.closeWrite(nil)
	got, err := io.ReadAll(buf)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestStreamBufferBackpressure(t *testing.T) {
	buf := newStreamBuffer(16, 1) // window 32
	_, err := buf.WriteAt(make([]byte, 32), 0)
	assert.NoError(t, err)

	var wrote atomic.Bool
	unblocked := make(chan struct{})
	go func() {
		_, err := buf.WriteAt(make([]byte, 16), 32) // beyond the window: must block
		assert.NoError(t, err)
		wrote.Store(true)
		close(unblocked)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(t, wrote.Load())

	got := make([]byte, 16)
	_, err = io.ReadFull(buf, got)
	assert.NoError(t, err)
	select {
	case <-unblocked:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not unblock after reader progress")
	}
}

func TestStreamBufferPartialReadDoesNotFreeSlot(t *testing.T) {
	// A write one full ring ahead shares a page slot with the page currently
	// being read; it must stay blocked until that page is wholly consumed, or
	// it would overwrite the unread tail.
	data := testPattern(48)
	buf := newStreamBuffer(16, 1) // 2 pages: offset 32 reuses page 0's slot
	_, err := buf.WriteAt(data[:32], 0)
	assert.NoError(t, err)

	head := make([]byte, 4)
	_, err = io.ReadFull(buf, head) // partial read of page 0
	assert.NoError(t, err)

	var wrote atomic.Bool
	unblocked := make(chan struct{})
	go func() {
		_, err := buf.WriteAt(data[32:48], 32)
		assert.NoError(t, err)
		wrote.Store(true)
		close(unblocked)
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(t, wrote.Load(), "write into a partially read page's slot must block")

	rest := make([]byte, 12)
	_, err = io.ReadFull(buf, rest) // finish page 0: slot is now free
	assert.NoError(t, err)
	select {
	case <-unblocked:
	case <-time.After(5 * time.Second):
		t.Fatal("write did not unblock after the page was fully read")
	}
	buf.closeWrite(nil)
	tail, err := io.ReadAll(buf)
	assert.NoError(t, err)
	got := slices.Concat(head, rest, tail)
	assert.Equal(t, data, got)
}

func TestStreamBufferCoverageGap(t *testing.T) {
	buf := newStreamBuffer(16, 2)
	_, err := buf.WriteAt(make([]byte, 16), 32) // never write [0, 32)
	assert.NoError(t, err)
	buf.closeWrite(nil)
	_, err = io.ReadAll(buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "coverage gap")
}

func TestStreamBufferWriteErrorSurfacesAfterDrain(t *testing.T) {
	data := testPattern(16)
	buf := newStreamBuffer(16, 2)
	_, err := buf.WriteAt(data, 0)
	assert.NoError(t, err)
	buf.closeWrite(errors.New("boom"))
	got := make([]byte, 16)
	_, err = io.ReadFull(buf, got)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
	_, err = buf.Read(got)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestStreamBufferCloseUnblocksWriter(t *testing.T) {
	buf := newStreamBuffer(16, 1) // window 32
	_, err := buf.WriteAt(make([]byte, 32), 0)
	assert.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, err := buf.WriteAt(make([]byte, 16), 32)
		result <- err
	}()
	time.Sleep(20 * time.Millisecond)
	assert.NoError(t, buf.Close())
	select {
	case err := <-result:
		assert.IsError(t, err, io.ErrClosedPipe)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock the writer")
	}
}

// rangedReader serves correct byte ranges with an ETag, optionally delaying
// earlier offsets so chunks complete out of order, and optionally failing the
// chunk starting at failAt (-1 to disable).
type rangedReader struct {
	data   []byte
	delay  bool
	failAt int64
}

func (r *rangedReader) Open(_ context.Context, _ Key, opts ...RequestOption) (io.ReadCloser, http.Header, error) {
	const etag = `"v1"`
	size := int64(len(r.data))
	start, length, outcome := NewRequestOptions(opts...).ResolveRange(size, etag)
	headers := http.Header{}
	if outcome == RangeNotSatisfiable {
		headers.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		return nil, headers, ErrRangeNotSatisfiable
	}
	if outcome == RangePartial {
		if start == r.failAt {
			return nil, nil, errors.New("boom")
		}
		if r.delay {
			time.Sleep(time.Duration(size-start) / 100 * time.Millisecond)
		}
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size))
	}
	headers.Set(ETagKey, etag)
	headers.Set("Content-Length", strconv.FormatInt(length, 10))
	return io.NopCloser(bytes.NewReader(r.data[start : start+length])), headers, nil
}

func TestParallelGetIntoStreamBuffer(t *testing.T) {
	data := testPattern(10_000)
	c := &rangedReader{data: data, delay: true, failAt: -1}
	buf := newStreamBuffer(1000, 4)
	go func() {
		buf.closeWrite(ParallelGet(context.Background(), c, NewKey("k"), buf, 1000, 4))
	}()
	got, err := io.ReadAll(buf)
	assert.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestParallelGetIntoStreamBufferChunkFailure(t *testing.T) {
	// A failing chunk must not deadlock workers blocked in WriteAt: dispatch
	// run-ahead is bounded so their writes fit the window once the reader
	// drains to the gap, and the reader then surfaces the download error.
	data := testPattern(100_000)
	c := &rangedReader{data: data, failAt: 50_000}
	buf := newStreamBuffer(1000, 4)
	go func() {
		buf.closeWrite(ParallelGet(context.Background(), c, NewKey("k"), buf, 1000, 4))
	}()
	_, err := io.ReadAll(buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}
