package client

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
)

func TestSpillBufferReordersWrites(t *testing.T) {
	data := patternBytes(1 << 20)
	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- sb.streamTo(context.Background(), &out) }()

	const chunk = 64 << 10
	for _, off := range scrambledOffsets(len(data), chunk) {
		_, err := sb.WriteAt(data[off:off+chunk], int64(off))
		assert.NoError(t, err)
	}
	assert.NoError(t, sb.closeWrite(nil))
	assert.NoError(t, <-done)
	assert.Equal(t, data, out.Bytes())
}

func TestSpillBufferConcurrentWriters(t *testing.T) {
	const chunk = 256 << 10
	const workers = 8
	data := patternBytes(8 << 20)
	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	var out bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- sb.streamTo(context.Background(), &out) }()

	offsets := make(chan int, len(data)/chunk)
	for _, off := range scrambledOffsets(len(data), chunk) {
		offsets <- off
	}
	close(offsets)

	writeErrs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for off := range offsets {
				if _, err := sb.WriteAt(data[off:off+chunk], int64(off)); err != nil {
					writeErrs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(writeErrs)
	for err := range writeErrs {
		assert.NoError(t, err)
	}
	assert.NoError(t, sb.closeWrite(nil))
	assert.NoError(t, <-done)
	assert.Equal(t, data, out.Bytes())
}

func TestSpillBufferCoverageGap(t *testing.T) {
	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	_, err = sb.WriteAt([]byte("abc"), 10)
	assert.NoError(t, err)
	err = sb.closeWrite(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "coverage gap")
}

func TestSpillBufferStreamCancellation(t *testing.T) {
	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sb.streamTo(ctx, &bytes.Buffer{}) }()
	cancel()
	assert.IsError(t, <-done, context.Canceled)
}

func TestSpillBufferWriteErrorEndsStream(t *testing.T) {
	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	done := make(chan error, 1)
	go func() { done <- sb.streamTo(context.Background(), &bytes.Buffer{}) }()
	_, err = sb.WriteAt([]byte("prefix"), 0)
	assert.NoError(t, err)
	boom := errors.New("boom")
	assert.IsError(t, sb.closeWrite(boom), boom)
	assert.NoError(t, <-done)
}

// scrambledOffsets returns every chunk offset in [0, size) exactly once, out
// of order: a stride of 7 is coprime with any power-of-two chunk count, so
// the walk is a permutation.
func scrambledOffsets(size, chunk int) []int {
	numChunks := size / chunk
	offsets := make([]int, numChunks)
	for i := range numChunks {
		offsets[i] = ((i*7 + 3) % numChunks) * chunk
	}
	return offsets
}

func patternBytes(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}
