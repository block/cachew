package cache

import (
	"context"
	"io"
	"net/http"
	"strconv"

	"github.com/alecthomas/errors"
	"golang.org/x/sync/errgroup"
)

// ParallelGet streams an object from any Range-capable Cache by fetching it in
// chunkSize-byte chunks concurrently (up to concurrency requests in flight) and
// reassembling them in order. It generalises the S3 backend's parallel download
// to any Cache implementation: latency-bound backends (e.g. a remote cache) can
// saturate bandwidth with overlapping ranged reads.
//
// It Stats key for the object size, then issues ranged Opens. Objects of at
// most one chunk, or a concurrency below two, are served by a single Open. The
// returned headers are those of the Stat; the returned reader must be closed.
//
// Peak memory is bounded by concurrency*chunkSize, as each in-flight chunk is
// buffered until it can be written in order.
func ParallelGet(ctx context.Context, c Cache, key Key, chunkSize int64, concurrency int) (io.ReadCloser, http.Header, error) {
	headers, err := c.Stat(ctx, key)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	size, err := strconv.ParseInt(headers.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, nil, errors.Errorf("parallel get %s: invalid Content-Length: %w", key, err)
	}

	if concurrency <= 1 || chunkSize <= 0 || size <= chunkSize {
		rc, _, err := c.Open(ctx, key)
		return rc, headers, errors.WithStack(err)
	}

	// Reassemble chunks in order through a pipe. A cancellable context stops the
	// background workers promptly if the consumer disconnects or a read fails.
	dlCtx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	go func() {
		err := parallelGetChunks(dlCtx, c, key, size, chunkSize, concurrency, pw)
		cancel()
		pw.CloseWithError(err)
	}()
	return &cancelReadCloser{ReadCloser: pr, cancel: cancel}, headers, nil
}

// parallelGetChunks fetches the object in order-preserving chunks and writes
// them to w. Workers fetch concurrently into per-chunk buffers; a single writer
// drains them in sequence. An errgroup cancels all workers on the first error.
func parallelGetChunks(ctx context.Context, c Cache, key Key, size, chunkSize int64, concurrency int, w io.Writer) error {
	numChunks := int((size + chunkSize - 1) / chunkSize)
	numWorkers := min(concurrency, numChunks)

	// One buffered slot per chunk so a worker never blocks after producing.
	results := make([]chan []byte, numChunks)
	for i := range results {
		results[i] = make(chan []byte, 1)
	}

	work := make(chan int, numChunks)
	for i := range numChunks {
		work <- i
	}
	close(work)

	eg, egCtx := errgroup.WithContext(ctx)

	for range numWorkers {
		eg.Go(func() error {
			for seq := range work {
				if egCtx.Err() != nil {
					return egCtx.Err()
				}

				start := int64(seq) * chunkSize
				end := min(start+chunkSize, size)

				rc, _, err := c.Open(egCtx, key, Range(start, end))
				if err != nil {
					return errors.Errorf("open range %d-%d: %w", start, end, err)
				}
				data, readErr := io.ReadAll(rc)
				rc.Close() //nolint:errcheck,gosec
				if readErr != nil {
					return errors.Wrap(readErr, "read chunk")
				}
				results[seq] <- data
			}
			return nil
		})
	}

	// Write chunks in order; a write error cancels egCtx, stopping the workers.
	eg.Go(func() error {
		for _, ch := range results {
			select {
			case data := <-ch:
				if _, err := w.Write(data); err != nil {
					return errors.Wrap(err, "write chunk")
				}
			case <-egCtx.Done():
				return egCtx.Err()
			}
		}
		return nil
	})

	return errors.Wrap(eg.Wait(), "parallel get")
}
