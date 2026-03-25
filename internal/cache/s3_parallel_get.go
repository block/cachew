package cache

import (
	"context"
	"io"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"golang.org/x/sync/errgroup"
)

const (
	// s3DownloadChunkSize is the size of each parallel range-GET request.
	// 32 MiB matches the gradle-cache-tool's benchmarked default.
	s3DownloadChunkSize = 32 << 20
	// s3DownloadWorkers is the number of concurrent range-GET requests.
	// 8 workers should be enough to saturate the host's network connection.
	s3DownloadWorkers = 8
)

// parallelGetReader returns an io.ReadCloser that downloads the S3 object
// using parallel range-GET requests and reassembles chunks in order.
// For objects smaller than one chunk, it falls back to a single GetObject.
// The etag pins all chunk requests to one object revision, preventing
// corruption if the key is overwritten during a large read.
func (s *S3) parallelGetReader(ctx context.Context, bucket, objectName string, size int64, etag string) (io.ReadCloser, error) {
	if size <= s3DownloadChunkSize {
		// Small object: single stream.
		obj, err := s.client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
		if err != nil {
			return nil, errors.Errorf("failed to get object: %w", err)
		}
		return &s3Reader{obj: obj}, nil
	}

	// Large object: parallel range requests reassembled in order via io.Pipe.
	// Use a cancellable context so workers stop promptly if the consumer
	// disconnects or a write error occurs.
	dlCtx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	go func() {
		err := s.parallelGet(dlCtx, bucket, objectName, size, etag, pw)
		cancel()
		pw.CloseWithError(err)
	}()
	return &cancelReadCloser{ReadCloser: pr, cancel: cancel}, nil
}

// cancelReadCloser wraps an io.ReadCloser and cancels a context on Close,
// ensuring background goroutines are cleaned up when the consumer is done.
type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	c.cancel()
	return errors.Wrap(c.ReadCloser.Close(), "close parallel get reader")
}

// parallelGet downloads an S3 object in parallel chunks and writes them in
// order to w. Each worker downloads its chunk into memory so the TCP
// connection stays active at full speed. Peak memory: numWorkers × chunkSize.
// All chunk requests are pinned to the given etag to ensure consistency.
// An errgroup cancels all workers on the first error from any goroutine.
func (s *S3) parallelGet(ctx context.Context, bucket, objectName string, size int64, etag string, w io.Writer) error {
	numChunks := int((size + s3DownloadChunkSize - 1) / s3DownloadChunkSize)
	numWorkers := min(s3DownloadWorkers, numChunks)

	// One buffered channel per chunk so workers never block after sending.
	results := make([]chan []byte, numChunks)
	for i := range results {
		results[i] = make(chan []byte, 1)
	}

	// Work queue of chunk indices.
	work := make(chan int, numChunks)
	for i := range numChunks {
		work <- i
	}
	close(work)

	eg, egCtx := errgroup.WithContext(ctx)

	// Download workers: fetch chunks concurrently and send data on success,
	// or return an error which cancels all other workers via egCtx.
	for range numWorkers {
		eg.Go(func() error {
			for seq := range work {
				if egCtx.Err() != nil {
					return egCtx.Err()
				}

				start := int64(seq) * s3DownloadChunkSize
				end := min(start+s3DownloadChunkSize-1, size-1)

				opts := minio.GetObjectOptions{}
				if err := opts.SetRange(start, end); err != nil {
					return errors.Errorf("set range %d-%d: %w", start, end, err)
				}
				// Pin to the object revision from the initial stat to prevent
				// reading a mix of old and new data if the key is overwritten.
				if err := opts.SetMatchETag(etag); err != nil {
					return errors.Errorf("set etag %s: %w", etag, err)
				}

				obj, err := s.client.GetObject(egCtx, bucket, objectName, opts)
				if err != nil {
					return errors.Errorf("get range %d-%d: %w", start, end, err)
				}

				// Drain the body immediately so the TCP connection stays at
				// full speed. All workers do this concurrently, saturating
				// the available S3 bandwidth.
				data, readErr := io.ReadAll(obj)
				obj.Close() //nolint:errcheck,gosec
				if readErr != nil {
					return errors.Wrap(readErr, "read chunk")
				}
				results[seq] <- data
			}
			return nil
		})
	}

	// Write chunks in order. Runs in the errgroup so that a write error
	// cancels egCtx, which stops download workers promptly.
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
