package cache

import (
	"context"
	"io"
	"sync"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
)

const (
	// s3DownloadChunkSize is the size of each parallel range-GET request.
	// 32 MiB matches the gradle-cache-tool's benchmarked default.
	s3DownloadChunkSize = 32 << 20
	// s3DownloadWorkers is the number of concurrent range-GET requests.
	// Benchmarking showed no throughput difference from 4 to 128 workers
	// (extraction IOPS is the bottleneck), so 8 keeps connection count low.
	s3DownloadWorkers = 8
)

// parallelGetReader returns an io.ReadCloser that downloads the S3 object
// using parallel range-GET requests and reassembles chunks in order.
// For objects smaller than one chunk, it falls back to a single GetObject.
func (s *S3) parallelGetReader(ctx context.Context, bucket, objectName string, size int64) (io.ReadCloser, error) {
	if size <= s3DownloadChunkSize {
		// Small object: single stream.
		obj, err := s.client.GetObject(ctx, bucket, objectName, minio.GetObjectOptions{})
		if err != nil {
			return nil, errors.Errorf("failed to get object: %w", err)
		}
		return &s3Reader{obj: obj}, nil
	}

	// Large object: parallel range requests reassembled in order via io.Pipe.
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(s.parallelGet(ctx, bucket, objectName, size, pw))
	}()
	return pr, nil
}

// parallelGet downloads an S3 object in parallel chunks and writes them in
// order to w. Each worker downloads its chunk into memory so the TCP
// connection stays active at full speed. Peak memory: numWorkers × chunkSize.
func (s *S3) parallelGet(ctx context.Context, bucket, objectName string, size int64, w io.Writer) error {
	numChunks := int((size + s3DownloadChunkSize - 1) / s3DownloadChunkSize)
	numWorkers := min(s3DownloadWorkers, numChunks)

	type chunkResult struct {
		data []byte
		err  error
	}

	// One buffered channel per chunk so workers never block after reading.
	results := make([]chan chunkResult, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}

	// Work queue of chunk indices.
	work := make(chan int, numChunks)
	for i := range numChunks {
		work <- i
	}
	close(work)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			for seq := range work {
				start := int64(seq) * s3DownloadChunkSize
				end := min(start+s3DownloadChunkSize-1, size-1)

				opts := minio.GetObjectOptions{}
				if err := opts.SetRange(start, end); err != nil {
					results[seq] <- chunkResult{err: errors.Errorf("set range %d-%d: %w", start, end, err)}
					continue
				}

				obj, err := s.client.GetObject(ctx, bucket, objectName, opts)
				if err != nil {
					results[seq] <- chunkResult{err: errors.Errorf("get range %d-%d: %w", start, end, err)}
					continue
				}

				// Drain the body immediately so the TCP connection stays at
				// full speed. All workers do this concurrently, saturating
				// the available S3 bandwidth.
				data, readErr := io.ReadAll(obj)
				obj.Close() //nolint:errcheck,gosec
				results[seq] <- chunkResult{data: data, err: readErr}
			}
		})
	}

	// Write chunks in order. Each receive blocks until that chunk's worker
	// finishes, while other workers continue downloading concurrently.
	var writeErr error
	for _, ch := range results {
		r := <-ch
		if writeErr != nil {
			continue // drain remaining channels so goroutines can exit
		}
		if r.err != nil {
			writeErr = r.err
			continue
		}
		if _, err := w.Write(r.data); err != nil {
			writeErr = err
		}
	}

	wg.Wait()
	return writeErr
}
