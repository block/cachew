package cache

import (
	"context"
	"io"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// ParallelGet downloads an object from any Range-capable Cache into dst,
// fetching it in chunkSize-byte chunks concurrently. It delegates to
// [client.ParallelGet]; see that function for the full semantics.
func ParallelGet(ctx context.Context, c Cache, key Key, dst io.WriterAt, chunkSize int64, concurrency int) error {
	return errors.WithStack(client.ParallelGet(ctx, c, key, dst, chunkSize, concurrency))
}

// ParallelGetStream downloads an object from any Range-capable Cache into w
// as a sequential byte stream, reordering concurrently fetched chunks through
// a spill file in spillDir. It delegates to [client.ParallelGetStream]; see
// that function for the full semantics.
func ParallelGetStream(ctx context.Context, c Cache, key Key, w io.Writer, chunkSize int64, concurrency int, spillDir string) error {
	return errors.WithStack(client.ParallelGetStream(ctx, c, key, w, chunkSize, concurrency, spillDir))
}
