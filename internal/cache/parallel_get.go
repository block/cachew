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
