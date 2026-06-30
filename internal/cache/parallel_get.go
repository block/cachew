package cache

import (
	"context"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

// ParallelGet downloads an object from any Range-capable Cache, fetching it in
// chunkSize-byte chunks concurrently and handing each to sink. It delegates to
// [client.ParallelGet]; see that function for the full semantics.
func ParallelGet(ctx context.Context, c Cache, key Key, sink client.ChunkSink, chunkSize int64, concurrency int) error {
	return errors.WithStack(client.ParallelGet(ctx, c, key, sink, chunkSize, concurrency))
}
