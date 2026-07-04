package s3

import (
	"bytes"
	"context"
	"time"

	"github.com/alecthomas/errors"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"

	"github.com/block/cachew/internal/metadatadb"
)

type applyReq struct {
	ops   []metadatadb.Op
	reply chan error
}

func (n *namespace) apply(ctx context.Context, ops []metadatadb.Op) error {
	req := &applyReq{ops: ops, reply: make(chan error, 1)}
	select {
	case n.applyCh <- req:
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-n.b.ctx.Done():
		return errors.New("backend closed")
	}
	select {
	case err := <-req.reply:
		return errors.WithStack(err)
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-n.b.ctx.Done():
		// Callers pass context.Background() (api.go), so shutdown must
		// unblock them — but prefer a raced reply: the write may be durable.
		select {
		case err := <-req.reply:
			return errors.WithStack(err)
		default:
		}
		return errors.New("backend closed")
	}
}

// writeLoop is the group-commit writer: one PUT in flight; requests arriving
// during a flight queue on applyCh and drain into the next batch.
func (n *namespace) writeLoop() {
	defer n.b.wg.Done()
	for {
		var reqs []*applyReq
		select {
		case req := <-n.applyCh:
			reqs = append(reqs, req)
		case <-n.b.ctx.Done():
			// Drain requests stranded in the buffer so no caller hangs.
			for {
				select {
				case req := <-n.applyCh:
					req.reply <- errors.New("backend closed")
				default:
					return
				}
			}
		}
	drain:
		for {
			select {
			case req := <-n.applyCh:
				reqs = append(reqs, req)
			default:
				break drain
			}
		}
		err := n.commit(reqs)
		for _, req := range reqs {
			req.reply <- err
		}
	}
}

// commit PUTs the batch as one segment, then applies it locally so
// read-your-own-writes holds when the callers unblock.
func (n *namespace) commit(reqs []*applyReq) error {
	var ops []metadatadb.Op
	for _, req := range reqs {
		ops = append(ops, req.ops...)
	}
	data, err := marshalSegment(ops)
	if err != nil {
		return errors.Wrap(err, "marshal segment")
	}
	key, err := newSegmentKey()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx, cancel := context.WithTimeout(n.b.ctx, putTimeout)
	defer cancel()
	_, err = n.b.client.PutObject(ctx, n.b.bucket, n.prefix()+key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return errors.Wrap(err, "put segment")
	}

	n.stateMu.Lock()
	defer n.stateMu.Unlock()
	// Dedup against the committed cache only, never staging: committed
	// means the served state already contains the ops.
	if _, ok := n.cache[key]; ok {
		return nil
	}
	for _, o := range ops {
		metadatadb.ApplyOp(n.state, o)
	}
	n.cache[key] = &cacheEntry{ops: ops, insertedAt: time.Now()}
	return nil
}

// newSegmentKey relies on google/uuid's strict per-process V7 monotonicity:
// key order is the canonical-order tiebreak within a LastModified granule.
func newSegmentKey() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", errors.Wrap(err, "uuidv7")
	}
	return "segment-" + id.String() + ".json", nil
}
