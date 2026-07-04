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
		// Callers commonly pass context.Background() (see api.go), so the
		// backend's own shutdown must unblock them. Prefer a reply that
		// raced shutdown: the write may be durable and applied, and must
		// not be misreported. A reply landing after this check still loses
		// — an accepted ambiguity during Close.
		select {
		case err := <-req.reply:
			return errors.WithStack(err)
		default:
		}
		return errors.New("backend closed")
	}
}

// writeLoop is the group-commit writer: at most one segment PUT in flight per
// namespace. There is no time window — requests arriving while a PUT is in
// flight queue on applyCh and are drained into the next batch when it
// completes, so batch size scales with load while object count stays flat.
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

// commit PUTs one segment carrying the batch, then applies it locally so
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
	// Key dedup against the committed cache only (staging is invisible
	// here): if a tick listed and committed this segment first, the served
	// state already contains its ops.
	if _, ok := n.cache[key]; ok {
		return nil
	}
	for _, o := range ops {
		metadatadb.ApplyOp(n.state, o)
	}
	n.cache[key] = &cacheEntry{ops: ops, insertedAt: time.Now()}
	return nil
}

// newSegmentKey returns a fresh segment object name. The UUIDv7 prefix makes
// lexicographic key order match generation order within this process
// (google/uuid guarantees strict per-process monotonicity), which is the
// canonical-order tiebreak for same-granule LastModified ties.
func newSegmentKey() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", errors.Wrap(err, "uuidv7")
	}
	return "segment-" + id.String() + ".json", nil
}
