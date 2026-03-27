package metadatadb

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alecthomas/errors"
)

// Scalar ops

type scalarSetOp[V any] struct {
	name  string
	value V
}

func (o *scalarSetOp[V]) apply(state map[string]any) { state[o.name] = o.value }

type scalarDeleteOp struct{ name string }

func (o *scalarDeleteOp) apply(state map[string]any) { delete(state, o.name) }

// Int ops

type intSetOp struct {
	name  string
	value int64
}

func (o *intSetOp) apply(state map[string]any) { state[o.name] = o.value }

type intAddOp struct {
	name  string
	delta int64
}

func (o *intAddOp) apply(state map[string]any) {
	state[o.name] = toInt64(state[o.name]) + o.delta
}

type intMulOp struct {
	name   string
	factor int64
}

func (o *intMulOp) apply(state map[string]any) {
	state[o.name] = toInt64(state[o.name]) * o.factor
}

type intDivOp struct {
	name    string
	divisor int64
}

func (o *intDivOp) apply(state map[string]any) {
	if o.divisor == 0 {
		return
	}
	state[o.name] = toInt64(state[o.name]) / o.divisor
}

// Set ops — stored as map[string]any keyed by JSON-marshaled members.

type setAddOp[V comparable] struct {
	name   string
	member V
}

func (o *setAddOp[V]) apply(state map[string]any) {
	m, ok := state[o.name].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[o.name] = m
	}
	m[marshalKey(o.member)] = true
}

type setRemoveOp[V comparable] struct {
	name   string
	member V
}

func (o *setRemoveOp[V]) apply(state map[string]any) {
	if m, ok := state[o.name].(map[string]any); ok {
		delete(m, marshalKey(o.member))
	}
}

// IntMap ops — stored as map[string]any with int64 values.

type intMapSetOp[K comparable] struct {
	name  string
	key   K
	value int64
}

func (o *intMapSetOp[K]) apply(state map[string]any) {
	m, ok := state[o.name].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[o.name] = m
	}
	m[marshalKey(o.key)] = o.value
}

type intMapAddOp[K comparable] struct {
	name  string
	key   K
	delta int64
}

func (o *intMapAddOp[K]) apply(state map[string]any) {
	m, ok := state[o.name].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[o.name] = m
	}
	k := marshalKey(o.key)
	m[k] = toInt64(m[k]) + o.delta
}

type intMapMulOp[K comparable] struct {
	name   string
	key    K
	factor int64
}

func (o *intMapMulOp[K]) apply(state map[string]any) {
	m, ok := state[o.name].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[o.name] = m
	}
	k := marshalKey(o.key)
	m[k] = toInt64(m[k]) * o.factor
}

type intMapDivOp[K comparable] struct {
	name    string
	key     K
	divisor int64
}

func (o *intMapDivOp[K]) apply(state map[string]any) {
	if o.divisor == 0 {
		return
	}
	m, ok := state[o.name].(map[string]any)
	if !ok {
		return
	}
	k := marshalKey(o.key)
	m[k] = toInt64(m[k]) / o.divisor
}

type intMapDeleteOp[K comparable] struct {
	name string
	key  K
}

func (o *intMapDeleteOp[K]) apply(state map[string]any) {
	if m, ok := state[o.name].(map[string]any); ok {
		delete(m, marshalKey(o.key))
	}
}

// Map ops — stored as map[string]any keyed by JSON-marshaled keys.

type mapSetOp[K comparable, V any] struct {
	name  string
	key   K
	value V
}

func (o *mapSetOp[K, V]) apply(state map[string]any) {
	m, ok := state[o.name].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[o.name] = m
	}
	m[marshalKey(o.key)] = o.value
}

type mapDeleteOp[K comparable] struct {
	name string
	key  K
}

func (o *mapDeleteOp[K]) apply(state map[string]any) {
	if m, ok := state[o.name].(map[string]any); ok {
		delete(m, marshalKey(o.key))
	}
}

// List ops — stored as []any

type listAppendOp[V any] struct {
	name  string
	value V
}

func (o *listAppendOp[V]) apply(state map[string]any) {
	s, _ := state[o.name].([]any)
	state[o.name] = append(s, any(o.value))
}

// Sync

func (n *Namespace) doSync(ctx context.Context) error {
	n.syncMu.Lock()
	defer n.syncMu.Unlock()

	n.mu.Lock()
	pending := n.pending
	n.pending = nil
	n.mu.Unlock()

	hasPending := len(pending) > 0
	if hasPending {
		if err := n.store.backend.Lock(ctx, n.name); err != nil {
			n.restorePending(pending)
			return errors.Wrap(err, "lock namespace")
		}
		defer func() {
			if err := n.store.backend.Unlock(ctx, n.name); err != nil {
				n.store.logger.WarnContext(ctx, "unlock failed", "namespace", n.name, "error", err)
			}
		}()
	}

	remote, err := n.loadReplayStore(ctx, pending)
	if err != nil {
		n.restorePending(pending)
		return err
	}

	n.mu.Lock()
	n.state = remote
	for _, o := range n.pending {
		o.apply(n.state)
	}
	n.mu.Unlock()

	return nil
}

const maxTokenRetries = 3

// loadReplayStore loads remote state, replays ops, and stores the result.
// Retries the full cycle on ErrInvalidToken.
func (n *Namespace) loadReplayStore(ctx context.Context, pending []op) (map[string]any, error) {
	for range maxTokenRetries {
		remote, err := n.tryLoadReplayStore(ctx, pending)
		if errors.Is(err, ErrInvalidToken) {
			continue
		}
		return remote, err
	}
	return nil, errors.New("max token retries exceeded")
}

func (n *Namespace) tryLoadReplayStore(ctx context.Context, pending []op) (map[string]any, error) {
	data, token, err := n.store.backend.Load(ctx, n.name)
	if err != nil {
		return nil, errors.Wrap(err, "load namespace")
	}

	remote := make(map[string]any)
	if data != nil {
		if err := json.Unmarshal(data, &remote); err != nil {
			return nil, errors.Wrap(err, "unmarshal state")
		}
	}

	for _, o := range pending {
		o.apply(remote)
	}

	if len(pending) > 0 {
		merged, err := json.Marshal(remote)
		if err != nil {
			return nil, errors.Wrap(err, "marshal state")
		}
		if err := n.store.backend.Store(ctx, n.name, merged, token); err != nil {
			return nil, errors.Wrap(err, "store namespace")
		}
	}

	return remote, nil
}

func (n *Namespace) restorePending(ops []op) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pending = append(ops, n.pending...)
}

func (n *Namespace) syncLoop() {
	defer close(n.done)
	logger := n.store.logger.With("namespace", n.name)
	ticker := time.NewTicker(n.store.config.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.store.ctx.Done():
			return
		case <-ticker.C:
			if err := n.doSync(n.store.ctx); err != nil {
				logger.WarnContext(n.store.ctx, "sync failed", "error", err)
			}
		}
	}
}
