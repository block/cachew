package s3

import (
	"encoding/json"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/metadatadb"
)

// wireOp is the JSON encoding of a metadatadb.Op, discriminated by Op. There
// is no format version: evolution is handled by adding new discriminators.
type wireOp struct {
	Op     string `json:"op"`
	Key    string `json:"key"`
	MapKey any    `json:"mapKey,omitempty"`
	Member any    `json:"member,omitempty"`
	Value  any    `json:"value,omitempty"`
	// IntValue carries IntSet/IntMapSet values: riding the any-typed Value
	// would decode them as float64 and silently lose precision above 2^53.
	// MapKey/Member/Value still ride any — production keys are strings; a
	// new op type with an int64 payload needs its own field like this one.
	IntValue int64 `json:"intValue,omitempty"`
	Delta    int64 `json:"delta,omitempty"`
	Factor   int64 `json:"factor,omitempty"`
	Divisor  int64 `json:"divisor,omitempty"`
}

// segment is the body of a segment object: a group-committed batch of ops.
// A clock probe is a segment with zero ops.
type segment struct {
	Ops []wireOp `json:"ops"`
}

func marshalSegment(ops []metadatadb.Op) ([]byte, error) {
	seg := segment{Ops: make([]wireOp, 0, len(ops))}
	for _, o := range ops {
		w, err := encodeOp(o)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		seg.Ops = append(seg.Ops, w)
	}
	return errors.WithStack2(json.Marshal(seg))
}

func unmarshalSegment(data []byte) ([]metadatadb.Op, error) {
	var seg segment
	if err := json.Unmarshal(data, &seg); err != nil {
		return nil, errors.Wrap(err, "unmarshal segment")
	}
	ops := make([]metadatadb.Op, 0, len(seg.Ops))
	for _, w := range seg.Ops {
		o, err := decodeOp(w)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ops = append(ops, o)
	}
	return ops, nil
}

func encodeOp(o metadatadb.Op) (wireOp, error) {
	switch o := o.(type) {
	case metadatadb.ScalarSet:
		return wireOp{Op: "ScalarSet", Key: o.Key, Value: o.Value}, nil
	case metadatadb.ScalarDelete:
		return wireOp{Op: "ScalarDelete", Key: o.Key}, nil
	case metadatadb.IntSet:
		return wireOp{Op: "IntSet", Key: o.Key, IntValue: o.Value}, nil
	case metadatadb.IntAdd:
		return wireOp{Op: "IntAdd", Key: o.Key, Delta: o.Delta}, nil
	case metadatadb.IntMul:
		return wireOp{Op: "IntMul", Key: o.Key, Factor: o.Factor}, nil
	case metadatadb.IntDiv:
		return wireOp{Op: "IntDiv", Key: o.Key, Divisor: o.Divisor}, nil
	case metadatadb.SetAdd:
		return wireOp{Op: "SetAdd", Key: o.Key, Member: o.Member}, nil
	case metadatadb.SetRemove:
		return wireOp{Op: "SetRemove", Key: o.Key, Member: o.Member}, nil
	case metadatadb.IntMapSet:
		return wireOp{Op: "IntMapSet", Key: o.Key, MapKey: o.MapKey, IntValue: o.Value}, nil
	case metadatadb.IntMapAdd:
		return wireOp{Op: "IntMapAdd", Key: o.Key, MapKey: o.MapKey, Delta: o.Delta}, nil
	case metadatadb.IntMapMul:
		return wireOp{Op: "IntMapMul", Key: o.Key, MapKey: o.MapKey, Factor: o.Factor}, nil
	case metadatadb.IntMapDiv:
		return wireOp{Op: "IntMapDiv", Key: o.Key, MapKey: o.MapKey, Divisor: o.Divisor}, nil
	case metadatadb.IntMapDelete:
		return wireOp{Op: "IntMapDelete", Key: o.Key, MapKey: o.MapKey}, nil
	case metadatadb.MapSet:
		return wireOp{Op: "MapSet", Key: o.Key, MapKey: o.MapKey, Value: o.Value}, nil
	case metadatadb.MapDelete:
		return wireOp{Op: "MapDelete", Key: o.Key, MapKey: o.MapKey}, nil
	case metadatadb.ListAppend:
		return wireOp{Op: "ListAppend", Key: o.Key, Value: o.Value}, nil
	default:
		return wireOp{}, errors.Errorf("unsupported op type %T", o)
	}
}

func decodeOp(w wireOp) (metadatadb.Op, error) {
	switch w.Op {
	case "ScalarSet":
		return metadatadb.ScalarSet{Key: w.Key, Value: w.Value}, nil
	case "ScalarDelete":
		return metadatadb.ScalarDelete{Key: w.Key}, nil
	case "IntSet":
		return metadatadb.IntSet{Key: w.Key, Value: w.IntValue}, nil
	case "IntAdd":
		return metadatadb.IntAdd{Key: w.Key, Delta: w.Delta}, nil
	case "IntMul":
		return metadatadb.IntMul{Key: w.Key, Factor: w.Factor}, nil
	case "IntDiv":
		return metadatadb.IntDiv{Key: w.Key, Divisor: w.Divisor}, nil
	case "SetAdd":
		return metadatadb.SetAdd{Key: w.Key, Member: w.Member}, nil
	case "SetRemove":
		return metadatadb.SetRemove{Key: w.Key, Member: w.Member}, nil
	case "IntMapSet":
		return metadatadb.IntMapSet{Key: w.Key, MapKey: w.MapKey, Value: w.IntValue}, nil
	case "IntMapAdd":
		return metadatadb.IntMapAdd{Key: w.Key, MapKey: w.MapKey, Delta: w.Delta}, nil
	case "IntMapMul":
		return metadatadb.IntMapMul{Key: w.Key, MapKey: w.MapKey, Factor: w.Factor}, nil
	case "IntMapDiv":
		return metadatadb.IntMapDiv{Key: w.Key, MapKey: w.MapKey, Divisor: w.Divisor}, nil
	case "IntMapDelete":
		return metadatadb.IntMapDelete{Key: w.Key, MapKey: w.MapKey}, nil
	case "MapSet":
		return metadatadb.MapSet{Key: w.Key, MapKey: w.MapKey, Value: w.Value}, nil
	case "MapDelete":
		return metadatadb.MapDelete{Key: w.Key, MapKey: w.MapKey}, nil
	case "ListAppend":
		return metadatadb.ListAppend{Key: w.Key, Value: w.Value}, nil
	default:
		return nil, errors.Errorf("unknown op discriminator %q", w.Op)
	}
}

// mark is a rollup's high-water (LastModified, key) position in canonical
// order. The zero mark covers nothing: every real stamp exceeds it. LM must
// always be derived from LIST-returned values, never HEAD, so comparisons
// against listing stamps are exact.
type mark struct {
	LM  time.Time `json:"lm"`
	Key string    `json:"key"`
}

// covers reports whether (lm, key) is at or below the mark in canonical order.
func (m mark) covers(lm time.Time, key string) bool {
	if !lm.Equal(m.LM) {
		return lm.Before(m.LM)
	}
	return key <= m.Key
}

// rollupBody is the body of the rollup object: the replayed state of all
// folded segments plus the mark they are covered by.
type rollupBody struct {
	State json.RawMessage `json:"state"`
	Mark  mark            `json:"mark"`
}
