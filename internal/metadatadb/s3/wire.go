package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/metadatadb"
)

// unmarshalNumeric decodes with UseNumber: a plain unmarshal coerces JSON
// numbers in any-typed destinations to float64, corrupting int64 values
// above 2^53.
func unmarshalNumeric(data []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return errors.WithStack(dec.Decode(target))
}

func unmarshalState(data []byte) (map[string]any, error) {
	state := make(map[string]any)
	if err := unmarshalNumeric(data, &state); err != nil {
		return nil, errors.Wrap(err, "unmarshal state")
	}
	// JSON null decodes by setting the map to nil without error; accepting
	// it would panic later on the first write into the state.
	if state == nil {
		return nil, errors.New("state is null")
	}
	return state, nil
}

// There is no wire format version: evolution is handled by adding new op
// types here, keyed by Go type name.
//
//nolint:gochecknoglobals
var opTypes = func() map[string]reflect.Type {
	types := make(map[string]reflect.Type)
	for _, o := range []metadatadb.Op{
		metadatadb.ScalarSet{}, metadatadb.ScalarDelete{},
		metadatadb.IntSet{}, metadatadb.IntAdd{}, metadatadb.IntMul{}, metadatadb.IntDiv{},
		metadatadb.SetAdd{}, metadatadb.SetRemove{},
		metadatadb.IntMapSet{}, metadatadb.IntMapAdd{}, metadatadb.IntMapMul{},
		metadatadb.IntMapDiv{}, metadatadb.IntMapDelete{},
		metadatadb.MapSet{}, metadatadb.MapDelete{},
		metadatadb.ListAppend{},
	} {
		t := reflect.TypeOf(o)
		types[t.Name()] = t
	}
	return types
}()

// segment is a group-committed batch of ops; a clock probe has zero ops.
type segment struct {
	Ops []json.RawMessage `json:"ops"`
}

func marshalSegment(ops []metadatadb.Op) ([]byte, error) {
	seg := segment{Ops: make([]json.RawMessage, 0, len(ops))}
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

// encodeOp splices the discriminator in rather than round-tripping through a
// map, which would coerce int64 fields to float64 above 2^53.
func encodeOp(o metadatadb.Op) (json.RawMessage, error) {
	name := reflect.TypeOf(o).Name()
	if _, ok := opTypes[name]; !ok {
		return nil, errors.Errorf("unregistered op type %T", o)
	}
	data, err := json.Marshal(o)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal %s", name)
	}
	if string(data) == "{}" {
		return json.RawMessage(fmt.Sprintf(`{"op":%q}`, name)), nil
	}
	head := fmt.Sprintf(`{"op":%q,`, name)
	return json.RawMessage(head + string(data[1:])), nil
}

func decodeOp(data json.RawMessage) (metadatadb.Op, error) {
	var head struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, errors.Wrap(err, "unmarshal op discriminator")
	}
	typ, ok := opTypes[head.Op]
	if !ok {
		return nil, errors.Errorf("unknown op discriminator %q", head.Op)
	}
	v := reflect.New(typ)
	if err := unmarshalNumeric(data, v.Interface()); err != nil {
		return nil, errors.Wrapf(err, "unmarshal %s", head.Op)
	}
	op, ok := v.Elem().Interface().(metadatadb.Op)
	if !ok {
		return nil, errors.Errorf("%s does not implement Op", head.Op)
	}
	return op, nil
}

// mark is a rollup's high-water (LastModified, key) position in canonical
// order; the zero mark covers nothing. LM must always derive from
// LIST-returned values, never HEAD, so comparisons against listing stamps
// are exact.
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

// rollupBody is the replayed state of all folded segments plus their mark.
type rollupBody struct {
	State json.RawMessage `json:"state"`
	Mark  mark            `json:"mark"`
}
