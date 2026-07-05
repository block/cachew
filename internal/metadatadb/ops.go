package metadatadb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alecthomas/errors"
)

// Op is a write operation applied to the metadata store. Concrete types form a
// closed set (sum type) — backends handle each variant via exhaustive type switch.
//
//sumtype:decl
type Op interface {
	op() // private marker
}

// ReadOp is a read query against the metadata store. Like Op, concrete types
// form a closed set dispatched by the backend.
//
//sumtype:decl
type ReadOp interface {
	readOp() // private marker
}

// Scalar ops

// ScalarSet sets a scalar value. Last write wins.
type ScalarSet struct {
	Key   string
	Value any
}

func (ScalarSet) op() {}

// ScalarDelete removes a scalar value.
type ScalarDelete struct{ Key string }

func (ScalarDelete) op() {}

// ScalarGet reads a scalar value. Returns (value, true) or (nil, false).
type ScalarGet struct{ Key string }

func (ScalarGet) readOp() {}

// Int ops

// IntSet sets an integer to an exact value.
type IntSet struct {
	Key   string
	Value int64
}

func (IntSet) op() {}

// IntAdd adds a delta to an integer.
type IntAdd struct {
	Key   string
	Delta int64
}

func (IntAdd) op() {}

// IntMul multiplies an integer by a factor.
type IntMul struct {
	Key    string
	Factor int64
}

func (IntMul) op() {}

// IntDiv divides an integer by a divisor. Zero divisor is a no-op.
type IntDiv struct {
	Key     string
	Divisor int64
}

func (IntDiv) op() {}

// IntGet reads an integer value. Returns int64 (zero if absent).
type IntGet struct{ Key string }

func (IntGet) readOp() {}

// Set ops

// SetAdd adds a member to a set. Idempotent.
type SetAdd struct {
	Key    string
	Member any
}

func (SetAdd) op() {}

// SetRemove removes a member from a set. No-op if absent.
type SetRemove struct {
	Key    string
	Member any
}

func (SetRemove) op() {}

// SetContains checks whether a member exists in a set. Returns bool.
type SetContains struct {
	Key    string
	Member any
}

func (SetContains) readOp() {}

// SetMembers returns all set members.
type SetMembers struct{ Key string }

func (SetMembers) readOp() {}

// IntMap ops

// IntMapSet sets a keyed integer to an exact value.
type IntMapSet struct {
	Key    string
	MapKey any
	Value  int64
}

func (IntMapSet) op() {}

// IntMapAdd adds a delta to a keyed integer.
type IntMapAdd struct {
	Key    string
	MapKey any
	Delta  int64
}

func (IntMapAdd) op() {}

// IntMapMul multiplies a keyed integer by a factor.
type IntMapMul struct {
	Key    string
	MapKey any
	Factor int64
}

func (IntMapMul) op() {}

// IntMapDiv divides a keyed integer by a divisor. Zero divisor is a no-op.
type IntMapDiv struct {
	Key     string
	MapKey  any
	Divisor int64
}

func (IntMapDiv) op() {}

// IntMapDelete removes a key from an integer map.
type IntMapDelete struct {
	Key    string
	MapKey any
}

func (IntMapDelete) op() {}

// IntMapGet reads a keyed integer value. Returns int64 (zero if absent).
type IntMapGet struct {
	Key    string
	MapKey any
}

func (IntMapGet) readOp() {}

// IntMapKeys returns all keys in an integer map.
type IntMapKeys struct{ Key string }

func (IntMapKeys) readOp() {}

// IntMapEntries returns all entries in an integer map.
type IntMapEntries struct{ Key string }

func (IntMapEntries) readOp() {}

// Map ops

// MapSet sets a keyed value in a map. Last write per key wins.
type MapSet struct {
	Key    string
	MapKey any
	Value  any
}

func (MapSet) op() {}

// MapDelete removes a key from a map.
type MapDelete struct {
	Key    string
	MapKey any
}

func (MapDelete) op() {}

// MapGet reads a keyed value from a map. Returns (value, true) or (nil, false).
type MapGet struct {
	Key    string
	MapKey any
}

func (MapGet) readOp() {}

// MapKeys returns all keys in a map.
type MapKeys struct{ Key string }

func (MapKeys) readOp() {}

// MapEntries returns all entries in a map.
type MapEntries struct{ Key string }

func (MapEntries) readOp() {}

// List ops

// ListAppend appends a value to a list.
type ListAppend struct {
	Key   string
	Value any
}

func (ListAppend) op() {}

// ListEntries returns all list entries as []any.
type ListEntries struct{ Key string }

func (ListEntries) readOp() {}

// ListLen returns the length of a list as int.
type ListLen struct{ Key string }

func (ListLen) readOp() {}

// QueryStateInto executes a read query against raw namespace state and
// unmarshals the result into target. It is exported for Backend
// implementations that maintain state outside this package.
func QueryStateInto(state map[string]any, q ReadOp, target any) error {
	return errors.Wrap(jsonUnmarshalInto(queryState(state, q), target), "query state")
}

// ApplyOp applies a single write Op to raw namespace state via exhaustive
// type switch.
func ApplyOp(state map[string]any, o Op) { //nolint:funlen
	switch o := o.(type) {
	case ScalarSet:
		state[o.Key] = o.Value
	case ScalarDelete:
		delete(state, o.Key)

	case IntSet:
		state[o.Key] = o.Value
	case IntAdd:
		state[o.Key] = toInt64(state[o.Key]) + o.Delta
	case IntMul:
		state[o.Key] = toInt64(state[o.Key]) * o.Factor
	case IntDiv:
		if o.Divisor != 0 {
			state[o.Key] = toInt64(state[o.Key]) / o.Divisor
		}

	case SetAdd:
		m, ok := state[o.Key].(map[string]any)
		if !ok {
			m = make(map[string]any)
			state[o.Key] = m
		}
		m[marshalKey(o.Member)] = true
	case SetRemove:
		if m, ok := state[o.Key].(map[string]any); ok {
			delete(m, marshalKey(o.Member))
		}

	case IntMapSet:
		m := getOrCreateSubmap(state, o.Key)
		m[marshalKey(o.MapKey)] = o.Value
	case IntMapAdd:
		m := getOrCreateSubmap(state, o.Key)
		k := marshalKey(o.MapKey)
		m[k] = toInt64(m[k]) + o.Delta
	case IntMapMul:
		m := getOrCreateSubmap(state, o.Key)
		k := marshalKey(o.MapKey)
		m[k] = toInt64(m[k]) * o.Factor
	case IntMapDiv:
		if o.Divisor == 0 {
			return
		}
		m, ok := state[o.Key].(map[string]any)
		if !ok {
			return
		}
		k := marshalKey(o.MapKey)
		m[k] = toInt64(m[k]) / o.Divisor
	case IntMapDelete:
		if m, ok := state[o.Key].(map[string]any); ok {
			delete(m, marshalKey(o.MapKey))
		}

	case MapSet:
		m := getOrCreateSubmap(state, o.Key)
		m[marshalKey(o.MapKey)] = o.Value
	case MapDelete:
		if m, ok := state[o.Key].(map[string]any); ok {
			delete(m, marshalKey(o.MapKey))
		}

	case ListAppend:
		s, _ := state[o.Key].([]any)
		state[o.Key] = append(s, o.Value)

	default:
		panic(fmt.Sprintf("metadatadb: unhandled op type %T", o))
	}
}

// decodeMapKeys converts a map with JSON-encoded string keys into a map
// whose keys are the decoded JSON values. This produces a map that
// json.Marshal will encode correctly for the caller's target type.
func decodeMapKeys(m map[string]any) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		// k is a JSON-encoded key (e.g. "\"hello\"" for string "hello",
		// "42" for int 42). We need to decode it so that when json.Marshal
		// re-encodes the result map, the keys come out correctly. UseNumber
		// keeps large integer keys exact through fmt.Sprint.
		dec := json.NewDecoder(strings.NewReader(k))
		dec.UseNumber()
		var decoded any
		if err := dec.Decode(&decoded); err != nil {
			result[k] = v
			continue
		}
		// For string-keyed maps, decoded is a string — use it directly.
		// For numeric keys, fmt.Sprint produces "42" which json.Marshal
		// will use as the map key string.
		result[fmt.Sprint(decoded)] = v
	}
	return result
}

func marshalKey(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("metadatadb: marshal key %T: %v", v, err))
	}
	// Canonicalize composite keys: a struct marshals in field order, but the
	// same key decoded from a backend's wire format is a map, which
	// re-marshals with sorted keys — the two encodings must agree or
	// replayed entries land under different state keys than local ones.
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var decoded any
		if err := dec.Decode(&decoded); err == nil {
			if canon, err := json.Marshal(decoded); err == nil {
				return string(canon)
			}
		}
	}
	return string(data)
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		f, _ := n.Float64() //nolint:errcheck // malformed numbers become 0
		return int64(f)
	default:
		return 0
	}
}

func getOrCreateSubmap(state map[string]any, key string) map[string]any {
	m, ok := state[key].(map[string]any)
	if !ok {
		m = make(map[string]any)
		state[key] = m
	}
	return m
}

// queryResult is used to return optional values from queryState.
type queryResult struct {
	Value any
	OK    bool
}

// queryState executes a ReadOp against the in-memory state via exhaustive type switch.
func queryState(state map[string]any, q ReadOp) any { //nolint:funlen
	switch q := q.(type) {
	case ScalarGet:
		raw, ok := state[q.Key]
		if !ok {
			return nil
		}
		return queryResult{Value: raw, OK: true}

	case IntGet:
		return toInt64(state[q.Key])

	case SetContains:
		m, ok := state[q.Key].(map[string]any)
		if !ok {
			return false
		}
		_, ok = m[marshalKey(q.Member)]
		return ok
	case SetMembers:
		raw, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		members := make([]any, 0, len(raw))
		for k := range raw {
			members = append(members, json.RawMessage(k))
		}
		return members

	case IntMapGet:
		m, ok := state[q.Key].(map[string]any)
		if !ok {
			return int64(0)
		}
		return toInt64(m[marshalKey(q.MapKey)])
	case IntMapKeys:
		raw, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		keys := make([]any, 0, len(raw))
		for k := range raw {
			keys = append(keys, json.RawMessage(k))
		}
		return keys
	case IntMapEntries:
		raw, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		return decodeMapKeys(raw)

	case MapGet:
		m, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		v, ok := m[marshalKey(q.MapKey)]
		if !ok {
			return nil
		}
		return queryResult{Value: v, OK: true}
	case MapKeys:
		raw, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		keys := make([]any, 0, len(raw))
		for k := range raw {
			keys = append(keys, json.RawMessage(k))
		}
		return keys
	case MapEntries:
		raw, ok := state[q.Key].(map[string]any)
		if !ok {
			return nil
		}
		return decodeMapKeys(raw)

	case ListEntries:
		return state[q.Key]
	case ListLen:
		raw, ok := state[q.Key].([]any)
		if !ok {
			return 0
		}
		return len(raw)

	default:
		panic(fmt.Sprintf("metadatadb: unhandled query type %T", q))
	}
}
