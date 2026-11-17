// Package featureflags provides typed feature flags read from uppercased CACHEW_FF_<NAME> environment variables.
//
// Values are JSON-decoded into the flag's int or bool type; an unset variable yields the default.
package featureflags

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Type constrains the value types a Flag can hold.
type Type interface{ int | bool }

// Flag is a feature flag read from the CACHEW_FF_<NAME> environment variable.
type Flag[T Type] struct {
	name string
	dflt T
}

// New returns a Flag with a default value, panicking if its environment variable is set but invalid.
func New[T Type](name string, dflt T) Flag[T] {
	f := Flag[T]{name: name, dflt: dflt}
	f.Get() // no-op Get to validate that the envar is valid.
	return f
}

// Get returns the flag's current value, or its default if unset, panicking if the value is invalid.
func (f Flag[T]) Get() (out T) {
	v, ok := os.LookupEnv("CACHEW_FF_" + strings.ToUpper(f.name))
	if !ok {
		return f.dflt
	}
	err := json.Unmarshal([]byte(v), &out)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal feature flag %s: %v", f.name, err))
	}
	return out
}
