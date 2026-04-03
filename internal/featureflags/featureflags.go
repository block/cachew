// Package featureflags provides typed feature flags backed by environment
// variables. Each flag is read from an environment variable named
// CACHEW_FF_<NAME> (uppercased) and JSON-decoded into the flag's type. If the
// variable is unset, the flag returns its default value. Supported types are
// int and bool.
package featureflags

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Type interface{ int | bool }

type Flag[T Type] struct {
	name string
	dflt T
}

func New[T Type](name string, dflt T) Flag[T] {
	f := Flag[T]{name: name, dflt: dflt}
	f.Get() // no-op Get to validate that the envar is valid.
	return f
}

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
