//go:build !linux && !darwin

package client

import (
	"os"

	"github.com/alecthomas/errors"
)

// punchHole reports that hole punching is unavailable. Callers treat it as
// best-effort: the spilled data stays allocated, costing no more disk than a
// plain temporary file.
func punchHole(_ *os.File, _, _ int64) error {
	return errors.New("hole punching unsupported on this platform")
}
