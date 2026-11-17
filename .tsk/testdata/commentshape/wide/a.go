// Package wide exercises raised limits.
package wide

// Wrapped has a summary that wraps
// onto a second line.
func Wrapped() int { return 1 }

// Long has a summary. // want `at most 2 summary line\(s\), then optionally a blank comment line and up to 3`
//
// Its explanation
// runs to
// four
// lines.
func Long() int { return 2 }
