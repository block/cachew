// Package std exercises the default comment shape.
package std

// One is documented in a single line.
func One() int { return 1 }

// Two has a summary.
//
// It also has an explanation
// that spans two lines.
func Two() int { return 2 }

// Wrapped has a summary that wraps // want `shorten this comment to at most 1 summary line`
// onto a second line.
func Wrapped() int { return 3 }

// Long has a summary. // want `up to 2 line\(s\) of explanation`
//
// Its explanation
// runs to
// three lines.
func Long() int { return 4 }

// Split has a summary. // want `shorten this comment`
//
// One paragraph.
//
// Another paragraph.
func Split() int { return 5 }

// Directive lines do not count.
//
//go:noinline
func Directive() int { return 6 }

// Nosec lines do not count.
//
// They follow an explanation
// that spans two lines.
// #nosec G204 - not a real finding
func Nosec() int { return 8 }

//nolint:commentshape // kept to show suppression
// Suppressed wraps
// onto a second line.
func Suppressed() int { return 7 }

func inline() int {
	// A single inline line is fine.
	x := 1
	// Two inline lines // want `shorten this comment`
	// are too many.
	x++
	return x
}

/* A one-line block comment is fine. */
var _ = inline
