package std

// kept records an invariant the code does not show.
func kept() int { return 1 } //nolint:privatedoc // documents an invariant

var _ = kept
