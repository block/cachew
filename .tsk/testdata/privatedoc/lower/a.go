package lower

// tiny is one line.
func tiny() int { return 1 } // want `remove the doc comment on tiny: unexported functions under 3 lines`

// small is long enough once the threshold drops.
func small() int {
	return tiny()
}

// Exported is public, so it must be documented.
func Exported() int { return small() }
