package std

// short adds one.
func short(x int) int { // want `remove the doc comment on short: unexported functions under 20 lines`
	return x + 1
}

type box struct{ n int }

// get returns n.
func (b box) get() int { // want `remove the doc comment on get`
	return b.n
}

// Exported is public, so it must be documented.
func Exported() int {
	return short(1) + box{}.get() + long(1) + bare() + directive()
}

func bare() int {
	return 0
}

//go:noinline
func directive() int {
	return 0
}

// long is big enough to warrant an explanation.
func long(x int) int {
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	x++
	return x
}
