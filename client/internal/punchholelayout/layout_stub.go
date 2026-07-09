//go:build !darwin || !cgo

package punchholelayout

// SDKLayout reports false: without cgo on darwin there is no SDK struct to
// compare against.
func SDKLayout() (Layout, bool) {
	return Layout{}, false
}
