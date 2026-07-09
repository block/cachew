// Package punchholelayout captures the memory layout of the macOS SDK's
// fpunchhole_t via cgo, so tests can verify that the hand-defined Go mirror
// in package client stays identical to the C struct.
package punchholelayout

// Layout describes the size and field layout of a punch-hole argument struct,
// in bytes.
type Layout struct {
	Size           uintptr
	FlagsOffset    uintptr
	FlagsSize      uintptr
	ReservedOffset uintptr
	ReservedSize   uintptr
	OffsetOffset   uintptr
	OffsetSize     uintptr
	LengthOffset   uintptr
	LengthSize     uintptr
}
