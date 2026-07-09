//go:build darwin && cgo

package punchholelayout

/*
#include <sys/fcntl.h>
*/
import "C"

import "unsafe"

// SDKLayout returns the layout of struct fpunchhole_t as compiled from the
// SDK's <sys/fcntl.h>.
func SDKLayout() (Layout, bool) {
	var v C.fpunchhole_t
	return Layout{
		Size:           unsafe.Sizeof(v),
		FlagsOffset:    unsafe.Offsetof(v.fp_flags),
		FlagsSize:      unsafe.Sizeof(v.fp_flags),
		ReservedOffset: unsafe.Offsetof(v.reserved),
		ReservedSize:   unsafe.Sizeof(v.reserved),
		OffsetOffset:   unsafe.Offsetof(v.fp_offset),
		OffsetSize:     unsafe.Sizeof(v.fp_offset),
		LengthOffset:   unsafe.Offsetof(v.fp_length),
		LengthSize:     unsafe.Sizeof(v.fp_length),
	}, true
}
