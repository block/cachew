//go:build darwin

package client

import (
	"testing"
	"unsafe"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client/internal/punchholelayout"
)

func TestFpunchholeMatchesSDKLayout(t *testing.T) {
	sdk, ok := punchholelayout.SDKLayout()
	if !ok {
		t.Skip("cgo unavailable, cannot compare against the SDK's fpunchhole_t")
	}
	var arg fpunchhole
	assert.Equal(t, punchholelayout.Layout{
		Size:           unsafe.Sizeof(arg),
		FlagsOffset:    unsafe.Offsetof(arg.flags),
		FlagsSize:      unsafe.Sizeof(arg.flags),
		ReservedOffset: unsafe.Offsetof(arg.reserved),
		ReservedSize:   unsafe.Sizeof(arg.reserved),
		OffsetOffset:   unsafe.Offsetof(arg.offset),
		OffsetSize:     unsafe.Sizeof(arg.offset),
		LengthOffset:   unsafe.Offsetof(arg.length),
		LengthSize:     unsafe.Sizeof(arg.length),
	}, sdk)
}
