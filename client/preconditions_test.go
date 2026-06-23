package client_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestRangeHeaderFormat(t *testing.T) {
	tests := []struct {
		name       string
		start, end int64
		want       string
	}{
		{name: "FirstN", start: 0, end: 500, want: "bytes=0-499"},
		{name: "Middle", start: 100, end: 200, want: "bytes=100-199"},
		{name: "ToEnd", start: 0, end: -1, want: "bytes=0-"},
		{name: "FromOffsetToEnd", start: 100, end: -1, want: "bytes=100-"},
		{name: "Single", start: 5, end: 6, want: "bytes=5-5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := client.NewRequestOptions(client.Range(tt.start, tt.end))
			assert.Equal(t, tt.want, o.Range)
		})
	}
}

func TestResolveRange(t *testing.T) {
	const etag = `"e"`
	tests := []struct {
		name        string
		spec        string
		ifRange     string
		size        int64
		wantStart   int64
		wantLength  int64
		wantOutcome client.RangeOutcome
	}{
		{name: "NoRange", spec: "", size: 10, wantStart: 0, wantLength: 10, wantOutcome: client.RangeFull},
		{name: "FirstBytes", spec: "bytes=0-4", size: 10, wantStart: 0, wantLength: 5, wantOutcome: client.RangePartial},
		{name: "Middle", spec: "bytes=2-5", size: 10, wantStart: 2, wantLength: 4, wantOutcome: client.RangePartial},
		{name: "OpenEnded", spec: "bytes=3-", size: 10, wantStart: 3, wantLength: 7, wantOutcome: client.RangePartial},
		{name: "Suffix", spec: "bytes=-3", size: 10, wantStart: 7, wantLength: 3, wantOutcome: client.RangePartial},
		{name: "SuffixLargerThanSize", spec: "bytes=-20", size: 10, wantStart: 0, wantLength: 10, wantOutcome: client.RangePartial},
		{name: "EndBeyondSize", spec: "bytes=5-100", size: 10, wantStart: 5, wantLength: 5, wantOutcome: client.RangePartial},
		{name: "StartAtSize", spec: "bytes=10-20", size: 10, wantOutcome: client.RangeNotSatisfiable},
		{name: "StartBeyondSize", spec: "bytes=20-", size: 10, wantOutcome: client.RangeNotSatisfiable},
		{name: "SuffixZero", spec: "bytes=-0", size: 10, wantOutcome: client.RangeNotSatisfiable},
		{name: "ZeroSizeSuffix", spec: "bytes=-1", size: 0, wantOutcome: client.RangeNotSatisfiable},
		{name: "ZeroSizeRange", spec: "bytes=0-0", size: 0, wantOutcome: client.RangeNotSatisfiable},
		{name: "Multi", spec: "bytes=0-1,3-4", size: 10, wantLength: 10, wantOutcome: client.RangeFull},
		{name: "MissingPrefix", spec: "0-4", size: 10, wantLength: 10, wantOutcome: client.RangeFull},
		{name: "StartGreaterThanEnd", spec: "bytes=5-2", size: 10, wantLength: 10, wantOutcome: client.RangeFull},
		{name: "NonNumeric", spec: "bytes=a-b", size: 10, wantLength: 10, wantOutcome: client.RangeFull},
		{name: "IfRangeMatch", spec: "bytes=0-4", ifRange: etag, size: 10, wantStart: 0, wantLength: 5, wantOutcome: client.RangePartial},
		{name: "IfRangeMismatch", spec: "bytes=0-4", ifRange: `"other"`, size: 10, wantLength: 10, wantOutcome: client.RangeFull},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := client.NewRequestOptions(client.RangeHeader(tt.spec), client.IfRange(tt.ifRange))
			start, length, outcome := o.ResolveRange(tt.size, etag)
			assert.Equal(t, tt.wantOutcome, outcome)
			if outcome == client.RangeNotSatisfiable {
				return
			}
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantLength, length)
		})
	}
}
