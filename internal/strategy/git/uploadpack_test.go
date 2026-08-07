package git_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/strategy/git"
)

// pkt formats a single pkt-line with the length prefix.
func pkt(s string) string {
	n := len(s) + 4
	return fmt.Sprintf("%04x%s", n, s)
}

const (
	flushPkt = "0000"
	delimPkt = "0001"
)

func sha1OID(prefix string) string {
	s := prefix + strings.Repeat("0", 40-len(prefix))
	return s
}

func sha256OID(prefix string) string {
	s := prefix + strings.Repeat("0", 64-len(prefix))
	return s
}

func buildBody(lines ...string) []byte {
	return []byte(strings.Join(lines, ""))
}

func gzipBody(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(b)
	_ = w.Close()
	return buf.Bytes()
}

func TestParseUploadPackRequest(t *testing.T) {
	oid1 := sha1OID("aabbcc")
	oid2 := sha1OID("ddeeff")
	oid3 := sha256OID("112233")

	tests := []struct {
		name        string
		body        []byte
		gzip        bool
		expected    git.UploadPackRequest
		errContains string
	}{
		{
			name: "V0FirstWantNULCapabilities",
			// Real protocol v0 first-want: PKT-LINE("want" SP obj-id NUL capability-list LF).
			body: buildBody(
				pkt("want "+oid1+"\x00multi_ack side-band-64k agent=git/2.45\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid1}},
		},
		{
			name: "V0SubsequentWantSpaceCapabilities",
			// Some fixtures and non-git clients separate capabilities with a space;
			// accept either delimiter so incremental engages for both.
			body: buildBody(
				pkt("want "+oid1+" multi_ack side-band-64k agent=git/2.45\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid1}},
		},
		{
			name: "V0MultipleWantsAndHavesAndDone",
			body: buildBody(
				pkt("want "+oid1+"\x00multi_ack side-band-64k\n"),
				pkt("want "+oid2+"\n"),
				flushPkt,
				pkt("have "+oid1+"\n"),
				pkt("done\n"),
			),
			expected: git.UploadPackRequest{Wants: []string{oid1, oid2}, HasHaves: true},
		},
		{
			name: "V1IgnoresUnknownLines",
			body: buildBody(
				pkt("version=1\n"),
				pkt("want "+oid1+"\n"),
				pkt("want "+oid2+"\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid1, oid2}},
		},
		{
			name: "V2CommandFetchWithWantsAndHaves",
			body: buildBody(
				pkt("command=fetch\n"),
				pkt("agent=git/2.45\n"),
				delimPkt,
				pkt("want "+oid1+"\n"),
				pkt("want "+oid2+"\n"),
				pkt("have "+oid1+"\n"),
				pkt("done\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid1, oid2}, HasHaves: true},
		},
		{
			name: "V2CommandLsRefs",
			body: buildBody(
				pkt("command=ls-refs\n"),
				pkt("agent=git/2.45\n"),
				delimPkt,
				pkt("peel\n"),
				pkt("symrefs\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{LsRefs: true},
		},
		{
			name: "V2WantRef",
			body: buildBody(
				pkt("command=fetch\n"),
				delimPkt,
				pkt("want-ref refs/heads/main\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{WantRefs: true},
		},
		{
			name: "DuplicateWantsDeduped",
			body: buildBody(
				pkt("want "+oid1+"\n"),
				pkt("want "+oid1+"\n"),
				pkt("want "+oid2+"\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid1, oid2}},
		},
		{
			name: "SHA256Wants",
			body: buildBody(
				pkt("want "+oid3+"\n"),
				flushPkt,
			),
			expected: git.UploadPackRequest{Wants: []string{oid3}},
		},
		{
			name: "GzipEncodedBody",
			body: gzipBody(buildBody(
				pkt("want "+oid1+"\n"),
				pkt("have "+oid2+"\n"),
				flushPkt,
			)),
			gzip:     true,
			expected: git.UploadPackRequest{Wants: []string{oid1}, HasHaves: true},
		},
		{
			name:     "EmptyBody",
			body:     []byte{},
			expected: git.UploadPackRequest{},
		},
		{
			name:        "GarbageNonPktBody",
			body:        []byte("not a pkt-line body at all!!"),
			errContains: "invalid",
		},
		{
			name:        "TruncatedPktLength",
			body:        []byte("001"),
			errContains: "truncated",
		},
		{
			name:        "MalformedWantOID",
			body:        buildBody(pkt("want ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ\n")),
			errContains: "malformed want OID",
		},
		{
			name: "BodyExceedingParseLimit",
			// Build a body that just exceeds the 4 MiB limit using repeated tiny pkt-lines.
			body: func() []byte {
				chunk := []byte(pkt("have " + oid1 + "\n"))
				repeat := (git.UploadPackParseLimit / len(chunk)) + 1
				return bytes.Repeat(chunk, repeat)
			}(),
			errContains: "exceeds parse limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := git.ParseUploadPackRequest(tt.body, tt.gzip)
			if tt.errContains != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Zero(t, got, "a parse failure must not return a partially populated request")
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestParseUploadPackRequestManyWants pins the absence of a want-count cap:
// repos with thousands of refs (tt-metal advertises ~38k) must keep their full
// want list so the incremental path stays engaged. The only bound is
// uploadPackParseLimit, which admits ~84k want lines.
func TestParseUploadPackRequestManyWants(t *testing.T) {
	const wantCount = 20000

	var b strings.Builder
	for i := range wantCount {
		b.WriteString(pkt(fmt.Sprintf("want %040x\n", i)))
	}
	b.WriteString(flushPkt)

	got, err := git.ParseUploadPackRequest([]byte(b.String()), false)
	assert.NoError(t, err)
	assert.Equal(t, wantCount, len(got.Wants))
	assert.Equal(t, fmt.Sprintf("%040x", 0), got.Wants[0])
	assert.Equal(t, fmt.Sprintf("%040x", wantCount-1), got.Wants[wantCount-1])
}
