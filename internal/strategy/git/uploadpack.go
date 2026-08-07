package git

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/gitclone"
)

// uploadPackParseLimit bounds CPU and memory spent on hostile request bodies.
// Real upload-pack bodies are tiny (a few want lines plus optional haves), so
// 4 MiB covers monorepo fetches and still bounds crafted input.
const uploadPackParseLimit = 4 * 1024 * 1024

// There is deliberately no separate cap on the number of want OIDs.
// uploadPackParseLimit already bounds the want count: a want line is ~50 bytes
// on the wire, so 4 MiB admits at most ~84k wants, which bounds both the map
// here and the cat-file stdin the incremental path builds from it. An explicit
// count cap only ever fired on legitimate traffic: repos with many refs
// (tt-metal advertises ~38k) silently skipped the incremental path. cachew is a
// trusted-environment cache, not a security boundary.

// errBodyTooLarge is returned when a body exceeds uploadPackParseLimit. It is a
// sentinel so callers can tell a declined but otherwise legitimate fetch from an
// ordinary malformed body.
var errBodyTooLarge = errors.Errorf("upload-pack body exceeds parse limit of %d bytes", uploadPackParseLimit)

// UploadPackRequest is the parsed form of a git-upload-pack request body.
type UploadPackRequest struct {
	Wants    []string // unique want object IDs (40- or 64-char lower hex), in order of appearance
	HasHaves bool     // body contains at least one "have <oid>" line. Informational only: incremental pull-through does not gate on it, because a fetch with haves may still need objects the mirror lacks. RequestIsClone in repocounts.go is the path that does gate on haves.
	LsRefs   bool     // protocol v2 command=ls-refs (ref discovery, not a fetch)
	WantRefs bool     // body contains protocol v2 "want-ref <ref>" lines
}

// ParseUploadPackRequest parses a git-upload-pack request body (pkt-line format,
// protocol v0/v1 or v2). gzipEncoded indicates the body is gzip compressed
// (Content-Encoding: gzip). A malformed or oversized body returns an error so
// callers can skip incremental handling.
func ParseUploadPackRequest(body []byte, gzipEncoded bool) (UploadPackRequest, error) {
	data := body
	if gzipEncoded {
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return UploadPackRequest{}, errors.Wrap(err, "gzip reader")
		}
		decoded, err := io.ReadAll(io.LimitReader(gr, uploadPackParseLimit+1))
		_ = gr.Close() //nolint:errcheck // best-effort
		if err != nil {
			return UploadPackRequest{}, errors.Wrap(err, "decompress upload-pack body")
		}
		data = decoded
	}

	if int64(len(data)) > uploadPackParseLimit {
		return UploadPackRequest{}, errBodyTooLarge
	}

	return parsePktLines(data)
}

// parsePktLines decodes a pkt-line stream and extracts upload-pack semantics.
func parsePktLines(data []byte) (UploadPackRequest, error) {
	var result UploadPackRequest
	seen := make(map[string]struct{})

	for len(data) > 0 {
		if len(data) < 4 {
			return UploadPackRequest{}, errors.New("truncated pkt-line length")
		}

		lenHex := data[:4]
		pktLen, err := parseHex4(lenHex)
		if err != nil {
			return UploadPackRequest{}, errors.Wrap(err, "invalid pkt-line length")
		}

		// pktLen 0 = flush-pkt, 1 = delim-pkt (v2), 2 = response-end. None carry a
		// payload. The pkt-line spec reserves 0, 1 and 2; 3 is invalid.
		if pktLen == 3 {
			return UploadPackRequest{}, errors.New("invalid pkt-line length value 3")
		}
		if pktLen <= 2 {
			data = data[4:]
			continue
		}

		if pktLen > len(data) {
			return UploadPackRequest{}, errors.Errorf("pkt-line length %d exceeds remaining data %d", pktLen, len(data))
		}

		payload := data[4:pktLen]
		data = data[pktLen:]

		payload = bytes.TrimSuffix(payload, []byte("\n"))

		if err := handleLine(payload, &result, seen); err != nil {
			return UploadPackRequest{}, err
		}
	}

	return result, nil
}

// handleLine processes a single decoded pkt-line payload.
func handleLine(line []byte, r *UploadPackRequest, seen map[string]struct{}) error {
	switch {
	case bytes.HasPrefix(line, []byte("want ")):
		// First want line may carry capabilities after the OID. Protocol v0
		// separates them with a NUL (gitprotocol-pack(5)); some clients and
		// fixtures use a space. Truncate at the first of either.
		rest := line[5:]
		if i := bytes.IndexAny(rest, " \x00"); i >= 0 {
			rest = rest[:i]
		}
		oid := string(rest)
		if err := gitclone.ValidateOID(oid); err != nil {
			return errors.Errorf("malformed want OID: %q", oid)
		}
		if _, dup := seen[oid]; !dup {
			seen[oid] = struct{}{}
			r.Wants = append(r.Wants, oid)
		}

	case bytes.HasPrefix(line, []byte("have ")):
		r.HasHaves = true

	case bytes.HasPrefix(line, []byte("command=ls-refs")):
		r.LsRefs = true

	case bytes.HasPrefix(line, []byte("want-ref ")):
		r.WantRefs = true
	}

	return nil
}

// parseHex4 converts 4 ASCII hex bytes to an integer.
func parseHex4(b []byte) (int, error) {
	dst := make([]byte, 2)
	n, err := hex.Decode(dst, b)
	if err != nil || n != 2 {
		return 0, errors.Errorf("invalid hex pkt-line length %q", b)
	}
	return int(dst[0])<<8 | int(dst[1]), nil
}
