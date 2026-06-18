package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"

	"github.com/alecthomas/errors"
)

// HashingWriter wraps an io.Writer to compute a SHA-256 content hash as data
// passes through. After all writes complete, call ETag() to retrieve the
// resulting strong ETag value.
type HashingWriter struct {
	w    io.Writer
	hash hash.Hash
}

// NewHashingWriter returns a writer that hashes all bytes written through it.
func NewHashingWriter(w io.Writer) *HashingWriter {
	h := sha256.New()
	return &HashingWriter{
		w:    io.MultiWriter(w, h),
		hash: h,
	}
}

// Write passes p through to the underlying writer and updates the hash.
func (hw *HashingWriter) Write(p []byte) (int, error) {
	n, err := hw.w.Write(p)
	return n, errors.WithStack(err)
}

// ETag returns the strong ETag derived from the content hash.
// Must only be called after all writes are complete.
func (hw *HashingWriter) ETag() string {
	return `"sha256:` + hex.EncodeToString(hw.hash.Sum(nil)) + `"`
}

// SetETag injects an ETag header into the given header map.
func SetETag(headers http.Header, etag string) {
	headers.Set("ETag", etag)
}

// CheckPreconditions evaluates If-Match and If-None-Match preconditions against
// the given headers. Returns ErrPreconditionFailed or ErrNotModified if a
// precondition is not satisfied, or nil if all preconditions pass.
func CheckPreconditions(headers http.Header, conds ...Precondition) error {
	if len(conds) == 0 {
		return nil
	}
	p := ResolvePreconditions(conds)
	etag := headers.Get("ETag")
	if p.IfMatch != "" && !CheckIfMatch(p.IfMatch, etag) {
		return ErrPreconditionFailed
	}
	if p.IfNoneMatch != "" && CheckIfNoneMatch(p.IfNoneMatch, etag) {
		return ErrNotModified
	}
	return nil
}

// RequestHeaders that should be stripped when storing object metadata.
// These are per-request conditional headers that should not be persisted.
var RequestHeaders = []string{ //nolint:gochecknoglobals
	"If-Match",
	"If-None-Match",
	"If-Modified-Since",
	"If-Unmodified-Since",
	"If-Range",
}

// CheckIfNoneMatch reports whether the given ETag matches any of the values in
// the If-None-Match header using weak comparison (RFC 7232 §3.2).
// The "*" wildcard matches any existing resource, even one without an ETag.
func CheckIfNoneMatch(ifNoneMatch string, etag string) bool {
	if ifNoneMatch == "" {
		return false
	}
	if ifNoneMatch == "*" {
		return true
	}
	if etag == "" {
		return false
	}
	return parseETagList(ifNoneMatch, etag, true)
}

// CheckIfMatch reports whether the given ETag satisfies the If-Match
// precondition (RFC 7232 §3.1). An empty If-Match header is always satisfied.
// The "*" wildcard is satisfied by any existing resource, even one without an ETag.
func CheckIfMatch(ifMatch string, etag string) bool {
	if ifMatch == "" {
		return true
	}
	if ifMatch == "*" {
		return true
	}
	if etag == "" {
		return false
	}
	return parseETagList(ifMatch, etag, false)
}

// parseETagList checks if targetETag appears in a comma-separated ETag list.
// When weakComparison is true, W/ prefixes are ignored (RFC 7232 §2.3.2).
// When false, strong comparison is used (RFC 7232 §2.3.1).
func parseETagList(list, targetETag string, weakComparison bool) bool {
	for list != "" {
		// Skip leading whitespace and commas
		for len(list) > 0 && (list[0] == ' ' || list[0] == '\t' || list[0] == ',') {
			list = list[1:]
		}
		if list == "" {
			break
		}

		// Handle weak ETags by skipping the W/ prefix for comparison
		candidate := list
		weak := false
		if len(candidate) >= 2 && candidate[0] == 'W' && candidate[1] == '/' {
			candidate = candidate[2:]
			weak = true
		}

		if len(candidate) == 0 || candidate[0] != '"' {
			// Malformed — skip to next comma
			if idx := indexOf(list, ','); idx >= 0 {
				list = list[idx+1:]
			} else {
				break
			}
			continue
		}

		// Find closing quote
		end := 1
		for end < len(candidate) && candidate[end] != '"' {
			end++
		}
		if end >= len(candidate) {
			break
		}
		end++ // include closing quote

		etag := candidate[:end]
		if weak {
			etag = "W/" + etag
		}

		var match bool
		if weakComparison {
			match = weakEqual(etag, targetETag)
		} else {
			match = strongEqual(etag, targetETag)
		}
		if match {
			return true
		}

		list = candidate[end:]
	}
	return false
}

func strongEqual(a, b string) bool {
	return a == b && !isWeakETag(a) && !isWeakETag(b)
}

func isWeakETag(etag string) bool {
	return len(etag) >= 2 && etag[0] == 'W' && etag[1] == '/'
}

func weakEqual(a, b string) bool {
	return stripWeakPrefix(a) == stripWeakPrefix(b)
}

func stripWeakPrefix(etag string) string {
	if len(etag) >= 2 && etag[0] == 'W' && etag[1] == '/' {
		return etag[2:]
	}
	return etag
}

func indexOf(s string, c byte) int {
	for i := range len(s) {
		if s[i] == c {
			return i
		}
	}
	return -1
}
