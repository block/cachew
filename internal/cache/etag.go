package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"net/http"
)

// ETagKey is the HTTP header key used to store the ETag.
const ETagKey = "ETag"

// etagWriter computes a SHA256 hash of all written data. After all data has
// been written, call SetETag to populate the ETag header.
type etagWriter struct {
	hash hash.Hash
}

func newETagWriter() *etagWriter {
	return &etagWriter{hash: sha256.New()}
}

// WriteBytes feeds p into the running hash. sha256 never returns an error.
func (e *etagWriter) WriteBytes(p []byte) {
	_, _ = e.hash.Write(p)
}

// SetETag sets the ETag header on the given headers from the accumulated hash.
func (e *etagWriter) SetETag(headers http.Header) {
	headers.Set(ETagKey, `"`+hex.EncodeToString(e.hash.Sum(nil))+`"`)
}
