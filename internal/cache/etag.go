package cache

import (
	"net/http"

	"github.com/alecthomas/errors"
	"github.com/google/uuid"

	"github.com/block/cachew/client"
)

// ETagKey is the HTTP header key used to store the ETag.
const ETagKey = client.ETagKey

// ValidateRawETag verifies that etag is an unquoted cache ETag value.
func ValidateRawETag(etag string) error { return errors.WithStack(client.ValidateRawETag(etag)) }

// FormatETag formats a raw ETag value as a strong HTTP ETag.
func FormatETag(etag string) (string, error) { return errors.WithStack2(client.FormatETag(etag)) }

// RawETagFromHeader extracts a raw ETag value from a strong HTTP ETag header.
func RawETagFromHeader(etag string) (string, error) {
	return errors.WithStack2(client.RawETagFromHeader(etag))
}

// WithETag sets the raw ETag value to store on Create.
func WithETag(etag string) Option { return client.WithETag(etag) }

func createETag(opts ...Option) (string, string, error) {
	ro := NewRequestOptions(opts...)
	raw := ro.ETag
	if !ro.ETagSet {
		raw = uuid.NewString()
	}
	quoted, err := FormatETag(raw)
	if err != nil {
		return "", "", err
	}
	return raw, quoted, nil
}

func setCreateETag(headers http.Header, opts ...Option) error {
	_, quoted, err := createETag(opts...)
	if err != nil {
		return errors.WithStack(err)
	}
	headers.Set(ETagKey, quoted)
	return nil
}
