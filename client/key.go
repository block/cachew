package client

import (
	"crypto/sha256"
	"encoding/hex"
)

// Key represents a unique identifier for a cached object.
type Key [32]byte

// ParseKey from its hex-encoded string form.
func ParseKey(key string) (Key, error) {
	var k Key
	return k, k.UnmarshalText([]byte(key))
}

// NewKey returns the SHA256 of s.
func NewKey(s string) Key { return Key(sha256.Sum256([]byte(s))) }

func (k *Key) String() string { return hex.EncodeToString(k[:]) }

func (k *Key) UnmarshalText(text []byte) error {
	if len(text) == 64 {
		bytes, err := hex.DecodeString(string(text))
		if err == nil && len(bytes) == len(*k) {
			copy(k[:], bytes)
			return nil
		}
	}
	*k = NewKey(string(text))
	return nil
}

func (k *Key) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}
