package client

import (
	"regexp"

	"github.com/alecthomas/errors"
)

// DefaultNamespace is used when a namespace is not explicitly specified.
const DefaultNamespace Namespace = "default"

var namespaceRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

// Namespace identifies a logical partition within a cache or metadata store.
// Valid names start with an alphanumeric character and contain only
// alphanumerics, hyphens, and underscores.
type Namespace string

// ValidateNamespace checks that a namespace name is valid.
func ValidateNamespace(name string) error {
	if !namespaceRe.MatchString(name) {
		return errors.Errorf("invalid namespace %q: must match %s", name, namespaceRe)
	}
	return nil
}

// ParseNamespace validates and returns a Namespace from a plain string.
func ParseNamespace(name string) (Namespace, error) {
	if err := ValidateNamespace(name); err != nil {
		return "", err
	}
	return Namespace(name), nil
}

func (n *Namespace) String() string { return string(*n) }

// UnmarshalText implements encoding.TextUnmarshaler with validation.
func (n *Namespace) UnmarshalText(text []byte) error {
	if err := ValidateNamespace(string(text)); err != nil {
		return err
	}
	*n = Namespace(text)
	return nil
}
