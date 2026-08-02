// Package gitcredential provides repository-scoped credentials for Git subprocesses.
package gitcredential

import (
	"context"

	"github.com/alecthomas/errors"
)

// Credential contains an HTTP Authorization header value and its repository URL scope.
type Credential struct {
	Authorization string
	URLScope      string
}

// Provider selects and obtains credentials for repository URLs.
type Provider interface {
	Credential(ctx context.Context, repositoryURL string) (Credential, bool, error)
}

// ProviderFunc adapts a function to Provider.
type ProviderFunc func(context.Context, string) (Credential, bool, error)

// Credential calls f.
func (f ProviderFunc) Credential(ctx context.Context, repositoryURL string) (Credential, bool, error) {
	return f(ctx, repositoryURL)
}

// Composite tries providers in order and stops at the first match.
type Composite []Provider

// Credential returns the first matching provider's result.
func (c Composite) Credential(ctx context.Context, repositoryURL string) (Credential, bool, error) {
	for _, provider := range c {
		if provider == nil {
			continue
		}
		credential, matched, err := provider.Credential(ctx, repositoryURL)
		if matched || err != nil {
			return credential, matched, errors.WithStack(err)
		}
	}
	return Credential{}, false, nil
}
