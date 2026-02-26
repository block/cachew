// Package githubapp provides GitHub App authentication and token management.
package githubapp

import "time"

// Config represents the configuration for a single GitHub App.
type Config struct {
	Name           string            `hcl:"name,label" help:"Name for this GitHub App configuration."`
	AppID          string            `hcl:"app-id,optional" help:"GitHub App ID"`
	PrivateKeyPath string            `hcl:"private-key-path,optional" help:"Path to GitHub App private key (PEM format)"`
	Installations  map[string]string `hcl:"installations,optional" help:"Mapping of org names to installation IDs"`
}

// TokenCacheConfig configures token caching behavior.
type TokenCacheConfig struct {
	RefreshBuffer time.Duration // How early to refresh before expiration
	JWTExpiration time.Duration // GitHub allows max 10 minutes
}

// DefaultTokenCacheConfig returns default token cache configuration.
func DefaultTokenCacheConfig() TokenCacheConfig {
	return TokenCacheConfig{
		RefreshBuffer: 5 * time.Minute,
		JWTExpiration: 10 * time.Minute,
	}
}
