// Package githubapp provides GitHub App authentication and token management.
package githubapp

import (
	"log/slog"
	"time"

	"github.com/alecthomas/errors"
)

type Config struct {
	AppID          string            `hcl:"app-id,optional" help:"GitHub App ID"`
	PrivateKeyPath string            `hcl:"private-key-path,optional" help:"Path to GitHub App private key (PEM format)"`
	Installations  map[string]string `hcl:"installations,optional" help:"Mapping of org names to installation IDs"`
}

// Installations maps organization names to GitHub App installation IDs.
type Installations struct {
	appID          string
	privateKeyPath string
	orgs           map[string]string
}

// NewInstallations creates an Installations instance from config.
func NewInstallations(config Config, logger *slog.Logger) (*Installations, error) {
	if len(config.Installations) == 0 {
		return nil, errors.New("installations is required")
	}

	logger.Info("GitHub App config initialized",
		"app_id", config.AppID,
		"private_key_path", config.PrivateKeyPath,
		"installations", len(config.Installations))

	return &Installations{
		appID:          config.AppID,
		privateKeyPath: config.PrivateKeyPath,
		orgs:           config.Installations,
	}, nil
}

func (i *Installations) IsConfigured() bool {
	return i != nil && i.appID != "" && i.privateKeyPath != "" && len(i.orgs) > 0
}

func (i *Installations) GetInstallationID(org string) string {
	if i == nil || i.orgs == nil {
		return ""
	}
	return i.orgs[org]
}

func (i *Installations) AppID() string {
	if i == nil {
		return ""
	}
	return i.appID
}

func (i *Installations) PrivateKeyPath() string {
	if i == nil {
		return ""
	}
	return i.privateKeyPath
}

type TokenCacheConfig struct {
	RefreshBuffer time.Duration // How early to refresh before expiration
	JWTExpiration time.Duration // GitHub allows max 10 minutes
}

func DefaultTokenCacheConfig() TokenCacheConfig {
	return TokenCacheConfig{
		RefreshBuffer: 5 * time.Minute,
		JWTExpiration: 10 * time.Minute,
	}
}
