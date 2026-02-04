// Package githubapp provides GitHub App authentication and token management.
package githubapp

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/alecthomas/errors"
)

type Config struct {
	AppID             string `hcl:"app-id" help:"GitHub App ID"`
	PrivateKeyPath    string `hcl:"private-key-path" help:"Path to GitHub App private key (PEM format)"`
	InstallationsJSON string `hcl:"installations-json" help:"JSON string mapping org names to installation IDs"`
}

// Installations maps organization names to GitHub App installation IDs.
type Installations struct {
	appID          string
	privateKeyPath string
	orgs           map[string]string
}

// NewInstallations creates an Installations instance from config.
func NewInstallations(config Config, logger *slog.Logger) (*Installations, error) {
	if config.InstallationsJSON == "" {
		return nil, errors.New("installations-json is required")
	}

	var orgs map[string]string
	if err := json.Unmarshal([]byte(config.InstallationsJSON), &orgs); err != nil {
		logger.Error("Failed to parse installations-json",
			"error", err,
			"installations_json", config.InstallationsJSON)
		return nil, errors.Wrap(err, "parse installations-json")
	}

	if len(orgs) == 0 {
		return nil, errors.New("installations-json must contain at least one organization")
	}

	logger.Info("GitHub App config initialized",
		"app_id", config.AppID,
		"private_key_path", config.PrivateKeyPath,
		"installations", len(orgs))

	return &Installations{
		appID:          config.AppID,
		privateKeyPath: config.PrivateKeyPath,
		orgs:           orgs,
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
