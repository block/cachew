package githubapp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// TokenManagerProvider is a function that lazily creates a singleton TokenManager.
type TokenManagerProvider func() (*TokenManager, error)

// NewTokenManagerProvider creates a provider that lazily initializes a TokenManager
// from one or more GitHub App configurations.
func NewTokenManagerProvider(configs []Config, logger *slog.Logger) TokenManagerProvider {
	return sync.OnceValues(func() (*TokenManager, error) {
		return newTokenManager(configs, logger)
	})
}

// appState holds token management state for a single GitHub App.
type appState struct {
	appID        string
	jwtGenerator *JWTGenerator
	cacheConfig  TokenCacheConfig
	httpClient   *http.Client
	orgs         map[string]string // org -> installation ID

	mu     sync.RWMutex
	tokens map[string]*cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// TokenManager manages GitHub App installation tokens across one or more apps.
type TokenManager struct {
	orgToApp    map[string]*appState
	fallbackApp *appState
	fallbackOrg string
}

func newTokenManager(configs []Config, logger *slog.Logger) (*TokenManager, error) {
	orgToApp := map[string]*appState{}

	for _, config := range configs {
		hasAny := config.AppID != "" || config.PrivateKeyPath != "" || len(config.Installations) > 0
		hasAll := config.AppID != "" && config.PrivateKeyPath != "" && len(config.Installations) > 0
		if !hasAny {
			continue
		}
		if !hasAll {
			return nil, errors.Errorf("github-app: incomplete configuration (app-id=%q, private-key-path=%q, installations=%d)",
				config.AppID, config.PrivateKeyPath, len(config.Installations))
		}

		cacheConfig := DefaultTokenCacheConfig()
		jwtGen, err := NewJWTGenerator(config.AppID, config.PrivateKeyPath, cacheConfig.JWTExpiration)
		if err != nil {
			return nil, errors.Wrapf(err, "github app %q", config.AppID)
		}

		app := &appState{
			appID:        config.AppID,
			jwtGenerator: jwtGen,
			cacheConfig:  cacheConfig,
			httpClient:   http.DefaultClient,
			orgs:         config.Installations,
			tokens:       make(map[string]*cachedToken),
		}

		for org := range config.Installations {
			if existing, exists := orgToApp[org]; exists {
				return nil, errors.Errorf("org %q is configured in both github-app %q and %q", org, existing.appID, config.AppID)
			}
			orgToApp[org] = app
		}

		logger.Info("GitHub App configured",
			"app_id", config.AppID,
			"orgs", len(config.Installations))
	}

	if len(orgToApp) == 0 {
		return nil, nil //nolint:nilnil
	}

	tm := &TokenManager{orgToApp: orgToApp}

	for _, config := range configs {
		if config.FallbackOrg != "" {
			app, ok := orgToApp[config.FallbackOrg]
			if !ok {
				return nil, errors.Errorf("fallback-org %q is not in the installations map for app %q", config.FallbackOrg, config.AppID)
			}
			tm.fallbackApp = app
			tm.fallbackOrg = config.FallbackOrg
			logger.Info("GitHub App fallback configured",
				"fallback_org", config.FallbackOrg,
				"app_id", config.AppID)
			break
		}
	}

	return tm, nil
}

// GetTokenForOrg returns an installation token for the given GitHub organization.
// If no installation is configured for the org, it falls back to the fallback org's
// token to ensure authenticated rate limits.
func (tm *TokenManager) GetTokenForOrg(ctx context.Context, org string) (string, error) {
	if tm == nil {
		return "", errors.New("token manager not initialized")
	}

	app, ok := tm.orgToApp[org]
	if !ok {
		if tm.fallbackApp == nil {
			return "", errors.Errorf("no GitHub App configured for org: %s", org)
		}
		logging.FromContext(ctx).InfoContext(ctx, "Using fallback org token",
			slog.String("requested_org", org),
			slog.String("fallback_org", tm.fallbackOrg))
		return tm.fallbackApp.getToken(ctx, tm.fallbackOrg)
	}

	return app.getToken(ctx, org)
}

// GetTokenForURL extracts the org from a GitHub URL and returns an installation token.
func (tm *TokenManager) GetTokenForURL(ctx context.Context, url string) (string, error) {
	if tm == nil {
		return "", errors.New("token manager not initialized")
	}
	org, err := extractOrgFromURL(url)
	if err != nil {
		return "", err
	}

	return tm.GetTokenForOrg(ctx, org)
}

func (a *appState) getToken(ctx context.Context, org string) (string, error) {
	logger := logging.FromContext(ctx).With(slog.String("org", org), slog.String("app_id", a.appID))

	installationID := a.orgs[org]
	if installationID == "" {
		return "", errors.Errorf("no installation ID for org: %s", org)
	}

	a.mu.RLock()
	cached, exists := a.tokens[org]
	a.mu.RUnlock()

	if exists && time.Now().Add(a.cacheConfig.RefreshBuffer).Before(cached.expiresAt) {
		logger.DebugContext(ctx, "Using cached GitHub App token")
		return cached.token, nil
	}

	logger.DebugContext(ctx, "Fetching new GitHub App installation token",
		slog.String("installation_id", installationID))

	token, expiresAt, err := a.fetchInstallationToken(ctx, installationID)
	if err != nil {
		return "", errors.Wrap(err, "fetch installation token")
	}

	a.mu.Lock()
	a.tokens[org] = &cachedToken{
		token:     token,
		expiresAt: expiresAt,
	}
	a.mu.Unlock()

	logger.InfoContext(ctx, "GitHub App token refreshed",
		slog.Time("expires_at", expiresAt))

	return token, nil
}

func (a *appState) fetchInstallationToken(ctx context.Context, installationID string) (string, time.Time, error) {
	jwt, err := a.jwtGenerator.GenerateJWT()
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "generate JWT")
	}

	url := fmt.Sprintf("https://api.github.com/app/installations/%s/access_tokens", installationID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "create request")
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-Github-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return "", time.Time{}, errors.Errorf("GitHub API returned status %d", resp.StatusCode)
	}

	var result struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", time.Time{}, errors.Wrap(err, "decode response")
	}

	return result.Token, result.ExpiresAt, nil
}

func extractOrgFromURL(url string) (string, error) {
	url = strings.TrimPrefix(url, "https://")
	url = strings.TrimPrefix(url, "http://")
	url = strings.TrimPrefix(url, "git@")

	if !strings.HasPrefix(url, "github.com/") && !strings.HasPrefix(url, "github.com:") {
		return "", errors.Errorf("not a GitHub URL: %s", url)
	}
	url = strings.TrimPrefix(url, "github.com/")
	url = strings.TrimPrefix(url, "github.com:")

	parts := strings.Split(url, "/")
	if len(parts) < 1 || parts[0] == "" {
		return "", errors.Errorf("cannot extract org from URL: %s", url)
	}

	return parts[0], nil
}
