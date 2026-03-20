package githubapp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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

const githubAPIBase = "https://api.github.com"

// appState holds token management state for a single GitHub App.
type appState struct {
	appID        string
	jwtGenerator *JWTGenerator
	cacheConfig  TokenCacheConfig
	httpClient   *http.Client
	apiBase      string

	installationMu    sync.RWMutex
	installationCache map[string]string // org -> installation ID (dynamically discovered)

	mu     sync.RWMutex
	tokens map[string]*cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// TokenManager manages GitHub App installation tokens across one or more apps.
type TokenManager struct {
	mu       sync.RWMutex
	orgToApp map[string]*appState

	apps        []*appState // all configured apps, for dynamic installation discovery
	fallbackApp *appState
	fallbackOrg string
}

func newTokenManager(configs []Config, logger *slog.Logger) (*TokenManager, error) {
	var apps []*appState

	for _, config := range configs {
		hasAny := config.AppID != "" || config.PrivateKeyPath != ""
		if !hasAny {
			continue
		}
		if config.AppID == "" || config.PrivateKeyPath == "" {
			return nil, errors.Errorf("github-app: incomplete configuration (app-id=%q, private-key-path=%q)",
				config.AppID, config.PrivateKeyPath)
		}

		cacheConfig := DefaultTokenCacheConfig()
		jwtGen, err := NewJWTGenerator(config.AppID, config.PrivateKeyPath, cacheConfig.JWTExpiration)
		if err != nil {
			return nil, errors.Wrapf(err, "github app %q", config.AppID)
		}

		apps = append(apps, &appState{
			appID:             config.AppID,
			jwtGenerator:      jwtGen,
			cacheConfig:       cacheConfig,
			httpClient:        http.DefaultClient,
			apiBase:           githubAPIBase,
			installationCache: make(map[string]string),
			tokens:            make(map[string]*cachedToken),
		})

		logger.Info("GitHub App configured", "app_id", config.AppID)
	}

	if len(apps) == 0 {
		return nil, nil //nolint:nilnil
	}

	tm := &TokenManager{
		orgToApp: make(map[string]*appState),
		apps:     apps,
	}

	for i, config := range configs {
		if config.FallbackOrg != "" {
			if i >= len(apps) {
				continue
			}
			tm.fallbackApp = apps[i]
			tm.fallbackOrg = config.FallbackOrg
			logger.Info("GitHub App fallback configured", "fallback_org", config.FallbackOrg, "app_id", config.AppID)
			break
		}
	}

	return tm, nil
}

// GetTokenForOrg returns an installation token for the given GitHub organization.
// It dynamically discovers the installation ID via the GitHub API on first use,
// caches the result, and falls back to the fallback org's token for orgs where
// the app is not installed.
func (tm *TokenManager) GetTokenForOrg(ctx context.Context, org string) (string, error) {
	if tm == nil {
		return "", errors.New("token manager not initialized")
	}

	logger := logging.FromContext(ctx)

	// Check cached discovery first
	tm.mu.RLock()
	app, ok := tm.orgToApp[org]
	tm.mu.RUnlock()
	if ok {
		return app.getToken(ctx, org)
	}

	// Discover installation via GitHub API
	for _, app := range tm.apps {
		installationID, err := app.lookupInstallationID(ctx, org)
		if err != nil {
			logger.DebugContext(ctx, "Dynamic installation lookup failed", "org", org, "app_id", app.appID, "error", err)
			continue
		}

		logger.InfoContext(ctx, "Dynamically discovered GitHub App installation", "org", org, "app_id", app.appID, "installation_id", installationID)

		// Cache the mapping for future requests
		tm.mu.Lock()
		tm.orgToApp[org] = app
		tm.mu.Unlock()
		return app.getToken(ctx, org)
	}

	// Fall back to fallback org
	if tm.fallbackApp != nil {
		logger.InfoContext(ctx, "Using fallback org token", "requested_org", org, "fallback_org", tm.fallbackOrg)
		return tm.fallbackApp.getToken(ctx, tm.fallbackOrg)
	}

	return "", errors.Errorf("no GitHub App installation found for org: %s", org)
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
	logger := logging.FromContext(ctx).With("org", org, "app_id", a.appID)

	a.installationMu.RLock()
	installationID := a.installationCache[org]
	a.installationMu.RUnlock()
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

	logger.DebugContext(ctx, "Fetching new GitHub App installation token", "installation_id", installationID)

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

	logger.InfoContext(ctx, "GitHub App token refreshed", "expires_at", expiresAt)

	return token, nil
}

func (a *appState) fetchInstallationToken(ctx context.Context, installationID string) (string, time.Time, error) {
	jwt, err := a.jwtGenerator.GenerateJWT()
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "generate JWT")
	}

	url := fmt.Sprintf("%s/app/installations/%s/access_tokens", a.apiBase, installationID)
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

// lookupInstallationID queries the GitHub API to find the installation ID for the
// given org. It tries /orgs/{org}/installation first, then /users/{user}/installation.
// Results are cached in installationCache.
func (a *appState) lookupInstallationID(ctx context.Context, org string) (string, error) {
	// Check cache first
	a.installationMu.RLock()
	if id, ok := a.installationCache[org]; ok {
		a.installationMu.RUnlock()
		return id, nil
	}
	a.installationMu.RUnlock()

	jwt, err := a.jwtGenerator.GenerateJWT()
	if err != nil {
		return "", errors.Wrap(err, "generate JWT")
	}

	// Try org endpoint first, then user endpoint
	for _, endpoint := range []string{
		fmt.Sprintf("%s/orgs/%s/installation", a.apiBase, org),
		fmt.Sprintf("%s/users/%s/installation", a.apiBase, org),
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return "", errors.Wrap(err, "create request")
		}

		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Authorization", "Bearer "+jwt)
		req.Header.Set("X-Github-Api-Version", "2022-11-28")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return "", errors.Wrap(err, "execute request")
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return "", errors.Errorf("GitHub API returned status %d for %s", resp.StatusCode, endpoint)
		}

		var result struct {
			ID int64 `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", errors.Wrap(err, "decode response")
		}

		installationID := strconv.FormatInt(result.ID, 10)

		a.installationMu.Lock()
		a.installationCache[org] = installationID
		a.installationMu.Unlock()

		return installationID, nil
	}

	return "", errors.Errorf("no GitHub App installation found for %s", org)
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
