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

// NewTokenManagerProvider creates a provider that lazily initializes a TokenManager.
func NewTokenManagerProvider(config Config, logger *slog.Logger) TokenManagerProvider {
	return sync.OnceValues(func() (*TokenManager, error) {
		if config.AppID == "" || config.PrivateKeyPath == "" || config.InstallationsJSON == "" {
			return nil, nil // Not configured, return nil without error
		}

		installations, err := NewInstallations(config, logger)
		if err != nil {
			return nil, errors.Wrap(err, "create installations")
		}

		return NewTokenManager(installations, DefaultTokenCacheConfig())
	})
}

type TokenManager struct {
	installations *Installations
	cacheConfig   TokenCacheConfig
	jwtGenerator  *JWTGenerator
	httpClient    *http.Client

	mu     sync.RWMutex
	tokens map[string]*cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

func NewTokenManager(installations *Installations, cacheConfig TokenCacheConfig) (*TokenManager, error) {
	if !installations.IsConfigured() {
		return nil, errors.New("GitHub App not configured")
	}

	jwtGenerator, err := NewJWTGenerator(installations.AppID(), installations.PrivateKeyPath(), cacheConfig.JWTExpiration)
	if err != nil {
		return nil, errors.Wrap(err, "create JWT generator")
	}

	return &TokenManager{
		installations: installations,
		cacheConfig:   cacheConfig,
		jwtGenerator:  jwtGenerator,
		httpClient:    http.DefaultClient,
		tokens:        make(map[string]*cachedToken),
	}, nil
}

func (tm *TokenManager) GetTokenForOrg(ctx context.Context, org string) (string, error) {
	if tm == nil {
		return "", errors.New("token manager not initialized")
	}
	logger := logging.FromContext(ctx).With(slog.String("org", org))

	installationID := tm.installations.GetInstallationID(org)
	if installationID == "" {
		return "", errors.Errorf("no GitHub App installation configured for org: %s", org)
	}

	tm.mu.RLock()
	cached, exists := tm.tokens[org]
	tm.mu.RUnlock()

	if exists && time.Now().Add(tm.cacheConfig.RefreshBuffer).Before(cached.expiresAt) {
		logger.DebugContext(ctx, "Using cached GitHub App token")
		return cached.token, nil
	}

	logger.DebugContext(ctx, "Fetching new GitHub App installation token",
		slog.String("installation_id", installationID))

	token, expiresAt, err := tm.fetchInstallationToken(ctx, installationID)
	if err != nil {
		return "", errors.Wrap(err, "fetch installation token")
	}

	tm.mu.Lock()
	tm.tokens[org] = &cachedToken{
		token:     token,
		expiresAt: expiresAt,
	}
	tm.mu.Unlock()

	logger.InfoContext(ctx, "GitHub App token refreshed",
		slog.Time("expires_at", expiresAt))

	return token, nil
}

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

func (tm *TokenManager) fetchInstallationToken(ctx context.Context, installationID string) (string, time.Time, error) {
	jwt, err := tm.jwtGenerator.GenerateJWT()
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

	resp, err := tm.httpClient.Do(req)
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
