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

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

// TokenManagerProvider is a function that lazily creates a singleton TokenManager.
type TokenManagerProvider func() (*TokenManager, error)

// NewTokenManagerProvider creates a provider that lazily initializes a TokenManager
// from one or more GitHub App configurations.
func NewTokenManagerProvider(configs []Config, logger *slog.Logger) TokenManagerProvider {
	return sync.OnceValues(func() (*TokenManager, error) {
		return newTokenManager(configs, logger, githubAPIBase, httputil.UpstreamClient())
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

	listMu             sync.Mutex // serialises installation listings
	installationMu     sync.RWMutex
	installations      map[string]string // lowercased account login -> installation ID, nil until listed
	installationOwners map[string]string // installation ID -> account login

	mu     sync.RWMutex
	tokens map[string]*cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// TokenManager manages GitHub App installation tokens across one or more apps.
type TokenManager struct {
	apps        []*appState
	fallbackApp *appState
	fallbackOrg string
}

func newTokenManager(configs []Config, logger *slog.Logger, apiBase string, httpClient *http.Client) (*TokenManager, error) {
	tm := &TokenManager{}

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

		app := &appState{
			appID:        config.AppID,
			jwtGenerator: jwtGen,
			cacheConfig:  cacheConfig,
			httpClient:   NewAPIClient(httpClient),
			apiBase:      apiBase,
			tokens:       make(map[string]*cachedToken),
		}
		tm.apps = append(tm.apps, app)

		logger.Info("GitHub App configured", "app_id", config.AppID)
		if config.FallbackOrg != "" && tm.fallbackApp == nil {
			tm.fallbackApp = app
			tm.fallbackOrg = config.FallbackOrg
			logger.Info("GitHub App fallback configured", "fallback_org", config.FallbackOrg, "app_id", config.AppID)
		}
	}
	if len(tm.apps) == 0 {
		return nil, nil //nolint:nilnil
	}

	return tm, nil
}

// GetTokenForOrg returns an installation token for the given GitHub organization.
// Installations are listed via the GitHub API on first use and refreshed by
// RunInstallationRefresh. Orgs without an installation get the fallback org's token.
func (tm *TokenManager) GetTokenForOrg(ctx context.Context, org string) (string, error) {
	token, _, err := tm.GetTokenForOrgWithInstallation(ctx, org)
	return token, err
}

// Installation is the GitHub App installation that minted a token.
type Installation struct {
	// ID is unique across GitHub Apps.
	ID    string
	Owner string
}

// GetTokenForOrgWithInstallation also returns the installation that minted the token.
func (tm *TokenManager) GetTokenForOrgWithInstallation(ctx context.Context, org string) (string, Installation, error) {
	if tm == nil {
		return "", Installation{}, errors.New("token manager not initialized")
	}

	logger := logging.FromContext(ctx)
	for _, app := range tm.apps {
		installationID, err := app.lookupInstallationID(ctx, org)
		if err != nil {
			logger.DebugContext(ctx, "GitHub App installation lookup failed", "org", org, "app_id", app.appID, "error", err)
			continue
		}
		return app.getInstallationToken(ctx, installationID)
	}

	if tm.fallbackApp != nil {
		logger.InfoContext(ctx, "Using fallback org token", "requested_org", org, "fallback_org", tm.fallbackOrg)
		installationID, err := tm.fallbackApp.lookupInstallationID(ctx, tm.fallbackOrg)
		if err != nil {
			return "", Installation{}, errors.Wrap(err, "resolve fallback installation")
		}
		return tm.fallbackApp.getInstallationToken(ctx, installationID)
	}

	return "", Installation{}, errors.Errorf("no GitHub App installation found for org: %s", org)
}

// RefreshInstallations relists the installations of every configured app. An app
// whose listing fails keeps its previously known installations.
func (tm *TokenManager) RefreshInstallations(ctx context.Context) error {
	var errs []error
	for _, app := range tm.apps {
		if err := app.refreshInstallations(ctx); err != nil {
			errs = append(errs, errors.Wrapf(err, "app %s", app.appID))
		}
	}
	return errors.Join(errs...)
}

const installationRefreshInterval = 5 * time.Minute
const installationRefreshTimeout = 30 * time.Second

// RunInstallationRefresh calls RefreshInstallations now and every five minutes until
// ctx is cancelled, so installations added later are found. It passes failures to
// onError, which must be non-nil.
func (tm *TokenManager) RunInstallationRefresh(ctx context.Context, onError func(error)) {
	ticker := time.NewTicker(installationRefreshInterval)
	defer ticker.Stop()
	for {
		refreshCtx, cancel := context.WithTimeout(ctx, installationRefreshTimeout)
		err := tm.RefreshInstallations(refreshCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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

func (a *appState) getInstallationToken(ctx context.Context, installationID string) (string, Installation, error) {
	a.installationMu.RLock()
	owner := a.installationOwners[installationID]
	a.installationMu.RUnlock()
	if owner == "" {
		return "", Installation{}, errors.Errorf("no owner for installation: %s", installationID)
	}
	installation := Installation{ID: installationID, Owner: owner}
	logger := logging.FromContext(ctx).With("org", owner, "app_id", a.appID)

	a.mu.RLock()
	cached, exists := a.tokens[installationID]
	a.mu.RUnlock()

	if exists && time.Now().Add(a.cacheConfig.RefreshBuffer).Before(cached.expiresAt) {
		logger.DebugContext(ctx, "Using cached GitHub App token")
		return cached.token, installation, nil
	}

	logger.DebugContext(ctx, "Fetching new GitHub App installation token", "installation_id", installationID)

	token, expiresAt, err := a.fetchInstallationToken(ctx, owner, installationID)
	if err != nil {
		return "", Installation{}, errors.Wrap(err, "fetch installation token")
	}

	a.mu.Lock()
	a.tokens[installationID] = &cachedToken{
		token:     token,
		expiresAt: expiresAt,
	}
	a.mu.Unlock()

	logger.InfoContext(ctx, "GitHub App token refreshed", "expires_at", expiresAt)

	return token, installation, nil
}

func (a *appState) fetchInstallationToken(ctx context.Context, org, installationID string) (string, time.Time, error) {
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
	req = req.WithContext(httputil.WithCooldownScope(req.Context(), "github-app:"+a.appID))
	req.Header.Set("X-Github-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(WithAPIMetrics(req, org, "create_installation_token"))
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

// lookupInstallationID returns the app's installation ID for the given org or user
// from the listed installations, listing them first if they have never been listed.
func (a *appState) lookupInstallationID(ctx context.Context, org string) (string, error) {
	if err := a.ensureInstallations(ctx); err != nil {
		return "", err
	}
	a.installationMu.RLock()
	id, ok := a.installations[strings.ToLower(org)]
	a.installationMu.RUnlock()
	if !ok {
		return "", errors.Errorf("no GitHub App installation found for %s", org)
	}
	return id, nil
}

func (a *appState) ensureInstallations(ctx context.Context) error {
	if a.listed() {
		return nil
	}
	a.listMu.Lock()
	defer a.listMu.Unlock()
	if a.listed() {
		return nil
	}
	return a.listInstallations(ctx)
}

func (a *appState) listed() bool {
	a.installationMu.RLock()
	defer a.installationMu.RUnlock()
	return a.installations != nil
}

// refreshInstallations replaces the known installations with the app's current
// installations, keeping the previous set if listing fails.
func (a *appState) refreshInstallations(ctx context.Context) error {
	a.listMu.Lock()
	defer a.listMu.Unlock()
	return a.listInstallations(ctx)
}

func (a *appState) listInstallations(ctx context.Context) error {
	jwt, err := a.jwtGenerator.GenerateJWT()
	if err != nil {
		return errors.Wrap(err, "generate JWT")
	}
	owners := map[string]string{}
	for page := 1; ; page++ {
		installations, err := a.listInstallationsPage(ctx, jwt, page)
		if err != nil {
			return err
		}
		for _, installation := range installations {
			if installation.Account.Login != "" {
				owners[strconv.FormatInt(installation.ID, 10)] = installation.Account.Login
			}
		}
		if len(installations) < installationsPerPage {
			break
		}
	}
	a.setInstallations(owners)
	return nil
}

const installationsPerPage = 100

type listedInstallation struct {
	ID      int64 `json:"id"`
	Account struct {
		Login string `json:"login"`
	} `json:"account"`
}

func (a *appState) listInstallationsPage(ctx context.Context, jwt string, page int) ([]listedInstallation, error) {
	url := fmt.Sprintf("%s/app/installations?per_page=%d&page=%d", a.apiBase, installationsPerPage, page)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+jwt)
	req = req.WithContext(httputil.WithCooldownScope(req.Context(), "github-app:"+a.appID))
	req.Header.Set("X-Github-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(WithAPIMetrics(req, "unknown", "list_installations"))
	if err != nil {
		return nil, errors.Wrap(err, "list installations")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("GitHub API returned status %d for %s", resp.StatusCode, url)
	}
	var installations []listedInstallation
	if err := json.NewDecoder(resp.Body).Decode(&installations); err != nil {
		return nil, errors.Wrap(err, "decode installations")
	}
	return installations, nil
}

// setInstallations replaces the known installations, keyed by installation ID with
// the account login as the owner.
func (a *appState) setInstallations(owners map[string]string) {
	installations := make(map[string]string, len(owners))
	for id, owner := range owners {
		installations[strings.ToLower(owner)] = id
	}
	a.installationMu.Lock()
	defer a.installationMu.Unlock()
	a.installations = installations
	a.installationOwners = owners
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
