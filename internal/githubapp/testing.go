package githubapp

import (
	"log/slog"
	"net/http"
	"testing"
)

// NewTokenManagerForTest creates a TokenManager with a custom API base URL and HTTP client for testing.
func NewTokenManagerForTest(t *testing.T, configs []Config, logger *slog.Logger, apiBase string, httpClient *http.Client) *TokenManager {
	t.Helper()

	tm, err := newTokenManager(configs, logger)
	if err != nil {
		t.Fatal(err)
	}

	for _, app := range tm.apps {
		app.apiBase = apiBase
		app.httpClient = httpClient
	}

	return tm
}
