package githubapp

import (
	"log/slog"
	"net/http"
	"testing"
)

// NewTokenManagerForTest creates a TokenManager with a custom API base URL and HTTP client for testing.
func NewTokenManagerForTest(t *testing.T, configs []Config, logger *slog.Logger, apiBase string, httpClient *http.Client) *TokenManager {
	t.Helper()

	tm, err := newTokenManager(configs, logger, apiBase, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}
