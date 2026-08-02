package githubapp

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"

	"github.com/block/cachew/gitcredential"
)

type gitCredentialProvider struct {
	tokenManager *TokenManager
}

// NewGitCredentialProvider adapts GitHub App tokens to repository-scoped Git credentials.
func NewGitCredentialProvider(tokenManager *TokenManager) gitcredential.Provider {
	return gitCredentialProvider{tokenManager: tokenManager}
}

func (p gitCredentialProvider) Credential(ctx context.Context, repositoryURL string) (gitcredential.Credential, bool, error) {
	if p.tokenManager == nil {
		return gitcredential.Credential{}, false, nil
	}
	u, err := url.Parse(repositoryURL)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || !strings.EqualFold(u.Hostname(), "github.com") {
		return gitcredential.Credential{}, false, nil //nolint:nilerr // Invalid and non-GitHub URLs do not match this adapter.
	}
	token, err := p.tokenManager.GetTokenForURL(ctx, repositoryURL)
	if err != nil || token == "" {
		return gitcredential.Credential{}, false, nil //nolint:nilerr // Preserve the existing fallback to system Git credentials.
	}
	urlScope, err := gitcredential.NormalizeRepositoryURLScope(repositoryURL)
	if err != nil {
		return gitcredential.Credential{}, false, nil //nolint:nilerr // Non-canonical URLs cannot receive a scoped credential.
	}
	authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token))
	return gitcredential.Credential{Authorization: authorization, URLScope: urlScope}, true, nil
}
