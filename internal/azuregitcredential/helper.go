// Package azuregitcredential implements the Cachew credential command protocol for Azure DevOps.
package azuregitcredential

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
)

// Helper validates repository requests and obtains Azure DevOps access tokens.
type Helper struct {
	credential azcore.TokenCredential
	scope      string
	now        func() time.Time
}

// New creates an Azure DevOps credential helper for an access-token audience.
func New(credential azcore.TokenCredential, audience string) (*Helper, error) {
	if credential == nil {
		return nil, errors.New("Azure token credential is required")
	}
	scope, err := scopeForAudience(audience)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Helper{
		credential: credential,
		scope:      scope,
		now:        time.Now,
	}, nil
}

// Credential obtains an Azure DevOps authorization for a canonical remote URL.
func (h *Helper) Credential(ctx context.Context, remoteURL string) (gitcredential.CommandResult, error) {
	if !isAzureDevOpsURL(remoteURL) {
		return gitcredential.CommandResult{}, errors.New("repository URL must use dev.azure.com")
	}

	token, err := h.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{h.scope}})
	if err != nil {
		return gitcredential.CommandResult{}, errors.Wrap(err, "get Azure DevOps access token")
	}
	if token.Token == "" || strings.ContainsAny(token.Token, "\r\n\x00") {
		return gitcredential.CommandResult{}, errors.New("Azure Identity returned an invalid access token")
	}
	if !token.ExpiresOn.After(h.now()) {
		return gitcredential.CommandResult{}, errors.New("Azure Identity returned an expired access token")
	}

	return gitcredential.CommandResult{
		Authorization: "Bearer " + token.Token,
		ExpiresAt:     token.ExpiresOn,
	}, nil
}

// Run handles one credential protocol request.
func (h *Helper) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	if err := gitcredential.ServeCommand(ctx, stdin, stdout, h); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func scopeForAudience(audience string) (string, error) {
	if audience == "" || strings.TrimSpace(audience) != audience || len(audience) > 2048 ||
		strings.ContainsAny(audience, " \t\r\n\x00") {
		return "", errors.New("invalid Azure access-token audience")
	}
	audience = strings.TrimSuffix(audience, "/")
	if audience == "" {
		return "", errors.New("invalid Azure access-token audience")
	}
	if strings.HasSuffix(audience, "/.default") {
		return audience, nil
	}
	return audience + "/.default", nil
}

func isAzureDevOpsURL(repositoryURL string) bool {
	return strings.HasPrefix(repositoryURL, "https://dev.azure.com/")
}
