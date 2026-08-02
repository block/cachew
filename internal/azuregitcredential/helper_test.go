package azuregitcredential_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/azuregitcredential"
)

const azureDevOpsAudience = "499b84ac-1321-427f-aa17-267ca6975798"

type tokenCredential struct {
	token   azcore.AccessToken
	options policy.TokenRequestOptions
	calls   int
	err     error
}

func (c *tokenCredential) GetToken(_ context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	c.calls++
	c.options = options
	return c.token, c.err
}

func TestHelperRun(t *testing.T) {
	credential := &tokenCredential{token: azcore.AccessToken{Token: "azure-token", ExpiresOn: time.Now().Add(time.Hour)}}
	helper, err := azuregitcredential.New(credential, "api://custom-audience")
	assert.NoError(t, err)

	stdin := bytes.NewBufferString("{\"version\":1,\"remote_url\":\"https://dev.azure.com/org/project/_git/repo\"}\n")
	stdout := &bytes.Buffer{}
	assert.NoError(t, helper.Run(t.Context(), stdin, stdout))
	assert.Equal(t, 1, credential.calls)
	assert.Equal(t, []string{"api://custom-audience/.default"}, credential.options.Scopes)

	var output struct {
		Version       int       `json:"version"`
		Authorization string    `json:"authorization"`
		ExpiresAt     time.Time `json:"expires_at"`
	}
	assert.NoError(t, json.Unmarshal(stdout.Bytes(), &output))
	assert.Equal(t, 1, output.Version)
	assert.Equal(t, "Bearer azure-token", output.Authorization)
	assert.True(t, credential.token.ExpiresOn.Equal(output.ExpiresAt))
}

func TestHelperRejectsNonAzureDevOpsRemoteBeforeGettingToken(t *testing.T) {
	credential := &tokenCredential{token: azcore.AccessToken{Token: "azure-token", ExpiresOn: time.Now().Add(time.Hour)}}
	helper, err := azuregitcredential.New(credential, azureDevOpsAudience)
	assert.NoError(t, err)

	stdin := bytes.NewBufferString("{\"version\":1,\"remote_url\":\"https://example.com/org/repo\"}\n")
	err = helper.Run(t.Context(), stdin, &bytes.Buffer{})
	assert.Error(t, err)
	assert.Equal(t, 0, credential.calls)
}

func TestNewRejectsInvalidAudience(t *testing.T) {
	credential := &tokenCredential{}
	_, err := azuregitcredential.New(credential, "invalid audience")
	assert.Error(t, err)
}

func TestHelperRejectsInvalidToken(t *testing.T) {
	tests := []azcore.AccessToken{
		{Token: "", ExpiresOn: time.Now().Add(time.Hour)},
		{Token: "token\nvalue", ExpiresOn: time.Now().Add(time.Hour)},
		{Token: "expired", ExpiresOn: time.Now().Add(-time.Minute)},
	}
	for _, token := range tests {
		credential := &tokenCredential{token: token}
		helper, err := azuregitcredential.New(credential, azureDevOpsAudience)
		assert.NoError(t, err)
		stdin := bytes.NewBufferString("{\"version\":1,\"remote_url\":\"https://dev.azure.com/org/project/_git/repo\"}\n")
		assert.Error(t, helper.Run(t.Context(), stdin, &bytes.Buffer{}))
	}
}
