// Package azureblobclient provides shared Azure Blob client configuration.
package azureblobclient

import (
	"context"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/alecthomas/errors"
)

// Config configures an Azure Blob service client.
type Config struct {
	AccountURL          string `hcl:"account-url,optional" help:"Azure Blob service URL, for example https://account.blob.core.windows.net."`
	ConnectionStringEnv string `hcl:"connection-string-env,optional" default:"AZURE_STORAGE_CONNECTION_STRING" help:"Environment variable containing an Azure Storage connection string, primarily for Azurite."`
}

// ClientProvider lazily returns a shared Azure Blob client.
type ClientProvider func() (*azblob.Client, error)

// NewClientProvider creates a lazy Azure Blob client provider.
func NewClientProvider(_ context.Context, config Config) ClientProvider {
	if config.ConnectionStringEnv == "" {
		config.ConnectionStringEnv = "AZURE_STORAGE_CONNECTION_STRING"
	}
	return sync.OnceValues(func() (*azblob.Client, error) {
		if config.ConnectionStringEnv != "" {
			if connectionString := os.Getenv(config.ConnectionStringEnv); connectionString != "" {
				client, err := azblob.NewClientFromConnectionString(connectionString, nil)
				return client, errors.WithStack(err)
			}
		}
		if config.AccountURL == "" {
			return nil, errors.New("azure-blob account-url is required")
		}
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, errors.Wrap(err, "create Azure credential")
		}
		client, err := azblob.NewClient(config.AccountURL, credential, nil)
		return client, errors.WithStack(err)
	})
}
