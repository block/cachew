package cache_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/alecthomas/assert/v2"
	"github.com/google/uuid"

	"github.com/block/cachew/internal/azureblobclient"
	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

func TestAzureBlobCache(t *testing.T) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("AZURE_STORAGE_CONNECTION_STRING is not set")
	}
	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{})
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		assert.NoError(t, err)
		container := "cachew" + strings.ReplaceAll(uuid.NewString(), "-", "")
		_, err = client.CreateContainer(ctx, container, nil)
		assert.NoError(t, err)
		t.Cleanup(func() { _, _ = client.DeleteContainer(t.Context(), container, nil) })
		backend, err := cache.NewAzureBlob(ctx, cache.AzureBlobConfig{Container: container, MaxTTL: 3 * time.Second},
			azureblobclient.NewClientProvider(ctx, azureblobclient.Config{ConnectionStringEnv: "AZURE_STORAGE_CONNECTION_STRING"}))
		assert.NoError(t, err)
		return backend
	})
}
