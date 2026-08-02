package azureblob_test

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/alecthomas/assert/v2"
	"github.com/google/uuid"

	"github.com/block/cachew/internal/azureblobclient"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	metadataazureblob "github.com/block/cachew/internal/metadatadb/azureblob"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
)

func TestAzureBlobBackend(t *testing.T) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("AZURE_STORAGE_CONNECTION_STRING is not set")
	}
	metadatadbtest.Suite(t, func(t *testing.T, count int) []metadatadb.Backend {
		_, ctx := logging.Configure(t.Context(), logging.Config{})
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		assert.NoError(t, err)
		container := "cachew" + strings.ReplaceAll(uuid.NewString(), "-", "")
		_, err = client.CreateContainer(ctx, container, nil)
		assert.NoError(t, err)
		t.Cleanup(func() { _, _ = client.DeleteContainer(t.Context(), container, nil) })
		provider := azureblobclient.NewClientProvider(ctx, azureblobclient.Config{})
		backends := make([]metadatadb.Backend, count)
		for index := range count {
			backends[index], err = metadataazureblob.New(ctx, provider, metadataazureblob.Config{Container: container})
			assert.NoError(t, err)
		}
		return backends
	})
}

func TestAzureBlobBackendConverges(t *testing.T) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("AZURE_STORAGE_CONNECTION_STRING is not set")
	}
	_, ctx := logging.Configure(t.Context(), logging.Config{})
	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	assert.NoError(t, err)
	container := "cachew" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = client.CreateContainer(ctx, container, nil)
	assert.NoError(t, err)
	defer func() { _, _ = client.DeleteContainer(t.Context(), container, nil) }()
	provider := azureblobclient.NewClientProvider(ctx, azureblobclient.Config{})
	first, err := metadataazureblob.New(ctx, provider, metadataazureblob.Config{Container: container})
	assert.NoError(t, err)
	second, err := metadataazureblob.New(ctx, provider, metadataazureblob.Config{Container: container})
	assert.NoError(t, err)
	firstStore := metadatadb.New(ctx, first)
	secondStore := metadatadb.New(ctx, second)
	assert.NoError(t, metadatadb.NewInt(firstStore.Namespace("test"), "count").Add(2))
	assert.NoError(t, metadatadb.NewInt(secondStore.Namespace("test"), "count").Add(3))
	var wg sync.WaitGroup
	errorsCh := make(chan error, 2)
	wg.Go(func() { errorsCh <- metadatadb.NewInt(firstStore.Namespace("test"), "count").Add(1) })
	wg.Go(func() { errorsCh <- metadatadb.NewInt(secondStore.Namespace("test"), "count").Add(1) })
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		assert.NoError(t, err)
	}
	assert.NoError(t, firstStore.Namespace("test").Flush(ctx))
	assert.Equal(t, int64(7), metadatadb.NewInt(firstStore.Namespace("test"), "count").Get())
}
