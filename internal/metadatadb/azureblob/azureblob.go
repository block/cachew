// Package azureblob implements a native Azure Blob metadata backend.
package azureblob

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/azureblobclient"
	"github.com/block/cachew/internal/metadatadb"
)

const (
	maxCASAttempts   = 20
	operationTimeout = 30 * time.Second
)

// Config configures the Azure Blob metadata backend.
type Config struct {
	Container string `hcl:"container" help:"Azure Blob container name."`
}

// Register registers the Azure Blob metadata backend.
func Register(r *metadatadb.Registry, provider azureblobclient.ClientProvider) {
	metadatadb.Register(r, "azure-blob", "Stores metadata state in Azure Blob Storage",
		func(ctx context.Context, config Config) (*Backend, error) { return New(ctx, provider, config) })
}

// Backend stores each metadata namespace as a conditionally updated JSON blob.
type Backend struct {
	client    *azblob.Client
	container string
	mu        sync.Mutex
	locks     map[string]*sync.Mutex
	state     map[string]map[string]any
}

var _ metadatadb.Backend = (*Backend)(nil)

// New creates an Azure Blob metadata backend and verifies its container.
func New(ctx context.Context, provider azureblobclient.ClientProvider, config Config) (*Backend, error) {
	if config.Container == "" {
		return nil, errors.New("container is required")
	}
	client, err := provider()
	if err != nil {
		return nil, errors.Wrap(err, "create Azure Blob client")
	}
	if _, err := client.ServiceClient().NewContainerClient(config.Container).GetProperties(ctx, nil); err != nil {
		return nil, errors.Wrap(err, "get Azure Blob container properties")
	}
	return &Backend{client: client, container: config.Container, locks: make(map[string]*sync.Mutex),
		state: make(map[string]map[string]any)}, nil
}

func (b *Backend) Apply(ctx context.Context, namespace string, ops ...metadatadb.Op) error {
	if len(ops) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	lock := b.namespaceLock(namespace)
	lock.Lock()
	defer lock.Unlock()
	for attempt := range maxCASAttempts {
		state, etag, err := b.read(ctx, namespace)
		if err != nil {
			return err
		}
		for _, op := range ops {
			metadatadb.ApplyOp(state, op)
		}
		data, err := json.Marshal(state)
		if err != nil {
			return errors.Wrap(err, "marshal metadata state")
		}
		conditions := &blob.ModifiedAccessConditions{}
		if etag == nil {
			conditions.IfNoneMatch = ptrETag(azcore.ETagAny)
		} else {
			conditions.IfMatch = etag
		}
		_, err = b.client.UploadBuffer(ctx, b.container, b.key(namespace), data, &azblob.UploadBufferOptions{
			AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: conditions},
		})
		if err == nil {
			b.mu.Lock()
			b.state[namespace] = state
			b.mu.Unlock()
			return nil
		}
		if !azureBlobPreconditionFailed(err) {
			return errors.Wrap(err, "write metadata state")
		}
		select {
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		}
	}
	return errors.New("metadata state remained contended")
}

func (b *Backend) Query(ctx context.Context, namespace string, query metadatadb.ReadOp, target any) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	lock := b.namespaceLock(namespace)
	lock.Lock()
	defer lock.Unlock()
	state, _, readErr := b.read(ctx, namespace)
	if readErr == nil {
		b.mu.Lock()
		b.state[namespace] = state
		b.mu.Unlock()
	} else {
		b.mu.Lock()
		state = b.state[namespace]
		if state == nil {
			state = make(map[string]any)
		}
		b.mu.Unlock()
	}
	return errors.Wrap(metadatadb.QueryStateInto(state, query, target), "azure blob query")
}

func (b *Backend) Flush(ctx context.Context, namespace string) error {
	lock := b.namespaceLock(namespace)
	lock.Lock()
	defer lock.Unlock()
	state, _, err := b.read(ctx, namespace)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.state[namespace] = state
	b.mu.Unlock()
	return nil
}

func (b *Backend) Close(context.Context) error { return nil }

func (b *Backend) read(ctx context.Context, namespace string) (map[string]any, *azcore.ETag, error) {
	response, err := b.client.DownloadStream(ctx, b.container, b.key(namespace), nil)
	if azureBlobNotFound(err) {
		return make(map[string]any), nil, nil
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "read metadata state")
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	state := make(map[string]any)
	if err := decoder.Decode(&state); err != nil {
		return nil, nil, errors.Wrap(err, "decode metadata state")
	}
	if state == nil {
		return nil, nil, errors.New("metadata state is null")
	}
	return state, response.ETag, nil
}

func (b *Backend) namespaceLock(namespace string) *sync.Mutex {
	b.mu.Lock()
	defer b.mu.Unlock()
	lock := b.locks[namespace]
	if lock == nil {
		lock = &sync.Mutex{}
		b.locks[namespace] = lock
	}
	return lock
}

func (b *Backend) key(namespace string) string {
	if namespace == "" {
		return ".metadata/azure-blob/.root.json"
	}
	return ".metadata/azure-blob/" + base64.RawURLEncoding.EncodeToString([]byte(namespace)) + ".json"
}

func azureBlobNotFound(err error) bool {
	var responseError *azcore.ResponseError
	return errors.As(err, &responseError) && responseError.StatusCode == http.StatusNotFound
}

func azureBlobPreconditionFailed(err error) bool {
	var responseError *azcore.ResponseError
	return errors.As(err, &responseError) &&
		(responseError.StatusCode == http.StatusPreconditionFailed || responseError.StatusCode == http.StatusConflict)
}

func ptrETag(etag azcore.ETag) *azcore.ETag { return &etag }
