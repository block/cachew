package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/azureblobclient"
	"github.com/block/cachew/internal/httputil"
)

// AzureBlobConfig configures an Azure Blob cache backend.
type AzureBlobConfig struct {
	Container         string        `hcl:"container" help:"Azure Blob container name."`
	MaxTTL            time.Duration `hcl:"max-ttl,optional" default:"1h" help:"Maximum cache entry TTL."`
	UploadConcurrency uint16        `hcl:"upload-concurrency,optional" default:"4"`
	UploadBlockSizeMB int64         `hcl:"upload-block-size-mb,optional" default:"16"`
}

// RegisterAzureBlob registers the native Azure Blob cache backend.
func RegisterAzureBlob(r *Registry, provider azureblobclient.ClientProvider) {
	Register(r, "azure-blob", "Caches objects in Azure Blob Storage", func(ctx context.Context, config AzureBlobConfig) (*AzureBlob, error) {
		return NewAzureBlob(ctx, config, provider)
	})
}

type azureBlobMeta struct {
	Headers   http.Header `json:"headers"`
	ExpiresAt time.Time   `json:"expires_at"`
	Tag       string      `json:"tag"`
}

// AzureBlob is a native Azure Blob Storage cache.
type AzureBlob struct {
	config    AzureBlobConfig
	client    *azblob.Client
	namespace Namespace
}

var _ Cache = (*AzureBlob)(nil)

// NewAzureBlob creates an Azure Blob cache and verifies its container.
func NewAzureBlob(ctx context.Context, config AzureBlobConfig, provider azureblobclient.ClientProvider) (*AzureBlob, error) {
	if config.MaxTTL == 0 {
		config.MaxTTL = time.Hour
	}
	if config.UploadConcurrency == 0 {
		config.UploadConcurrency = 4
	}
	if config.UploadBlockSizeMB == 0 {
		config.UploadBlockSizeMB = 16
	}
	if config.MaxTTL < 0 {
		return nil, errors.New("max-ttl must be positive")
	}
	if config.UploadBlockSizeMB < 0 {
		return nil, errors.New("upload-block-size-mb must be positive")
	}
	if config.Container == "" {
		return nil, errors.New("container is required")
	}
	client, err := provider()
	if err != nil {
		return nil, errors.Wrap(err, "create Azure Blob client")
	}
	_, err = client.ServiceClient().NewContainerClient(config.Container).GetProperties(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "get Azure Blob container properties")
	}
	return &AzureBlob{config: config, client: client}, nil
}

func (c *AzureBlob) String() string { return "azure-blob:" + c.config.Container }
func (c *AzureBlob) Close() error   { return nil }

func (c *AzureBlob) Namespace(namespace Namespace) Cache {
	clone := *c
	clone.namespace = namespace
	return &clone
}

func (c *AzureBlob) keyPath(namespace Namespace, key Key) string {
	hexKey := key.String()
	if namespace == "" {
		return hexKey[:2] + "/" + hexKey
	}
	return string(namespace) + "/" + hexKey[:2] + "/" + hexKey
}

func (c *AzureBlob) metaPath(namespace Namespace, key Key) string {
	return c.keyPath(namespace, key) + ".meta"
}

func (c *AzureBlob) properties(ctx context.Context, key Key) (blob.GetPropertiesResponse, http.Header, error) {
	name := c.keyPath(c.namespace, key)
	properties, err := c.client.ServiceClient().NewContainerClient(c.config.Container).NewBlobClient(name).GetProperties(ctx, nil)
	if azureBlobNotFound(err) {
		return blob.GetPropertiesResponse{}, nil, os.ErrNotExist
	}
	if err != nil {
		return blob.GetPropertiesResponse{}, nil, errors.Wrap(err, "get blob properties")
	}
	meta, err := c.readMeta(ctx, c.namespace, key)
	if err != nil {
		return blob.GetPropertiesResponse{}, nil, err
	}
	storedTag := azureMetadataValue(properties.Metadata, "cachewtag")
	if storedTag != meta.Tag {
		return blob.GetPropertiesResponse{}, nil, os.ErrNotExist
	}
	if time.Now().After(meta.ExpiresAt) {
		return blob.GetPropertiesResponse{}, nil, errors.Join(os.ErrNotExist, c.Delete(ctx, key))
	}
	headers := meta.Headers.Clone()
	if properties.LastModified != nil && headers.Get("Last-Modified") == "" {
		headers.Set("Last-Modified", properties.LastModified.UTC().Format(http.TimeFormat))
	}
	if properties.ContentLength != nil {
		headers.Set("Content-Length", strconv.FormatInt(*properties.ContentLength, 10))
	}
	return properties, headers, nil
}

func (c *AzureBlob) Stat(ctx context.Context, key Key, opts ...Option) (http.Header, error) {
	_, headers, err := c.properties(ctx, key)
	if err != nil {
		return nil, err
	}
	if result, err := conditionalShortCircuit(headers, opts); err != nil {
		return result, err
	}
	return headers, nil
}

func (c *AzureBlob) Open(ctx context.Context, key Key, opts ...Option) (io.ReadCloser, http.Header, error) {
	properties, headers, err := c.properties(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if result, err := conditionalShortCircuit(headers, opts); err != nil {
		return nil, result, err
	}
	size := *properties.ContentLength
	start, length, partial, err := rangeShortCircuit(headers, size, opts)
	if err != nil {
		return nil, headers, err
	}
	downloadOptions := &azblob.DownloadStreamOptions{}
	if partial {
		downloadOptions.Range = blob.HTTPRange{Offset: start, Count: length}
	}
	if properties.ETag != nil {
		downloadOptions.AccessConditions = &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: properties.ETag}}
	}
	response, err := c.client.DownloadStream(ctx, c.config.Container, c.keyPath(c.namespace, key), downloadOptions)
	if azureBlobNotFound(err) || azureBlobPreconditionFailed(err) {
		return nil, nil, os.ErrNotExist
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "download blob")
	}
	return response.Body, headers, nil
}

func (c *AzureBlob) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration, opts ...Option) (Writer, error) {
	if ttl == 0 || ttl > c.config.MaxTTL {
		ttl = c.config.MaxTTL
	}
	clonedHeaders := httputil.FilterHeaders(headers, httputil.TransportHeaders...)
	if err := setCreateETag(clonedHeaders, opts...); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp("", "cachew-azure-blob-*")
	if err != nil {
		return nil, errors.Wrap(err, "create Azure Blob upload temp file")
	}
	var tagBytes [16]byte
	if _, err := rand.Read(tagBytes[:]); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name()) // #nosec G703 -- path is returned by os.CreateTemp.
		return nil, errors.Wrap(err, "generate blob tag")
	}
	return &azureBlobWriter{cache: c, ctx: ctx, key: key, file: file, headers: clonedHeaders,
		expiresAt: time.Now().Add(ttl).UTC(), tag: hex.EncodeToString(tagBytes[:])}, nil
}

type azureBlobWriter struct {
	cache     *AzureBlob
	ctx       context.Context
	key       Key
	file      *os.File
	headers   http.Header
	expiresAt time.Time
	tag       string
	closed    bool
}

func (w *azureBlobWriter) Write(data []byte) (int, error) {
	n, err := w.file.Write(data)
	return n, errors.WithStack(err)
}
func (w *azureBlobWriter) Abort(_ error) error { return w.cleanup() }

func (w *azureBlobWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer os.Remove(w.file.Name()) // #nosec G703 -- path is returned by os.CreateTemp.
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return errors.Wrap(err, "sync Azure Blob upload")
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		_ = w.file.Close()
		return errors.Wrap(err, "rewind Azure Blob upload")
	}
	metadata := map[string]*string{"cachewtag": &w.tag}
	_, err := w.cache.client.UploadFile(w.ctx, w.cache.config.Container, w.cache.keyPath(w.cache.namespace, w.key), w.file,
		&azblob.UploadFileOptions{BlockSize: w.cache.config.UploadBlockSizeMB << 20, Concurrency: w.cache.config.UploadConcurrency,
			Metadata: metadata})
	_ = w.file.Close()
	if err != nil {
		return errors.Wrap(err, "upload Azure Blob data")
	}
	meta := azureBlobMeta{Headers: w.headers, ExpiresAt: w.expiresAt, Tag: w.tag}
	if err := w.cache.writeMeta(w.ctx, w.cache.namespace, w.key, meta); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 30*time.Second)
		defer cancel()
		return errors.Join(err, w.cache.removeDataIfTag(cleanupCtx, w.key, w.tag))
	}
	return nil
}

func (w *azureBlobWriter) cleanup() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return errors.Join(w.file.Close(), os.Remove(w.file.Name())) // #nosec G703 -- path is returned by os.CreateTemp.
}

func (c *AzureBlob) removeDataIfTag(ctx context.Context, key Key, tag string) error {
	name := c.keyPath(c.namespace, key)
	blobClient := c.client.ServiceClient().NewContainerClient(c.config.Container).NewBlobClient(name)
	properties, err := blobClient.GetProperties(ctx, nil)
	if azureBlobNotFound(err) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get uncommitted blob properties")
	}
	if azureMetadataValue(properties.Metadata, "cachewtag") != tag {
		return nil
	}
	_, err = blobClient.Delete(ctx, &blob.DeleteOptions{AccessConditions: &blob.AccessConditions{
		ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfMatch: properties.ETag},
	}})
	if azureBlobPreconditionFailed(err) || azureBlobNotFound(err) {
		return nil
	}
	return errors.Wrap(err, "delete uncommitted blob")
}

func (c *AzureBlob) writeMeta(ctx context.Context, namespace Namespace, key Key, meta azureBlobMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "marshal Azure Blob metadata")
	}
	_, err = c.client.UploadBuffer(ctx, c.config.Container, c.metaPath(namespace, key), data, nil)
	return errors.Wrap(err, "upload Azure Blob metadata")
}

func (c *AzureBlob) readMeta(ctx context.Context, namespace Namespace, key Key) (azureBlobMeta, error) {
	response, err := c.client.DownloadStream(ctx, c.config.Container, c.metaPath(namespace, key), nil)
	if azureBlobNotFound(err) {
		return azureBlobMeta{}, os.ErrNotExist
	}
	if err != nil {
		return azureBlobMeta{}, errors.Wrap(err, "download Azure Blob metadata")
	}
	defer response.Body.Close()
	var meta azureBlobMeta
	if err := json.NewDecoder(response.Body).Decode(&meta); err != nil {
		return azureBlobMeta{}, errors.Wrap(err, "decode Azure Blob metadata")
	}
	return meta, nil
}

func (c *AzureBlob) Delete(ctx context.Context, key Key) error {
	_, err := c.client.DeleteBlob(ctx, c.config.Container, c.keyPath(c.namespace, key), nil)
	if err != nil && !azureBlobNotFound(err) {
		return errors.Wrap(err, "delete Azure Blob data")
	}
	_, metaErr := c.client.DeleteBlob(ctx, c.config.Container, c.metaPath(c.namespace, key), nil)
	if metaErr != nil && !azureBlobNotFound(metaErr) {
		return errors.Wrap(metaErr, "delete Azure Blob metadata")
	}
	return nil
}

func (c *AzureBlob) Invalidate(ctx context.Context, key Key) error { return c.Delete(ctx, key) }
func (c *AzureBlob) Stats(context.Context) (Stats, error)          { return Stats{}, ErrStatsUnavailable }
func (c *AzureBlob) ListNamespaces(context.Context) ([]string, error) {
	return nil, ErrStatsUnavailable
}

func azureMetadataValue(metadata map[string]*string, key string) string {
	for metadataKey, value := range metadata {
		if strings.EqualFold(metadataKey, key) && value != nil {
			return *value
		}
	}
	return ""
}

func azureBlobNotFound(err error) bool {
	var responseError *azcore.ResponseError
	return errors.As(err, &responseError) && responseError.StatusCode == http.StatusNotFound
}

func azureBlobPreconditionFailed(err error) bool {
	var responseError *azcore.ResponseError
	return errors.As(err, &responseError) && responseError.StatusCode == http.StatusPreconditionFailed
}
