// Package s3client provides shared S3 connection configuration and minio client construction.
//
// One global Config block feeds a ClientProvider that lazily builds a *minio.Client shared by all consumers.
package s3client

import (
	"context"
	"crypto/tls"
	"net/http"
	"sync"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/block/cachew/internal/logging"
)

// Config holds S3 connection parameters shared by all consumers, as a global HCL block (e.g. `hcl:"s3,block"`).
type Config struct {
	Endpoint      string `hcl:"endpoint,optional" help:"S3 endpoint URL (e.g., s3.amazonaws.com or localhost:9000)." default:"s3.amazonaws.com"`
	Region        string `hcl:"region,optional" help:"S3 region." default:"us-west-2"`
	UseSSL        bool   `hcl:"use-ssl,optional" help:"Use SSL for S3 connections." default:"true"`
	SkipSSLVerify bool   `hcl:"skip-ssl-verify,optional" help:"Skip SSL certificate verification." default:"false"`
}

// ClientProvider is a function that lazily creates a singleton *minio.Client.
type ClientProvider func() (*minio.Client, error)

// NewClientProvider returns a ClientProvider that constructs the minio client at most once from config.
func NewClientProvider(ctx context.Context, config Config) ClientProvider {
	return sync.OnceValues(func() (*minio.Client, error) {
		return NewClient(ctx, config)
	})
}

// NewClient constructs a *minio.Client from the given Config.
//
// Credentials come from the standard AWS chain: AWS_* environment variables, ~/.aws/credentials, then the
// EC2 instance or ECS container IAM role.
func NewClient(ctx context.Context, config Config) (*minio.Client, error) {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing shared S3 client",
		"endpoint", config.Endpoint,
		"region", config.Region,
		"use-ssl", config.UseSSL,
		"skip-ssl-verify", config.SkipSSLVerify,
	)

	// Create default transport for credential chain
	defaultTransport, err := minio.DefaultTransport(config.UseSSL)
	if err != nil {
		return nil, errors.Errorf("failed to create default transport: %w", err)
	}

	// Apply SSL verification settings if needed
	var transport http.RoundTripper
	if config.SkipSSLVerify {
		customTransport := defaultTransport.Clone()
		if customTransport.TLSClientConfig == nil {
			customTransport.TLSClientConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
		} else {
			customTransport.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
		customTransport.TLSClientConfig.InsecureSkipVerify = true
		transport = customTransport
		defaultTransport = customTransport
	}

	// Use AWS credential chain
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.EnvAWS{},
			&credentials.FileAWSCredentials{},
			&credentials.IAM{
				Client: &http.Client{
					Transport: defaultTransport,
				},
			},
		})

	options := &minio.Options{
		Creds:           creds,
		Secure:          config.UseSSL,
		Region:          config.Region,
		TrailingHeaders: true,
	}

	if transport != nil {
		options.Transport = transport
	}

	mc, err := minio.New(config.Endpoint, options)
	if err != nil {
		return nil, errors.Errorf("failed to create minio client: %w", err)
	}

	return mc, nil
}
