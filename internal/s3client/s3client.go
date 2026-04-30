// Package s3client provides shared S3 connection configuration and minio client construction.
//
// A single Config block is declared in the global configuration and a
// ClientProvider lazily constructs a singleton *minio.Client that can be
// shared across multiple consumers (cache backends, strategies, etc.).
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

// Config holds S3 connection parameters that are shared across all consumers.
// It is intended to be embedded as a global HCL block (e.g. `hcl:"s3,block"`).
type Config struct {
	Endpoint      string `hcl:"endpoint,optional" help:"S3 endpoint URL (e.g., s3.amazonaws.com or localhost:9000)." default:"s3.amazonaws.com"`
	Region        string `hcl:"region,optional" help:"S3 region." default:"us-west-2"`
	UseSSL        bool   `hcl:"use-ssl,optional" help:"Use SSL for S3 connections." default:"true"`
	SkipSSLVerify bool   `hcl:"skip-ssl-verify,optional" help:"Skip SSL certificate verification." default:"false"`
}

// ClientProvider is a function that lazily creates a singleton *minio.Client.
type ClientProvider func() (*minio.Client, error)

// NewClientProvider returns a ClientProvider that will construct the minio
// client at most once using the supplied Config.
func NewClientProvider(ctx context.Context, config Config) ClientProvider {
	return sync.OnceValues(func() (*minio.Client, error) {
		return NewClient(ctx, config)
	})
}

// NewClient constructs a *minio.Client from the given Config.
// The standard AWS credential chain is used:
//  1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
//  2. AWS credentials file (~/.aws/credentials)
//  3. IAM role from EC2 instance metadata or ECS container credentials
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
