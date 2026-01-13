package cache

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/textproto"
	"os"
	"time"

	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/block/sfptc/internal/logging"
)

func init() {
	Register("s3", NewS3)
}

type S3Config struct {
	Bucket        string        `hcl:"bucket" help:"S3 bucket name."`
	Endpoint      string        `hcl:"endpoint,optional" help:"S3 endpoint URL (e.g., s3.amazonaws.com or localhost:9000)." default:"s3.amazonaws.com"`
	Region        string        `hcl:"region,optional" help:"S3 region (defaults to us-west-2)." default:"us-west-2"`
	UseSSL        bool          `hcl:"use-ssl,optional" help:"Use SSL for S3 connections (defaults to true)." default:"true"`
	SkipSSLVerify bool          `hcl:"skip-ssl-verify,optional" help:"Skip SSL certificate verification (defaults to false)." default:"false"`
	MaxTTL        time.Duration `hcl:"max-ttl,optional" help:"Maximum time-to-live for entries in the S3 cache (defaults to 1 hour)." default:"1h"`
}

type S3 struct {
	logger *slog.Logger
	config S3Config
	client *minio.Client
}

var _ Cache = (*S3)(nil)

// NewS3 creates a new S3-based cache instance using the minio SDK.
//
// config.Endpoint and config.Bucket MUST be set.
//
// The standard AWS credential chain is used for authentication, which includes:
//  1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
//  2. AWS credentials file (~/.aws/credentials)
//  3. IAM role from EC2 instance metadata or ECS container credentials
//
// This [Cache] implementation stores cache entries in an S3-compatible object storage service.
// Metadata (headers and expiration time) are stored as object user metadata. The implementation
// uses the lightweight minio-go SDK to reduce overhead compared to the AWS SDK.
func NewS3(ctx context.Context, config S3Config) (*S3, error) {
	logging.FromContext(ctx).InfoContext(ctx, "Constructing S3 cache",
		"endpoint", config.Endpoint,
		"bucket", config.Bucket,
		"region", config.Region,
		"use-ssl", config.UseSSL,
		"max-ttl", config.MaxTTL)

	// Create default transport for credential chain
	defaultTransport, err := minio.DefaultTransport(config.UseSSL)
	if err != nil {
		return nil, errors.Errorf("failed to create default transport: %w", err)
	}

	// Apply SSL verification settings if needed
	var transport http.RoundTripper
	if config.SkipSSLVerify {
		// Clone the default transport and disable SSL verification
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
			&credentials.EnvAWS{},             // Check AWS environment variables
			&credentials.FileAWSCredentials{}, // Check ~/.aws/credentials
			&credentials.IAM{
				Client: &http.Client{
					Transport: defaultTransport,
				},
			}, // Check EC2 instance metadata or ECS container credentials
		})

	// Create minio client options
	options := &minio.Options{
		Creds:  creds,
		Secure: config.UseSSL,
		Region: config.Region,
	}

	// Only set custom transport if needed (for SkipSSLVerify)
	if transport != nil {
		options.Transport = transport
	}

	client, err := minio.New(config.Endpoint, options)
	if err != nil {
		return nil, errors.Errorf("failed to create minio client: %w", err)
	}

	// Verify bucket exists
	exists, err := client.BucketExists(ctx, config.Bucket)
	if err != nil {
		return nil, errors.Errorf("failed to check if bucket exists: %w", err)
	}
	if !exists {
		return nil, errors.Errorf("bucket %s does not exist", config.Bucket)
	}

	return &S3{
		logger: logging.FromContext(ctx),
		config: config,
		client: client,
	}, nil
}

func (s *S3) String() string {
	return fmt.Sprintf("s3:%s/%s", s.config.Endpoint, s.config.Bucket)
}

func (s *S3) Close() error {
	return nil
}

func (s *S3) Open(ctx context.Context, key Key) (io.ReadCloser, textproto.MIMEHeader, error) {
	objectName := key.String()

	// Get object info to check metadata
	objInfo, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, errors.Errorf("failed to stat object: %w", err)
	}

	// Check if object has expired
	// Note: UserMetadata keys are returned WITHOUT the "X-Amz-Meta-" prefix by minio-go
	expiresAtStr := objInfo.UserMetadata["Expires-At"]
	if expiresAtStr != "" {
		var expiresAt time.Time
		if err := expiresAt.UnmarshalText([]byte(expiresAtStr)); err == nil {
			if time.Now().After(expiresAt) {
				// Object expired, delete it and return not found
				return nil, nil, errors.Join(os.ErrNotExist, s.Delete(ctx, key))
			}
		}
	}

	// Retrieve headers from metadata
	// Note: UserMetadata keys are returned WITHOUT the "X-Amz-Meta-" prefix by minio-go
	headers := make(textproto.MIMEHeader)
	if headersJSON := objInfo.UserMetadata["Headers"]; headersJSON != "" {
		if err := json.Unmarshal([]byte(headersJSON), &headers); err != nil {
			return nil, nil, errors.Errorf("failed to unmarshal headers: %w", err)
		}
	}

	// Get object
	obj, err := s.client.GetObject(ctx, s.config.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, nil, errors.Errorf("failed to get object: %w", err)
	}

	return obj, headers, nil
}

func (s *S3) Create(ctx context.Context, key Key, headers textproto.MIMEHeader, ttl time.Duration) (io.WriteCloser, error) {
	if ttl > s.config.MaxTTL || ttl == 0 {
		ttl = s.config.MaxTTL
	}

	expiresAt := time.Now().Add(ttl)

	return &s3Writer{
		s3:        s,
		key:       key,
		buf:       &bytes.Buffer{},
		expiresAt: expiresAt,
		headers:   headers,
		ctx:       ctx,
	}, nil
}

func (s *S3) Delete(ctx context.Context, key Key) error {
	objectName := key.String()

	// Check if object exists first
	_, err := s.client.StatObject(ctx, s.config.Bucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code == "NoSuchKey" {
			return os.ErrNotExist
		}
		return errors.Errorf("failed to stat object: %w", err)
	}

	err = s.client.RemoveObject(ctx, s.config.Bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return errors.Errorf("failed to remove object: %w", err)
	}

	return nil
}

type s3Writer struct {
	s3        *S3
	key       Key
	buf       *bytes.Buffer
	expiresAt time.Time
	headers   textproto.MIMEHeader
	ctx       context.Context
}

func (w *s3Writer) Write(p []byte) (int, error) {
	return errors.WithStack2(w.buf.Write(p))
}

func (w *s3Writer) Close() error {
	// Check if context was cancelled
	if err := w.ctx.Err(); err != nil {
		return errors.Wrap(err, "create operation cancelled")
	}

	objectName := w.key.String()

	// Prepare user metadata
	userMetadata := make(map[string]string)

	// Store expiration time
	expiresAtBytes, err := w.expiresAt.MarshalText()
	if err != nil {
		return errors.Errorf("failed to marshal expiration time: %w", err)
	}
	userMetadata["Expires-At"] = string(expiresAtBytes)

	// Store headers as JSON
	if len(w.headers) > 0 {
		headersJSON, err := json.Marshal(w.headers)
		if err != nil {
			return errors.Errorf("failed to marshal headers: %w", err)
		}
		userMetadata["Headers"] = string(headersJSON)
	}

	// Upload object
	_, err = w.s3.client.PutObject(
		w.ctx,
		w.s3.config.Bucket,
		objectName,
		w.buf,
		int64(w.buf.Len()),
		minio.PutObjectOptions{
			UserMetadata: userMetadata,
		},
	)
	if err != nil {
		return errors.Errorf("failed to put object: %w", err)
	}

	return nil
}
