package cache_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	testcontainersminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/block/sfptc/internal/cache"
	"github.com/block/sfptc/internal/cache/cachetest"
	"github.com/block/sfptc/internal/logging"
)

var (
	minioContainer *testcontainersminio.MinioContainer
	minioEndpoint  string
	minioBucket    = "test-bucket"
	minioUsername  = "minioadmin"
	minioPassword  = "minioadmin"
)

// TestMain manages the MinIO container lifecycle for the entire test package.
// The container is started once before all tests run and terminated after all tests complete.
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Check for opt-out environment variable
	if os.Getenv("SKIP_TESTCONTAINERS") != "" {
		os.Exit(m.Run())
	}

	// Start MinIO container
	var err error
	minioContainer, err = testcontainersminio.Run(ctx,
		"minio/minio:RELEASE.2024-01-16T16-07-38Z",
		testcontainersminio.WithUsername(minioUsername),
		testcontainersminio.WithPassword(minioPassword),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start MinIO container: %v\n", err)
		fmt.Fprintf(os.Stderr, "Ensure Docker is running and accessible.\n")
		os.Exit(1)
	}

	// Get connection details
	connStr, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get MinIO connection string: %v\n", err)
		_ = minioContainer.Terminate(ctx)
		os.Exit(1)
	}

	// ConnectionString returns just "host:port", but we need to handle it properly
	// The minio-go SDK expects just the host:port without protocol
	parsedURL, err := url.Parse(connStr)
	switch {
	case err != nil:
		// If it can't be parsed as URL, it might already be just host:port
		minioEndpoint = connStr
	case parsedURL.Host != "":
		// If it parsed successfully and has a Host, use that
		minioEndpoint = parsedURL.Host
	default:
		// Otherwise use the raw string
		minioEndpoint = connStr
	}

	// Create test bucket
	if err := createBucket(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create test bucket: %v\n", err)
		_ = minioContainer.Terminate(ctx)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if err := minioContainer.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to terminate MinIO container: %v\n", err)
	}

	os.Exit(code)
}

// createBucket creates the test bucket in the MinIO container.
func createBucket(ctx context.Context) error {
	// Use the minio-go SDK (already in dependencies) to create bucket
	client, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUsername, minioPassword, ""),
		Secure: false,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio client: %w", err)
	}

	// Create bucket if it doesn't exist
	exists, err := client.BucketExists(ctx, minioBucket)
	if err != nil {
		return fmt.Errorf("failed to check if bucket exists: %w", err)
	}

	if !exists {
		if err := client.MakeBucket(ctx, minioBucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return nil
}

// TestS3Cache tests the S3 cache implementation using testcontainers-go.
//
// This test automatically starts a MinIO container using testcontainers-go.
// Docker must be running for these tests to execute.
//
// To skip these tests (e.g., during development without Docker):
//
//	SKIP_TESTCONTAINERS=1 go test ./internal/cache
//
// The MinIO container:
// - Starts once per test package run
// - Uses credentials: minioadmin/minioadmin
// - Listens on a random available port
// - Cleans up automatically after tests complete
func TestS3Cache(t *testing.T) {
	if minioContainer == nil {
		t.Skip("MinIO container not available - Docker may not be running or SKIP_TESTCONTAINERS is set")
	}

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

		// Clean up any existing objects in the bucket before creating a new cache instance
		// This ensures test isolation since all tests share the same bucket
		client, err := minio.New(minioEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(minioUsername, minioPassword, ""),
			Secure: false,
		})
		assert.NoError(t, err)

		// Remove all objects from the bucket
		objectsCh := client.ListObjects(ctx, minioBucket, minio.ListObjectsOptions{Recursive: true})
		for obj := range objectsCh {
			if obj.Err != nil {
				t.Logf("Error listing objects: %v", obj.Err)
				continue
			}
			err := client.RemoveObject(ctx, minioBucket, obj.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Logf("Error removing object %s: %v", obj.Key, err)
			}
		}

		// Set credentials via environment variables for the AWS credential chain
		t.Setenv("AWS_ACCESS_KEY_ID", minioUsername)
		t.Setenv("AWS_SECRET_ACCESS_KEY", minioPassword)

		useSSL := false
		c, err := cache.NewS3(ctx, cache.S3Config{
			Endpoint: minioEndpoint,
			Bucket:   minioBucket,
			Region:   "",
			UseSSL:   &useSSL, // MinIO container serves HTTP, not HTTPS
			MaxTTL:   100 * time.Millisecond,
		})
		assert.NoError(t, err)
		return c
	})
}
