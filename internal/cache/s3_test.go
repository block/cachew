package cache_test

import (
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/cache/cachetest"
	"github.com/block/cachew/internal/logging"
)

const (
	minioPort     = "19000"
	minioAddr     = "localhost:" + minioPort
	minioUsername = "minioadmin"
	minioPassword = "minioadmin"
	minioBucket   = "test-bucket"
)

// startMinio starts a MinIO server via Docker.
func startMinio(t *testing.T) {
	t.Helper()

	containerName := "minio-test-" + t.Name()

	// Start MinIO container
	cmd := exec.CommandContext(t.Context(), "docker", "run", "-d",
		"--name", containerName,
		"-p", minioPort+":9000",
		"-e", "MINIO_ROOT_USER="+minioUsername,
		"-e", "MINIO_ROOT_PASSWORD="+minioPassword,
		"minio/minio", "server", "/data",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to start minio container: %v\n%s", err, output)
	}

	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	})

	// Wait for MinIO to be ready
	waitForMinio(t)

	// Create test bucket
	createBucket(t)
}

// waitForMinio waits for the MinIO server to be ready.
func waitForMinio(t *testing.T) {
	t.Helper()

	client, err := minio.New(minioAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUsername, minioPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal(errors.New("timed out waiting for minio to start"))
		case <-ticker.C:
			_, err := client.ListBuckets(t.Context())
			if err == nil {
				return
			}
		}
	}
}

// createBucket creates the test bucket in MinIO.
func createBucket(t *testing.T) {
	t.Helper()

	client, err := minio.New(minioAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUsername, minioPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	exists, err := client.BucketExists(t.Context(), minioBucket)
	assert.NoError(t, err)

	if !exists {
		err = client.MakeBucket(t.Context(), minioBucket, minio.MakeBucketOptions{})
		assert.NoError(t, err)
	}
}

// cleanBucket removes all objects from the bucket.
func cleanBucket(t *testing.T) {
	t.Helper()

	client, err := minio.New(minioAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(minioUsername, minioPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	objectsCh := client.ListObjects(t.Context(), minioBucket, minio.ListObjectsOptions{Recursive: true})
	for obj := range objectsCh {
		if obj.Err != nil {
			continue
		}
		_ = client.RemoveObject(t.Context(), minioBucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

// TestS3Cache tests the S3 cache implementation using MinIO in Docker.
func TestS3Cache(t *testing.T) {
	startMinio(t)

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

		// Clean bucket to ensure test isolation
		cleanBucket(t)

		// Set credentials via environment variables for the AWS credential chain
		t.Setenv("AWS_ACCESS_KEY_ID", minioUsername)
		t.Setenv("AWS_SECRET_ACCESS_KEY", minioPassword)

		c, err := cache.NewS3(ctx, cache.S3Config{
			Endpoint:         minioAddr,
			Bucket:           minioBucket,
			Region:           "",
			UseSSL:           false,
			MaxTTL:           100 * time.Millisecond,
			UploadPartSizeMB: 16,
		})
		assert.NoError(t, err)
		return c
	})
}

func TestS3CacheSoak(t *testing.T) {
	if os.Getenv("SOAK_TEST") == "" {
		t.Skip("Skipping soak test; set SOAK_TEST=1 to run")
	}

	startMinio(t)

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

	// Clean bucket to ensure test isolation
	cleanBucket(t)

	// Set credentials via environment variables for the AWS credential chain
	t.Setenv("AWS_ACCESS_KEY_ID", minioUsername)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioPassword)

	c, err := cache.NewS3(ctx, cache.S3Config{
		Endpoint:         minioAddr,
		Bucket:           minioBucket,
		Region:           "",
		UseSSL:           false,
		MaxTTL:           10 * time.Minute,
		UploadPartSizeMB: 16,
	})
	assert.NoError(t, err)
	defer c.Close()

	cachetest.Soak(t, c, cachetest.SoakConfig{
		Duration:         30 * time.Second,
		NumObjects:       100,
		MaxObjectSize:    64 * 1024,
		MinObjectSize:    1024,
		OverwritePercent: 30,
		Concurrency:      4,
		TTL:              5 * time.Minute,
	})
}
