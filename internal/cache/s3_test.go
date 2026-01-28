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
	rustfsPort     = "19000"
	rustfsAddr     = "localhost:" + rustfsPort
	rustfsUsername = "rustfsadmin"
	rustfsPassword = "rustfsadmin"
	rustfsBucket   = "test-bucket"
)

// startRustfs starts a rustfs server and returns a cleanup function.
func startRustfs(t *testing.T) {
	t.Helper()

	dir := t.TempDir()

	// Start rustfs server
	cmd := exec.CommandContext(t.Context(), "rustfs",
		"--address", ":"+rustfsPort,
		"--access-key", rustfsUsername,
		"--secret-key", rustfsPassword,
		dir,
	)

	err := cmd.Start()
	assert.NoError(t, err)

	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	// Wait for rustfs to be ready
	waitForRustfs(t)

	// Create test bucket
	createBucket(t)
}

// waitForRustfs waits for the rustfs server to be ready.
func waitForRustfs(t *testing.T) {
	t.Helper()

	client, err := minio.New(rustfsAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(rustfsUsername, rustfsPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal(errors.New("timed out waiting for rustfs to start"))
		case <-ticker.C:
			_, err := client.ListBuckets(t.Context())
			if err == nil {
				return
			}
		}
	}
}

// createBucket creates the test bucket in rustfs.
func createBucket(t *testing.T) {
	t.Helper()

	client, err := minio.New(rustfsAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(rustfsUsername, rustfsPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	exists, err := client.BucketExists(t.Context(), rustfsBucket)
	assert.NoError(t, err)

	if !exists {
		err = client.MakeBucket(t.Context(), rustfsBucket, minio.MakeBucketOptions{})
		assert.NoError(t, err)
	}
}

// cleanBucket removes all objects from the bucket.
func cleanBucket(t *testing.T) {
	t.Helper()

	client, err := minio.New(rustfsAddr, &minio.Options{
		Creds:  credentials.NewStaticV4(rustfsUsername, rustfsPassword, ""),
		Secure: false,
	})
	assert.NoError(t, err)

	objectsCh := client.ListObjects(t.Context(), rustfsBucket, minio.ListObjectsOptions{Recursive: true})
	for obj := range objectsCh {
		if obj.Err != nil {
			continue
		}
		_ = client.RemoveObject(t.Context(), rustfsBucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

// TestS3Cache tests the S3 cache implementation using rustfs.
//
// This test starts a rustfs server per test run.
// The rustfs binary must be available in PATH (managed by Hermit).
//
// The rustfs server:
// - Starts once per test run
// - Uses credentials: rustfsadmin/rustfsadmin
// - Listens on port 19000
// - Stores data in a temporary directory
// - Cleans up automatically after the test completes
func TestS3Cache(t *testing.T) {
	startRustfs(t)

	cachetest.Suite(t, func(t *testing.T) cache.Cache {
		_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelDebug})

		// Clean bucket to ensure test isolation
		cleanBucket(t)

		// Set credentials via environment variables for the AWS credential chain
		t.Setenv("AWS_ACCESS_KEY_ID", rustfsUsername)
		t.Setenv("AWS_SECRET_ACCESS_KEY", rustfsPassword)

		c, err := cache.NewS3(ctx, cache.S3Config{
			Endpoint:         rustfsAddr,
			Bucket:           rustfsBucket,
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

	startRustfs(t)

	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})

	// Clean bucket to ensure test isolation
	cleanBucket(t)

	// Set credentials via environment variables for the AWS credential chain
	t.Setenv("AWS_ACCESS_KEY_ID", rustfsUsername)
	t.Setenv("AWS_SECRET_ACCESS_KEY", rustfsPassword)

	c, err := cache.NewS3(ctx, cache.S3Config{
		Endpoint:         rustfsAddr,
		Bucket:           rustfsBucket,
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
		// Concurrency limited to 1 due to rustfs bug with concurrent access to the same key.
		// Real MinIO/S3 handles concurrency correctly.
		Concurrency: 1,
		TTL:         5 * time.Minute,
	})
}
