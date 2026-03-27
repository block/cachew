// Package minitest provides a reusable MinIO test server via Docker.
package minitest

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	containerName = "minio-test"
	Port          = "19000"
	Addr          = "localhost:" + Port
	Username      = "minioadmin"
	Password      = "minioadmin"
	Bucket        = "test-bucket"
)

// Start ensures a shared MinIO container is running, creating it if needed.
// The container persists across tests and packages.
func Start(t *testing.T) {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", Password)

	// If it's already up and healthy, nothing to do.
	if isHealthy(t) {
		return
	}

	// Try to start — if the container already exists (from another parallel
	// package), docker run fails but that's fine, we just wait for it.
	cmd := exec.CommandContext(t.Context(), "docker", "run", "-d",
		"--name", containerName,
		"-p", Port+":9000",
		"-e", "MINIO_ROOT_USER="+Username,
		"-e", "MINIO_ROOT_PASSWORD="+Password,
		"minio/minio", "server", "/data",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		// Only fatal if the container doesn't exist at all.
		if !strings.Contains(string(output), "already in use") {
			t.Fatalf("failed to start minio container: %v\n%s", err, output)
		}
	}

	waitForReady(t)
	createBucket(t)
}

func isHealthy(t *testing.T) bool {
	t.Helper()
	client := Client(t)
	_, err := client.ListBuckets(t.Context())
	return err == nil
}

// Client returns a minio client connected to the test server.
func Client(t *testing.T) *minio.Client {
	t.Helper()
	client, err := minio.New(Addr, &minio.Options{
		Creds:  credentials.NewStaticV4(Username, Password, ""),
		Secure: false,
	})
	assert.NoError(t, err)
	return client
}

// CleanBucket removes all objects from the test bucket.
func CleanBucket(t *testing.T) {
	t.Helper()
	client := Client(t)
	for obj := range client.ListObjects(t.Context(), Bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		if err := client.RemoveObject(t.Context(), Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			t.Logf("failed to remove object %s: %v", obj.Key, err)
		}
	}
}

func waitForReady(t *testing.T) {
	t.Helper()
	client := Client(t)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout:
			t.Fatal(errors.New("timed out waiting for minio to start"))
		case <-ticker.C:
			if _, err := client.ListBuckets(t.Context()); err == nil {
				return
			}
		}
	}
}

func createBucket(t *testing.T) {
	t.Helper()
	client := Client(t)
	exists, err := client.BucketExists(t.Context(), Bucket)
	assert.NoError(t, err)
	if !exists {
		assert.NoError(t, client.MakeBucket(t.Context(), Bucket, minio.MakeBucketOptions{}))
	}
}
