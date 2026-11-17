// Package s3clienttest provides a reusable MinIO test server via Docker.
package s3clienttest

import (
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Connection settings for the shared MinIO test container.
const (
	containerName = "minio-test"
	Port          = "19000"
	Addr          = "localhost:" + Port
	Username      = "minioadmin"
	Password      = "minioadmin"
)

var bucketNameRe = regexp.MustCompile(`[^a-z0-9-]`)

// Start ensures a shared MinIO container is running, then creates and returns this test's deterministic bucket.
//
// The name derives from the calling package and t.Name() so packages can't collide in parallel. Stale objects
// from a crashed run are removed up front, and t.Cleanup removes objects when the test finishes.
func Start(t *testing.T) string {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", Password)

	if !isHealthy(t) {
		startContainer(t)
		waitForReady(t)
	}

	bucket := bucketName(t)
	client := Client(t)

	// Ensure the bucket exists, creating it if necessary.
	exists, err := client.BucketExists(t.Context(), bucket)
	assert.NoError(t, err)
	if !exists {
		assert.NoError(t, client.MakeBucket(t.Context(), bucket, minio.MakeBucketOptions{}))
	}

	// Remove any stale objects from a previous (possibly crashed) run.
	CleanBucket(t, bucket)
	t.Cleanup(func() { CleanBucket(t, bucket) })

	return bucket
}

const modulePrefix = "github.com/block/cachew/"

// bucketName returns a deterministic, S3-legal bucket name from the calling package and test name.
//
// The package is included because t.Name() alone is not unique across packages.
func bucketName(t *testing.T) string {
	t.Helper()

	// Walk up the call stack past this package to find the caller's function.
	pkg := "unknown"
	var pcs [10]uintptr
	n := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if !strings.Contains(frame.Function, "s3clienttest") {
			// frame.Function is e.g. "github.com/block/cachew/internal/cache_test.TestS3Cache"
			fn := strings.TrimPrefix(frame.Function, modulePrefix)
			if idx := strings.LastIndex(fn, "."); idx >= 0 {
				pkg = fn[:idx]
			}
			break
		}
		if !more {
			break
		}
	}

	raw := pkg + "-" + t.Name()
	return bucketNameRe.ReplaceAllString(strings.ToLower(raw), "-")
}

// CleanBucket removes all objects from the given bucket.
func CleanBucket(t *testing.T, bucket string) {
	t.Helper()
	client := Client(t)
	for obj := range client.ListObjects(t.Context(), bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		if err := client.RemoveObject(t.Context(), bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			t.Logf("failed to remove object %s: %v", obj.Key, err)
		}
	}
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

// startContainer starts the shared MinIO container, tolerating races with other test processes.
//
// After "name already in use", "docker start" can fail with "No such container" until the other process
// finishes creating it, so retry until the container is up or the deadline passes.
func startContainer(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		cmd := exec.CommandContext(t.Context(), "docker", "run", "-d",
			"--name", containerName,
			"-p", Port+":9000",
			"-e", "MINIO_ROOT_USER="+Username,
			"-e", "MINIO_ROOT_PASSWORD="+Password,
			"cgr.dev/chainguard/minio:latest", "server", "/data",
		)
		output, err := cmd.CombinedOutput()
		if err == nil {
			return
		}
		if !strings.Contains(string(output), "already in use") {
			t.Fatalf("failed to start minio container: %v\n%s", err, output)
		}
		// Container exists but may be stopped — try restarting it.
		if _, restartErr := exec.CommandContext(t.Context(), "docker", "start", containerName).CombinedOutput(); restartErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for minio container: %v\n%s", err, output)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func isHealthy(t *testing.T) bool {
	t.Helper()
	client := Client(t)
	_, err := client.ListBuckets(t.Context())
	return err == nil
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
