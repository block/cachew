//go:build integration

package strategy_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/validate"
	"golang.org/x/crypto/bcrypt"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy"
)

const (
	// registry:3, CNCF Distribution 3.1.2.
	distributionImage = "registry@sha256:38907510915c3f1604c7e83d6ab602ab3020d092f73bdaed8696773693ff768c"
	registryUsername  = "cachew"
	registryPassword  = "cachew-test-password"
)

func TestOCIIntegration(t *testing.T) {
	tests := []struct {
		name     string
		htpasswd bool
	}{
		{name: "Anonymous"},
		{name: "Htpasswd", htpasswd: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := strategy.OCIConfig{TagTTL: time.Nanosecond}
			auth := authn.Anonymous
			if tt.htpasswd {
				config.Username, config.Password = registryUsername, registryPassword
				auth = &authn.Basic{Username: registryUsername, Password: registryPassword}
			}
			registry, stopRegistry := startDistribution(t, tt.htpasswd)
			cachew := startOCICachew(t, registry, config)

			app := pushRandomImage(t, registry+"/test/app:latest", auth)
			multi, err := random.Index(1024, 2, 2)
			assert.NoError(t, err)
			assert.NoError(t, remote.WriteIndex(parseRef(t, registry+"/test/multi:latest"), multi,
				remote.WithAuth(auth), remote.WithContext(t.Context())))

			assert.Equal(t, digestOf(t, app), pullImage(t, cachew+"/test/app:latest"))
			assert.Equal(t, digestOf(t, multi), pullIndex(t, cachew+"/test/multi:latest"))

			updated := pushRandomImage(t, registry+"/test/app:latest", auth)
			byDigest := cachew + "/test/app@" + digestOf(t, updated)
			assert.Equal(t, digestOf(t, updated), pullImage(t, cachew+"/test/app:latest"))
			assert.Equal(t, digestOf(t, updated), pullImage(t, byDigest))

			stopRegistry()
			assert.Equal(t, digestOf(t, updated), pullImage(t, byDigest))
			assert.Equal(t, digestOf(t, updated), pullImage(t, cachew+"/test/app:latest"))
			assert.Equal(t, digestOf(t, multi), pullIndex(t, cachew+"/test/multi:latest"))
		})
	}
}

// startDistribution runs the reference registry in Docker and returns its
// address and a function that stops it.
func startDistribution(t *testing.T, htpasswd bool) (string, func()) {
	t.Helper()
	args := []string{"run", "--detach", "--rm", "--publish", "127.0.0.1::5000"}
	if htpasswd {
		dir := t.TempDir()
		hash, err := bcrypt.GenerateFromPassword([]byte(registryPassword), bcrypt.MinCost)
		assert.NoError(t, err)
		entry := registryUsername + ":" + string(hash) + "\n"
		assert.NoError(t, os.WriteFile(filepath.Join(dir, "htpasswd"), []byte(entry), 0o644))
		args = append(args, "--volume", dir+":/auth:ro",
			"--env", "REGISTRY_AUTH=htpasswd",
			"--env", "REGISTRY_AUTH_HTPASSWD_REALM=cachew",
			"--env", "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd")
	}
	id := runDocker(t, append(args, distributionImage)...)
	stop := func() { _ = exec.Command("docker", "rm", "--force", id).Run() }
	t.Cleanup(stop)
	addr, _, _ := strings.Cut(runDocker(t, "port", id, "5000/tcp"), "\n")
	waitForRegistry(t, addr)
	return addr, stop
}

func runDocker(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "docker", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	assert.NoError(t, err, "docker %s: %s", strings.Join(args, " "), stderr.String())
	return strings.TrimSpace(string(out))
}

func waitForRegistry(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/v2/", nil)
		assert.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry at %s did not become ready: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startOCICachew serves the OCI strategy for upstream on 127.0.0.1, using the
// hostname form that clients without path-prefix support need.
func startOCICachew(t *testing.T, upstream string, config strategy.OCIConfig) string {
	t.Helper()
	_, ctx := logging.Configure(context.Background(), logging.Config{Level: slog.LevelError})
	c, err := cache.NewMemory(ctx, cache.MemoryConfig{MaxTTL: time.Hour})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	config.Registry = "registry.test"
	config.Upstream = "http://" + upstream
	config.Hosts = []string{"127.0.0.1"}
	config.BlobChunkSizeMB = 32
	config.BlobConcurrency = 4
	mux := http.NewServeMux()
	_, err = strategy.NewOCI(ctx, config, c, mux, newTestSpools(t))
	assert.NoError(t, err)
	server := httptest.NewUnstartedServer(mux)
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	server.Start()
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

func pushRandomImage(t *testing.T, ref string, auth authn.Authenticator) v1.Image {
	t.Helper()
	img, err := random.Image(64<<10, 3)
	assert.NoError(t, err)
	assert.NoError(t, remote.Write(parseRef(t, ref), img, remote.WithAuth(auth), remote.WithContext(t.Context())))
	return img
}

// pullImage fetches and validates the manifest, config and every layer, so
// each blob is downloaded and checked against its digest.
func pullImage(t *testing.T, ref string) string {
	t.Helper()
	img, err := remote.Image(parseRef(t, ref), remote.WithContext(t.Context()))
	assert.NoError(t, err)
	assert.NoError(t, validate.Image(img))
	return digestOf(t, img)
}

func pullIndex(t *testing.T, ref string) string {
	t.Helper()
	idx, err := remote.Index(parseRef(t, ref), remote.WithContext(t.Context()))
	assert.NoError(t, err)
	assert.NoError(t, validate.Index(idx))
	return digestOf(t, idx)
}

func parseRef(t *testing.T, ref string) name.Reference {
	t.Helper()
	r, err := name.ParseReference(ref, name.Insecure)
	assert.NoError(t, err)
	return r
}

func digestOf(t *testing.T, artifact interface{ Digest() (v1.Hash, error) }) string {
	t.Helper()
	d, err := artifact.Digest()
	assert.NoError(t, err)
	return d.String()
}
