package gitcredential_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
)

func TestCredentialHelperProcess(_ *testing.T) {
	if os.Getenv("CACHEW_CREDENTIAL_HELPER") != "1" {
		return
	}
	input, _ := io.ReadAll(os.Stdin)
	if path := os.Getenv("CACHEW_CREDENTIAL_REQUEST"); path != "" {
		_ = os.WriteFile(path, input, 0o600)
	}
	if path := os.Getenv("CACHEW_CREDENTIAL_COUNT"); path != "" {
		file, _ := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		_, _ = file.WriteString("1\n")
		_ = file.Close()
	}
	switch os.Getenv("CACHEW_CREDENTIAL_MODE") {
	case "malformed":
		_, _ = fmt.Fprint(os.Stdout, "not json")
	case "failure":
		os.Exit(7)
	case "oversized":
		_, _ = fmt.Fprint(os.Stdout, strings.Repeat("x", (64<<10)+1))
	case "timeout":
		time.Sleep(time.Second)
	default:
		_ = json.NewEncoder(os.Stdout).Encode(struct {
			Version       int       `json:"version"`
			Authorization string    `json:"authorization"`
			ExpiresAt     time.Time `json:"expires_at"`
		}{
			Version:       1,
			Authorization: "Bearer secret",
			ExpiresAt:     time.Now().Add(time.Hour),
		})
	}
	os.Exit(0)
}

func TestCommandProviderCredentialAndCache(t *testing.T) {
	provider, requestPath, countPath := newTestCommandProvider(t, 5*time.Second)
	credential, matched, err := provider.Credential(t.Context(), "HTTPS://EXAMPLE.COM:443/org/repo.git/")
	assert.NoError(t, err)
	assert.True(t, matched)
	assert.Equal(t, "Bearer secret", credential.Authorization)
	assert.Equal(t, "https://example.com/org/repo.git", credential.URLScope)

	credential, matched, err = provider.Credential(t.Context(), "https://example.com/org/repo")
	assert.NoError(t, err)
	assert.True(t, matched)
	assert.Equal(t, "Bearer secret", credential.Authorization)

	request, err := os.ReadFile(requestPath)
	assert.NoError(t, err)
	assert.Equal(t, "{\"version\":1,\"remote_url\":\"https://example.com/org/repo\"}\n", string(request))
	count, err := os.ReadFile(countPath)
	assert.NoError(t, err)
	assert.Equal(t, "1\n", string(count))
}

func TestCommandProviderUsesRequestedRemoteURLScopeOnCacheHit(t *testing.T) {
	provider, _, countPath := newTestCommandProvider(t, 5*time.Second)
	credential, matched, err := provider.Credential(t.Context(), "https://example.com/org/repo")
	assert.NoError(t, err)
	assert.True(t, matched)
	assert.Equal(t, "https://example.com/org/repo", credential.URLScope)

	credential, matched, err = provider.Credential(t.Context(), "https://example.com/org/repo.git")
	assert.NoError(t, err)
	assert.True(t, matched)
	assert.Equal(t, "https://example.com/org/repo.git", credential.URLScope)
	count, err := os.ReadFile(countPath)
	assert.NoError(t, err)
	assert.Equal(t, "1\n", string(count))
}

func TestCommandProviderExactMatch(t *testing.T) {
	provider, _, _ := newTestCommandProvider(t, 5*time.Second)
	credential, matched, err := provider.Credential(t.Context(), "https://example.com/org/repo-malicious")
	assert.NoError(t, err)
	assert.False(t, matched)
	assert.Equal(t, gitcredential.Credential{}, credential)
}

func TestCommandProviderFailuresFailClosed(t *testing.T) {
	for _, mode := range []string{"malformed", "failure", "oversized", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("CACHEW_CREDENTIAL_MODE", mode)
			timeout := 5 * time.Second
			if mode == "timeout" {
				timeout = 20 * time.Millisecond
			}
			provider, _, _ := newTestCommandProvider(t, timeout)
			_, matched, err := provider.Credential(t.Context(), "https://example.com/org/repo")
			assert.True(t, matched)
			assert.Error(t, err)
			assert.False(t, strings.Contains(err.Error(), "secret"))
		})
	}
}

func TestCommandProviderCoalescesConcurrentRefresh(t *testing.T) {
	provider, _, countPath := newTestCommandProvider(t, 5*time.Second)
	var wg sync.WaitGroup
	callErrors := make(chan error, 20)
	for range 20 {
		wg.Go(func() {
			_, matched, err := provider.Credential(context.Background(), "https://example.com/org/repo")
			if !matched {
				callErrors <- errors.Errorf("provider did not match")
				return
			}
			if err != nil {
				callErrors <- err
			}
		})
	}
	wg.Wait()
	close(callErrors)
	for err := range callErrors {
		assert.NoError(t, err)
	}
	count, err := os.ReadFile(countPath)
	assert.NoError(t, err)
	assert.Equal(t, "1\n", string(count))
}

func TestNewCommandProviderValidation(t *testing.T) {
	executable, err := os.Executable()
	assert.NoError(t, err)
	base := gitcredential.CommandConfig{
		Name:          "one",
		Command:       []string{executable},
		Remotes:       []string{"https://example.com/org/repo"},
		Timeout:       time.Second,
		RefreshBefore: time.Minute,
	}
	_, err = gitcredential.NewCommandProvider([]gitcredential.CommandConfig{base, {
		Name:          "two",
		Command:       []string{executable},
		Remotes:       []string{"https://EXAMPLE.com:443/org/repo.git/"},
		Timeout:       time.Second,
		RefreshBefore: time.Minute,
	}})
	assert.Error(t, err)

	base.Timeout = 0
	_, err = gitcredential.NewCommandProvider([]gitcredential.CommandConfig{base})
	assert.Error(t, err)
}

func newTestCommandProvider(t *testing.T, timeout time.Duration) (*gitcredential.CommandProvider, string, string) {
	t.Helper()
	executable, err := os.Executable()
	assert.NoError(t, err)
	requestPath := t.TempDir() + "/request"
	countPath := t.TempDir() + "/count"
	t.Setenv("CACHEW_CREDENTIAL_HELPER", "1")
	t.Setenv("CACHEW_CREDENTIAL_REQUEST", requestPath)
	t.Setenv("CACHEW_CREDENTIAL_COUNT", countPath)
	provider, err := gitcredential.NewCommandProvider([]gitcredential.CommandConfig{{
		Name:          "test",
		Command:       []string{executable, "-test.run=^TestCredentialHelperProcess$"},
		Remotes:       []string{"https://example.com/org/repo"},
		Timeout:       timeout,
		RefreshBefore: time.Minute,
	}})
	assert.NoError(t, err)
	return provider, requestPath, countPath
}
