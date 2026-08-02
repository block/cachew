package git //nolint:testpackage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

type proxyCredentialProvider struct {
	authorization string
	scope         string
	matched       bool
	err           error
	repositoryURL string
	calls         int
}

func (p *proxyCredentialProvider) Credential(_ context.Context, repositoryURL string) (gitcredential.Credential, bool, error) {
	p.calls++
	p.repositoryURL = repositoryURL
	scope := p.scope
	if scope == "" {
		var err error
		scope, err = gitcredential.NormalizeRepositoryURLScope(repositoryURL)
		if err != nil {
			return gitcredential.Credential{}, true, err
		}
	}
	return gitcredential.Credential{Authorization: p.authorization, URLScope: scope}, p.matched, p.err
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newProxyTestStrategy(t *testing.T, provider gitcredential.Provider, transport http.RoundTripper) (*Strategy, context.Context) {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{})
	manager, err := gitclone.NewManager(ctx, gitclone.Config{MirrorRoot: t.TempDir()}, provider)
	assert.NoError(t, err)
	return &Strategy{
		cloneManager: manager,
		proxy: &httputil.ReverseProxy{
			Director:  func(*http.Request) {},
			Transport: transport,
		},
	}, ctx
}

func TestCacheMissPassThroughAppliesCredential(t *testing.T) {
	provider := &proxyCredentialProvider{authorization: "Bearer plugin-token", matched: true}
	var authorization string
	var credentialApplied bool
	strategy, ctx := newProxyTestStrategy(t, provider, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		authorization = r.Header.Get("Authorization")
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	}))
	strategy.proxy.Director = func(r *http.Request) {
		credentialApplied = upstreamCredentialApplied(r.Context())
	}
	req := httptest.NewRequest(http.MethodGet, "http://cachew/git/dev.azure.com/org/repo.git/info/refs", nil).WithContext(ctx)
	resp := httptest.NewRecorder()
	err := strategy.serveWithSpool(resp, req, "dev.azure.com", "org/repo.git/info/refs",
		"https://dev.azure.com/org/repo.git")
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "Bearer plugin-token", authorization)
	assert.True(t, credentialApplied)
	assert.Equal(t, "https://dev.azure.com/org/repo.git", provider.repositoryURL)
}

func TestForwardToUpstreamPreservesIncomingCredentialWhenUnmatched(t *testing.T) {
	provider := &proxyCredentialProvider{matched: false}
	var authorization string
	strategy, ctx := newProxyTestStrategy(t, provider, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		authorization = r.Header.Get("Authorization")
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	req := httptest.NewRequest(http.MethodGet, "http://cachew/git/example.com/org/repo/info/refs", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer incoming-token")
	resp := httptest.NewRecorder()
	strategy.forwardToUpstream(resp, req, "example.com", "org/repo/info/refs")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "Bearer incoming-token", authorization)
}

func TestForwardToUpstreamFailsClosed(t *testing.T) {
	provider := &proxyCredentialProvider{matched: true, err: errors.New("provider failed")}
	called := false
	strategy, ctx := newProxyTestStrategy(t, provider, roundTripperFunc(func(*http.Request) (*http.Response, error) {
		called = true
		return nil, errors.New("transport should not be called")
	}))
	req := httptest.NewRequest(http.MethodGet, "http://cachew/git/example.com/org/repo/info/refs", nil).WithContext(ctx)
	resp := httptest.NewRecorder()
	strategy.forwardToUpstream(resp, req, "example.com", "org/repo/info/refs")
	assert.Equal(t, http.StatusBadGateway, resp.Code)
	assert.False(t, called)
}

func TestForwardToUpstreamRejectsInvalidCredential(t *testing.T) {
	tests := []*proxyCredentialProvider{
		{authorization: "Bearer token\nInjected: value", matched: true},
		{authorization: "Bearer token", scope: "https://example.com", matched: true},
	}
	for _, provider := range tests {
		called := false
		strategy, ctx := newProxyTestStrategy(t, provider, roundTripperFunc(func(*http.Request) (*http.Response, error) {
			called = true
			return nil, errors.New("transport should not be called")
		}))
		req := httptest.NewRequest(http.MethodGet, "http://cachew/git/example.com/org/repo/info/refs", nil).WithContext(ctx)
		resp := httptest.NewRecorder()
		strategy.forwardToUpstream(resp, req, "example.com", "org/repo/info/refs")
		assert.Equal(t, http.StatusBadGateway, resp.Code)
		assert.False(t, called)
	}
}

func TestForwardToUpstreamDoesNotAuthenticateUnknownPath(t *testing.T) {
	provider := &proxyCredentialProvider{authorization: "Bearer plugin-token", matched: true}
	var authorization string
	strategy, ctx := newProxyTestStrategy(t, provider, roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		authorization = r.Header.Get("Authorization")
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	}))
	req := httptest.NewRequest(http.MethodGet, "http://cachew/git/example.com/org/repo/archive.zip", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer incoming-token")
	resp := httptest.NewRecorder()
	strategy.forwardToUpstream(resp, req, "example.com", "org/repo/archive.zip")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "Bearer incoming-token", authorization)
	assert.Equal(t, 0, provider.calls)
}

func TestUpstreamRepositoryURL(t *testing.T) {
	tests := []struct {
		path     string
		expected string
		matched  bool
	}{
		{path: "org/repo.git/info/refs", expected: "https://dev.azure.com/org/repo.git", matched: true},
		{path: "org/repo/git-upload-pack", expected: "https://dev.azure.com/org/repo", matched: true},
		{path: "org/repo.git/info/lfs/objects/batch", expected: "https://dev.azure.com/org/repo.git", matched: true},
		{path: "org/repo/archive.zip", matched: false},
	}
	for _, tt := range tests {
		actual, matched := upstreamRepositoryURL("dev.azure.com", tt.path)
		assert.Equal(t, tt.expected, actual)
		assert.Equal(t, tt.matched, matched)
	}
}
