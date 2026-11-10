package strategy

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/alecthomas/errors"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// ociTransport authenticates upstream requests with the strategy's own credentials.
// Registries issue tokens per repository scope, so it keeps one transport per repository.
type ociTransport struct {
	registry name.Registry
	auth     authn.Authenticator
	base     http.RoundTripper

	mu           sync.Mutex
	repositories map[string]*ociRepositoryTransport
}

// ociRepositoryTransport is created lazily so that a slow token exchange for
// one repository does not block requests for others.
type ociRepositoryTransport struct {
	mu        sync.Mutex
	transport http.RoundTripper
}

func newOCITransport(upstream *url.URL, username, password string, base http.RoundTripper) (*ociTransport, error) {
	var opts []name.Option
	if upstream.Scheme == schemeHTTP {
		opts = append(opts, name.Insecure)
	}
	registry, err := name.NewRegistry(upstream.Host, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "invalid upstream registry")
	}
	auth := authn.Anonymous
	if username != "" || password != "" {
		auth = &authn.Basic{Username: username, Password: password}
	}
	return &ociTransport{
		registry:     registry,
		auth:         auth,
		base:         ociRateLimitTransport{base: base},
		repositories: map[string]*ociRepositoryTransport{},
	}, nil
}

func (t *ociTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	rt, err := t.forRepository(req)
	if err == nil {
		// Client credentials belong to cachew, not to the upstream registry.
		req = req.Clone(req.Context())
		req.Header.Del("Authorization")
		req.Header.Del("Cookie")
		resp, err = rt.RoundTrip(req)
	}
	if limited, ok := errors.AsType[*ociRateLimitError](err); ok {
		limited.response.Request = req
		return limited.response, nil
	}
	return resp, errors.WithStack(err)
}

func (t *ociTransport) forRepository(req *http.Request) (http.RoundTripper, error) {
	repository := ociRequestFrom(req.Context()).repository
	t.mu.Lock()
	entry, ok := t.repositories[repository]
	if !ok {
		entry = &ociRepositoryTransport{}
		t.repositories[repository] = entry
	}
	t.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.transport != nil {
		return entry.transport, nil
	}
	scope := t.registry.Repo(repository).Scope(transport.PullScope)
	// Failures are not kept, so a transient ping or token failure is retried
	// by the next request.
	rt, err := transport.NewWithContext(req.Context(), t.registry, t.auth, t.base, []string{scope})
	if err != nil {
		return nil, errors.Wrapf(err, "authenticate to %s for %s", t.registry, repository)
	}
	entry.transport = rt
	return rt, nil
}

// ociRateLimitTransport preserves 429 headers that the auth library discards
// when turning registry ping and token responses into errors.
type ociRateLimitTransport struct {
	base http.RoundTripper
}

func (t ociRateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusTooManyRequests {
		return resp, errors.WithStack(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, errors.Wrap(err, "read registry rate limit response")
	}
	limited := *resp
	limited.Body = io.NopCloser(bytes.NewReader(body))
	limited.ContentLength = int64(len(body))
	limited.Header = resp.Header.Clone()
	limited.Header.Del("Content-Length")
	return nil, &ociRateLimitError{response: &limited}
}

type ociRateLimitError struct {
	response *http.Response
}

func (e *ociRateLimitError) Error() string { return "registry rate limited" }
