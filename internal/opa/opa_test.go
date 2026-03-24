package opa_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/opa"
)

func newRequest(method, target string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	return r.WithContext(logging.ContextWithLogger(r.Context(), slog.Default()))
}

func TestMiddlewareDefaultPolicy(t *testing.T) {
	tests := []struct {
		Name           string
		Method         string
		Path           string
		RemoteAddr     string
		ExpectedStatus int
	}{
		{"LocalhostGetAdmin", http.MethodGet, "/admin/log/level", "127.0.0.1:12345", http.StatusOK},
		{"LocalhostPostAPI", http.MethodPost, "/api/v1/object/ns/key", "127.0.0.1:12345", http.StatusOK},
		{"RemoteGetStrategy", http.MethodGet, "/git/github.com/org/repo", "10.0.0.1:9999", http.StatusOK},
		{"RemotePostStrategy", http.MethodPost, "/git/github.com/org/repo", "10.0.0.1:9999", http.StatusOK},
		{"RemoteLiveness", http.MethodGet, "/_liveness", "10.0.0.1:9999", http.StatusOK},
		{"RemoteReadiness", http.MethodGet, "/_readiness", "10.0.0.1:9999", http.StatusOK},
		{"RemoteGetAdmin", http.MethodGet, "/admin/pprof/", "10.0.0.1:9999", http.StatusForbidden},
		{"RemotePostAPI", http.MethodPost, "/api/v1/object/ns/key", "10.0.0.1:9999", http.StatusForbidden},
		{"RemoteDeleteAPI", http.MethodDelete, "/api/v1/object/ns/key", "10.0.0.1:9999", http.StatusForbidden},
	}

	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{}, next)
	assert.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, test.Path)
			r.RemoteAddr = test.RemoteAddr
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewareInlinePolicy(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if input.method == "POST"
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Method         string
		ExpectedStatus int
	}{
		{"POSTAllowed", http.MethodPost, http.StatusOK},
		{"GETDenied", http.MethodGet, http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, "/")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewarePolicyFile(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if input.path[0] == "public"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.rego")
	assert.NoError(t, os.WriteFile(path, []byte(policy), 0o644))

	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{PolicyFile: path}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Path           string
		ExpectedStatus int
	}{
		{"PublicAllowed", "/public/file", http.StatusOK},
		{"PrivateDenied", "/private/file", http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(http.MethodGet, test.Path)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewarePathBasedPolicy(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if input.path[0] == "api"
allow if input.path[0] == "_liveness"
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Path           string
		ExpectedStatus int
	}{
		{"APIAllowed", "/api/v1/object", http.StatusOK},
		{"LivenessAllowed", "/_liveness", http.StatusOK},
		{"AdminDenied", "/admin/pprof/", http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(http.MethodGet, test.Path)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewareInlineData(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if data.allowed_methods[input.method]
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{
		Policy: policy,
		Data:   `{"allowed_methods": {"DELETE": true}}`,
	}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Method         string
		ExpectedStatus int
	}{
		{"DELETEAllowed", http.MethodDelete, http.StatusOK},
		{"GETDenied", http.MethodGet, http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, "/")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewareBothDataAndDataFileError(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{Data: "{}", DataFile: "x"}, next)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only one of")
}

func TestMiddlewareInlineDataInvalidJSON(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{Data: "{not json}"}, next)
	assert.Error(t, err)
}

func TestMiddlewareDataFile(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if data.allowed_methods[input.method]
`
	dataJSON := `{"allowed_methods": {"POST": true, "PUT": true}}`

	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.rego")
	dataPath := filepath.Join(dir, "data.json")
	assert.NoError(t, os.WriteFile(policyPath, []byte(policy), 0o644))
	assert.NoError(t, os.WriteFile(dataPath, []byte(dataJSON), 0o644))

	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{PolicyFile: policyPath, DataFile: dataPath}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Method         string
		ExpectedStatus int
	}{
		{"POSTAllowed", http.MethodPost, http.StatusOK},
		{"PUTAllowed", http.MethodPut, http.StatusOK},
		{"GETDenied", http.MethodGet, http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, "/")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewareDataFileMissing(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{DataFile: "/nonexistent"}, next)
	assert.Error(t, err)
}

func TestMiddlewareDataFileInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "bad.json")
	assert.NoError(t, os.WriteFile(dataPath, []byte("{not json}"), 0o644))

	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{DataFile: dataPath}, next)
	assert.Error(t, err)
}

func TestMiddlewareBothPolicyAndFileError(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{Policy: "x", PolicyFile: "y"}, next)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only one of")
}

func TestMiddlewarePolicyFileMissing(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{PolicyFile: "/nonexistent"}, next)
	assert.Error(t, err)
}

func TestMiddlewareInvalidPolicy(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	_, err := opa.Middleware(t.Context(), opa.Config{Policy: "not valid rego {"}, next)
	assert.Error(t, err)
}

func TestMiddlewareHeaderBasedPolicy(t *testing.T) {
	policy := `package cachew.authz
default allow := false
allow if input.headers["authorization"]
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	r := newRequest(http.MethodGet, "/")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)

	r = newRequest(http.MethodGet, "/")
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMiddlewareEmptyPolicyDeniesAll(t *testing.T) {
	policy := `package cachew.authz
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		r := newRequest(method, "/any/path")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusForbidden, w.Code)
	}
}
