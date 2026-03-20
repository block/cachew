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
		ExpectedStatus int
	}{
		{"GETAllowed", http.MethodGet, http.StatusOK},
		{"HEADAllowed", http.MethodHead, http.StatusOK},
		{"POSTDenied", http.MethodPost, http.StatusForbidden},
		{"PUTDenied", http.MethodPut, http.StatusForbidden},
		{"DELETEDenied", http.MethodDelete, http.StatusForbidden},
	}

	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{}, next)
	assert.NoError(t, err)

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, "/some/path")
			// Default policy requires localhost; httptest uses 192.0.2.1, so
			// non-localhost requests that are also non-GET/HEAD get two reasons.
			// Override RemoteAddr so we only test the method rule.
			r.RemoteAddr = "127.0.0.1:12345"
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
		})
	}
}

func TestMiddlewareDefaultPolicyDeniesNonLocalhost(t *testing.T) {
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{}, next)
	assert.NoError(t, err)

	r := newRequest(http.MethodGet, "/some/path")
	r.RemoteAddr = "10.0.0.1:9999"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Contains(t, w.Body.String(), "remote address not allowed")
}

func TestMiddlewareInlinePolicy(t *testing.T) {
	policy := `package cachew.authz
deny contains "only POST allowed" if input.method != "POST"
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
deny contains "private path" if input.path[0] != "public"
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
deny contains "path not allowed" if {
	not input.path[0] == "api"
	not input.path[0] == "_liveness"
}
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
deny contains "method not in allowed set" if not data.allowed_methods[input.method]
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
deny contains "method not in allowed set" if not data.allowed_methods[input.method]
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

func TestMiddlewareDenyReasons(t *testing.T) {
	policy := `package cachew.authz
deny contains "writes are not allowed" if input.method == "PUT"
deny contains "deletes are not allowed" if input.method == "DELETE"
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	tests := []struct {
		Name           string
		Method         string
		ExpectedStatus int
		ExpectedBody   string
	}{
		{"GETAllowed", http.MethodGet, http.StatusOK, ""},
		{"PUTDenied", http.MethodPut, http.StatusForbidden, "forbidden: writes are not allowed\n"},
		{"DELETEDenied", http.MethodDelete, http.StatusForbidden, "forbidden: deletes are not allowed\n"},
	}
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			r := newRequest(test.Method, "/")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			assert.Equal(t, test.ExpectedStatus, w.Code)
			if test.ExpectedBody != "" {
				assert.Equal(t, test.ExpectedBody, w.Body.String())
			}
		})
	}
}

func TestMiddlewareDenyUnauthenticated(t *testing.T) {
	policy := `package cachew.authz
deny contains "unauthenticated" if not input.headers["authorization"]
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	// Without Authorization header: denied.
	r := newRequest(http.MethodGet, "/")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, "forbidden: unauthenticated\n", w.Body.String())

	// With Authorization header: allowed.
	r = newRequest(http.MethodGet, "/")
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestMiddlewareDenyMultipleReasons(t *testing.T) {
	policy := `package cachew.authz
deny contains "reason-a" if input.method == "POST"
deny contains "reason-b" if input.method == "POST"
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	r := newRequest(http.MethodPost, "/")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusForbidden, w.Code)
	// Reasons are sorted deterministically.
	assert.Equal(t, "forbidden: reason-a; reason-b\n", w.Body.String())
}

func TestMiddlewareEmptyDenyAllowsAll(t *testing.T) {
	// A policy with no deny rules allows everything.
	policy := `package cachew.authz
`
	next := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	handler, err := opa.Middleware(t.Context(), opa.Config{Policy: policy}, next)
	assert.NoError(t, err)

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		r := newRequest(method, "/any/path")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		assert.Equal(t, http.StatusOK, w.Code)
	}
}
