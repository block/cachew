// Package opa provides OPA-based HTTP request authorization middleware.
package opa

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/open-policy-agent/opa/v1/rego"

	"github.com/block/cachew/internal/logging"
)

// DefaultPolicy allows only GET and HEAD requests.
const DefaultPolicy = `package cachew.authz

default allow := false

allow if input.method == "GET"
allow if input.method == "HEAD"
allow if startswith(input.remote_addr, "127.0.0.1:")
`

// Config for OPA policy evaluation. If neither Policy nor PolicyFile is set,
// a default policy allowing only GET and HEAD requests is used.
type Config struct {
	Policy     string `hcl:"policy,optional" help:"Inline Rego policy."`
	PolicyFile string `hcl:"policy-file,optional" help:"Path to a Rego policy file."`
	Data       string `hcl:"data,optional" help:"Inline JSON object loaded as OPA data.*"`
	DataFile   string `hcl:"data-file,optional" help:"Path to a JSON file loaded as OPA data.*"`
}

// Middleware returns an http.Handler that evaluates OPA policy before delegating to next.
// The policy must define a boolean "allow" rule under package cachew.authz.
func Middleware(ctx context.Context, cfg Config, next http.Handler) (http.Handler, error) {
	policy, err := loadPolicy(cfg)
	if err != nil {
		return nil, err
	}

	opts := []func(*rego.Rego){
		rego.Query("data.cachew.authz.allow"),
		rego.Module("policy.rego", policy),
	}

	if cfg.Data != "" || cfg.DataFile != "" {
		opaData, err := loadData(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, rego.Data(opaData))
	}

	prepared, err := rego.New(opts...).PrepareForEval(ctx)
	if err != nil {
		return nil, errors.Errorf("compile OPA policy: %w", err)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		input := buildInput(r)
		logger := logging.FromContext(r.Context())
		results, err := prepared.Eval(r.Context(), rego.EvalInput(input))
		if err != nil {
			logger.Error("OPA evaluation failed", "error", err)
			http.Error(w, "policy evaluation error", http.StatusInternalServerError)
			return
		}
		if !results.Allowed() {
			logger.Warn("OPA denied request", "method", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	}), nil
}

func loadPolicy(cfg Config) (string, error) {
	if cfg.Policy != "" && cfg.PolicyFile != "" {
		return "", errors.New("OPA config: only one of policy or policy-file may be set")
	}
	if cfg.PolicyFile != "" {
		data, err := os.ReadFile(cfg.PolicyFile)
		if err != nil {
			return "", errors.Errorf("read OPA policy file: %w", err)
		}
		return string(data), nil
	}
	if cfg.Policy != "" {
		return cfg.Policy, nil
	}
	return DefaultPolicy, nil
}

func loadData(cfg Config) (map[string]any, error) {
	if cfg.Data != "" && cfg.DataFile != "" {
		return nil, errors.New("OPA config: only one of data or data-file may be set")
	}
	var raw []byte
	switch {
	case cfg.DataFile != "":
		var err error
		raw, err = os.ReadFile(cfg.DataFile)
		if err != nil {
			return nil, errors.Errorf("read OPA data file: %w", err)
		}
	case cfg.Data != "":
		raw = []byte(cfg.Data)
	default:
		return nil, errors.New("OPA config: one of data or data-file must be set")
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, errors.Errorf("parse OPA data: %w", err)
	}
	return data, nil
}

func buildInput(r *http.Request) map[string]any {
	path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(path) == 1 && path[0] == "" {
		path = []string{}
	}

	headers := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		headers[strings.ToLower(k)] = v[0]
	}

	return map[string]any{
		"method":      r.Method,
		"path":        path,
		"headers":     headers,
		"remote_addr": r.RemoteAddr,
	}
}
