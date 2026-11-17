// Package opa provides OPA-based HTTP request authorization middleware.
package opa

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/alecthomas/errors"
	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/open-policy-agent/opa/v1/storage/inmem"
	"github.com/open-policy-agent/opa/v1/tester"

	"github.com/block/cachew/internal/logging"
)

// DefaultPolicy allows all requests from localhost and restricts remote access to non-admin paths.
const DefaultPolicy = `package cachew.authz

default allow := false
allow if startswith(input.remote_addr, "127.0.0.1:")
allow if not input.path[0] in {"api", "admin"}
`

// Config for OPA policy evaluation. Without Policy or PolicyFile, a default policy allows only GET and HEAD.
type Config struct {
	Policy     string `hcl:"policy,optional" help:"Inline Rego policy."`
	PolicyFile string `hcl:"policy-file,optional" help:"Path to a Rego policy file."`
	Data       string `hcl:"data,optional" help:"Inline JSON object loaded as OPA data.*"`
	DataFile   string `hcl:"data-file,optional" help:"Path to a JSON file loaded as OPA data.*"`
	Test       string `hcl:"test,optional" help:"Inline Rego test module run against the policy when cachewd starts."`
}

// Middleware returns an http.Handler that evaluates OPA policy before delegating to next.
//
// The policy must define a boolean "allow" rule in package cachew.authz; false rejects with 403.
func Middleware(ctx context.Context, cfg Config, next http.Handler) (http.Handler, error) {
	policy, err := loadPolicy(cfg)
	if err != nil {
		return nil, err
	}

	dataOpts, err := dataOptions(cfg)
	if err != nil {
		return nil, err
	}

	prepared, err := prepareQuery(ctx, "data.cachew.authz.allow", policy, dataOpts)
	if err != nil {
		return nil, errors.Errorf("compile OPA allow query: %w", err)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		input := buildInput(r)
		logger := logging.FromContext(r.Context())

		allowed, err := evalAllow(r.Context(), prepared, input)
		if err != nil {
			logger.ErrorContext(r.Context(), "OPA evaluation failed", "error", err)
			http.Error(w, "policy evaluation error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			logger.WarnContext(r.Context(), "OPA denied request", "method", r.Method, "path", r.URL.Path, "remote_addr", r.RemoteAddr)
			// The unread body is still on the wire, so the connection can't be reused.
			//
			// Saying so up front stops net/http reading up to 256KiB of body before replying, which it does because
			// otelhttp replaces r.Body and hides its unread length.
			if r.ContentLength != 0 {
				w.Header().Set("Connection", "close")
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	}), nil
}

// RunTests runs every test_* rule in the cfg.Test Rego module against the configured policy.
//
// It returns the passed count and an error listing failures; empty cfg.Test is a no-op. The policy loads as
// in Middleware, so an empty policy config tests DefaultPolicy.
func RunTests(ctx context.Context, cfg Config) (int, error) {
	if cfg.Test == "" {
		return 0, nil
	}

	policy, err := loadPolicy(cfg)
	if err != nil {
		return 0, err
	}
	modules, err := parseTestModules(policy, cfg.Test)
	if err != nil {
		return 0, err
	}

	runner := tester.NewRunner().SetModules(modules)
	if cfg.Data != "" || cfg.DataFile != "" {
		opaData, err := loadData(cfg)
		if err != nil {
			return 0, err
		}
		runner = runner.SetStore(inmem.NewFromObject(opaData))
	}

	ch, err := runner.RunTests(ctx, nil)
	if err != nil {
		return 0, errors.Errorf("run OPA tests: %w", err)
	}

	passed := 0
	var failures []string
	for result := range ch {
		switch {
		case result.Pass():
			passed++
		case result.Skip:
		case result.Error != nil:
			failures = append(failures, fmt.Sprintf("%s.%s: %v", result.Package, result.Name, result.Error))
		default:
			failures = append(failures, fmt.Sprintf("%s.%s: failed", result.Package, result.Name))
		}
	}
	if len(failures) > 0 {
		return passed, errors.Errorf("OPA tests failed: %s", strings.Join(failures, "; "))
	}
	return passed, nil
}

func parseTestModules(policy, test string) (map[string]*ast.Module, error) {
	policyModule, err := ast.ParseModule("policy.rego", policy)
	if err != nil {
		return nil, errors.Errorf("parse OPA policy: %w", err)
	}
	testModule, err := ast.ParseModule("test.rego", test)
	if err != nil {
		return nil, errors.Errorf("parse OPA test: %w", err)
	}
	return map[string]*ast.Module{"policy.rego": policyModule, "test.rego": testModule}, nil
}

func prepareQuery(ctx context.Context, query, policy string, dataOpts []func(*rego.Rego)) (rego.PreparedEvalQuery, error) {
	opts := make([]func(*rego.Rego), 0, 2+len(dataOpts))
	opts = append(opts, rego.Query(query), rego.Module("policy.rego", policy))
	opts = append(opts, dataOpts...)
	prepared, err := rego.New(opts...).PrepareForEval(ctx)
	if err != nil {
		return prepared, errors.Errorf("prepare query: %w", err)
	}
	return prepared, nil
}

func dataOptions(cfg Config) ([]func(*rego.Rego), error) {
	if cfg.Data == "" && cfg.DataFile == "" {
		return nil, nil
	}
	opaData, err := loadData(cfg)
	if err != nil {
		return nil, err
	}
	return []func(*rego.Rego){rego.Data(opaData)}, nil
}

func evalAllow(ctx context.Context, prepared rego.PreparedEvalQuery, input map[string]any) (bool, error) {
	results, err := prepared.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return false, errors.Errorf("evaluate allow query: %w", err)
	}
	if len(results) == 0 || len(results[0].Expressions) == 0 {
		return false, nil
	}
	allowed, ok := results[0].Expressions[0].Value.(bool)
	return ok && allowed, nil
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
