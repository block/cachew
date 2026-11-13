package main

import (
	"encoding/pem"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/open-policy-agent/opa/v1/rego"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
)

const opaTransportStartupChildEnv = "CACHEWD_OPA_TRANSPORT_STARTUP_CHILD"

func TestOPAHTTPSendWithTLSOptionsAfterProcessSetup(t *testing.T) {
	if os.Getenv(opaTransportStartupChildEnv) != "1" {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")
		cmd.Env = append(os.Environ(), opaTransportStartupChildEnv+"=1")
		output, err := cmd.CombinedOutput()
		assert.NoError(t, err, "%s", output)
		return
	}
	logger, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	configureProcess(logger, 0, time.Second, time.Minute)

	_, ok := http.DefaultTransport.(*http.Transport)
	assert.True(t, ok, "configureProcess must leave http.DefaultTransport a *http.Transport, got %T", http.DefaultTransport)
	_, ok = httputil.UpstreamTransport().(*http.Transport)
	assert.False(t, ok, "configureProcess must install the upstream transport")

	var upstreamHits int
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	assert.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: upstream.Certificate().Raw,
	}), 0o600))

	query, err := rego.New(
		rego.Query("data.cachew.allow"),
		rego.Module("policy.rego", `
package cachew

default allow := false

allow if {
	resp := http.send({"method": "HEAD", "url": input.url, "tls_ca_cert_file": input.ca_file})
	resp.status_code == 200
}
`),
	).PrepareForEval(ctx)
	assert.NoError(t, err)

	results, err := query.Eval(ctx, rego.EvalInput(map[string]any{"url": upstream.URL, "ca_file": caFile}))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, true, results[0].Expressions[0].Value)
	assert.Equal(t, 1, upstreamHits)
}
