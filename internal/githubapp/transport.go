package githubapp

import (
	"context"
	"net/http"
	"strconv"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
)

type apiCallKey struct{}

type apiCall struct {
	host      string
	owner     string
	operation string
}

type apiTransport struct {
	base  http.RoundTripper
	calls metric.Int64Counter
}

// NewAPIClient copies a client and adds API call counts and low-quota warnings.
func NewAPIClient(base *http.Client) *http.Client {
	client := *base
	transport := client.Transport
	if transport == nil {
		transport = httputil.UpstreamTransport()
	}
	client.Transport = &apiTransport{
		base: transport,
		calls: metrics.NewMetric[metric.Int64Counter](otel.Meter("cachew.github"), "github.api_calls", "{call}",
			"Number of GitHub API calls by owner and operation"),
	}
	return &client
}

// WithInstallationCooldown scopes upstream cooldowns to a GitHub App
// installation, since GitHub limits each installation separately.
func WithInstallationCooldown(ctx context.Context, installationID string) context.Context {
	return httputil.WithCooldownScope(ctx, "github-installation:"+installationID)
}

// WithAPIMetrics attributes an API request to its credential owner and operation.
func WithAPIMetrics(req *http.Request, owner, operation string) *http.Request {
	call := apiCall{host: req.URL.Host, owner: owner, operation: operation}
	return req.WithContext(context.WithValue(req.Context(), apiCallKey{}, call))
}

func (t *apiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return resp, errors.Wrap(err, "GitHub API request")
	}
	call, ok := req.Context().Value(apiCallKey{}).(apiCall)
	// Redirects to asset storage retain the context but are not GitHub API calls.
	if !ok || call.host != req.URL.Host {
		return resp, nil
	}
	t.calls.Add(req.Context(), 1, metric.WithAttributes(
		attribute.String("owner", call.owner), attribute.String("operation", call.operation),
	))
	remaining, remainingErr := strconv.ParseInt(resp.Header.Get("X-Ratelimit-Remaining"), 10, 64)
	limit, limitErr := strconv.ParseInt(resp.Header.Get("X-Ratelimit-Limit"), 10, 64)
	if remainingErr == nil && limitErr == nil && limit > 0 && remaining >= 0 && float64(remaining) < float64(limit)/10 {
		logging.FromContext(req.Context()).WarnContext(req.Context(), "GitHub API rate limit low",
			"owner", call.owner, "operation", call.operation, "remaining", remaining, "limit", limit,
			"used", resp.Header.Get("X-Ratelimit-Used"), "resets_at", resp.Header.Get("X-Ratelimit-Reset"))
	}
	return resp, nil
}
