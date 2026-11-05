package githubapp

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metrics"
)

const rateLimitPollInterval = time.Minute
const rateLimitPollTimeout = 15 * time.Second

type rateLimitValues struct {
	Remaining int64 `json:"remaining"`
	Limit     int64 `json:"limit"`
	Used      int64 `json:"used"`
}

// RateLimitPoller samples the primary API budgets of known App installations.
type RateLimitPoller struct {
	manager   *TokenManager
	interval  time.Duration
	remaining metric.Int64Gauge
	limit     metric.Int64Gauge
	used      metric.Int64Gauge
}

// NewRateLimitPoller uses a one-minute interval when interval is nonpositive.
func NewRateLimitPoller(manager *TokenManager, interval time.Duration) *RateLimitPoller {
	if interval <= 0 {
		interval = rateLimitPollInterval
	}
	meter := otel.Meter("cachew.github")
	return &RateLimitPoller{
		manager: manager, interval: interval,
		remaining: metrics.NewMetric[metric.Int64Gauge](meter, "github.ratelimit_remaining", "{request}",
			"GitHub API rate limit remaining requests"),
		limit: metrics.NewMetric[metric.Int64Gauge](meter, "github.ratelimit_limit", "{request}",
			"GitHub API rate limit total allowed requests"),
		used: metrics.NewMetric[metric.Int64Gauge](meter, "github.ratelimit_used", "{request}",
			"GitHub API rate limit used requests"),
	}
}

// Run polls immediately and then periodically until ctx is cancelled.
func (p *RateLimitPoller) Run(ctx context.Context) {
	if p.manager == nil {
		return
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := p.poll(ctx); err != nil && ctx.Err() == nil {
			logging.FromContext(ctx).WarnContext(ctx, "Failed to poll GitHub rate limit", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (p *RateLimitPoller) poll(ctx context.Context) error {
	var errs []error
	if p.manager.fallbackApp != nil {
		lookupCtx, cancel := context.WithTimeout(ctx, rateLimitPollTimeout)
		_, err := p.manager.fallbackApp.lookupInstallationID(lookupCtx, p.manager.fallbackOrg)
		cancel()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "resolve fallback installation"))
		}
	}
	for _, app := range p.manager.apps {
		seen := map[string]bool{}
		for owner, installationID := range app.knownInstallations() {
			if ctx.Err() != nil {
				return errors.WithStack(ctx.Err())
			}
			if seen[installationID] {
				continue
			}
			seen[installationID] = true
			if err := p.pollInstallation(ctx, app, owner, installationID); err != nil {
				errs = append(errs, errors.Wrapf(err, "app %s installation %s (%s)", app.appID, installationID, owner))
			}
		}
	}
	return errors.Join(errs...)
}

func (p *RateLimitPoller) pollInstallation(ctx context.Context, app *appState, owner, installationID string) error {
	ctx, cancel := context.WithTimeout(ctx, rateLimitPollTimeout)
	defer cancel()
	token, err := app.getToken(ctx, owner)
	if err != nil {
		return errors.Wrap(err, "get installation token")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, app.apiBase+"/rate_limit", nil)
	if err != nil {
		return errors.Wrap(err, "create rate-limit request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	resp, err := app.httpClient.Do(WithAPIMetrics(req, owner, "get_rate_limit"))
	if err != nil {
		return errors.Wrap(err, "fetch rate limit")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("GitHub API returned status %d", resp.StatusCode)
	}
	var result struct {
		Resources map[string]rateLimitValues `json:"resources"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return errors.Wrap(err, "decode rate limit")
	}
	for resource, values := range result.Resources {
		attrs := metric.WithAttributes(
			attribute.String("app", app.appID), attribute.String("installation_id", installationID),
			attribute.String("owner", owner), attribute.String("resource", resource),
		)
		p.remaining.Record(ctx, values.Remaining, attrs)
		p.limit.Record(ctx, values.Limit, attrs)
		p.used.Record(ctx, values.Used, attrs)
	}
	return nil
}

func (a *appState) knownInstallations() map[string]string {
	a.installationMu.RLock()
	defer a.installationMu.RUnlock()
	return maps.Clone(a.installationCache)
}
