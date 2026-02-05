package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func RegisterHermit(r *Registry, cachewURL string) {
	Register(r, "hermit", "Caches Hermit package downloads.", func(ctx context.Context, config HermitConfig, c cache.Cache, mux Mux) (*Hermit, error) {
		return NewHermit(ctx, cachewURL, config, nil, c, mux)
	})
}

type HermitConfig struct {
	GitHubBaseURL string `hcl:"github-base-url" help:"Base URL for GitHub release redirects" default:"${CACHEW_URL}/github.com"`
}

// Hermit caches Hermit package downloads.
// Acts as a smart router: GitHub releases redirect to github-releases strategy,
// all other sources are handled directly.
type Hermit struct {
	config          HermitConfig
	cache           cache.Cache
	client          *http.Client
	logger          *slog.Logger
	mux             Mux
	redirectHandler http.Handler
	directHandler   http.Handler
}

var _ Strategy = (*Hermit)(nil)

func NewHermit(ctx context.Context, cachewURL string, config HermitConfig, _ jobscheduler.Scheduler, c cache.Cache, mux Mux) (*Hermit, error) {
	logger := logging.FromContext(ctx)

	s := &Hermit{
		config: config,
		cache:  c,
		client: http.DefaultClient,
		logger: logger,
		mux:    mux,
	}

	s.directHandler = s.createDirectHandler(c)
	mux.Handle("GET /hermit/{host}/{path...}", s.directHandler)

	if config.GitHubBaseURL != "" {
		isInternalRedirect := strings.HasPrefix(config.GitHubBaseURL, cachewURL)
		s.redirectHandler = s.createRedirectHandler(isInternalRedirect, c)
		mux.Handle("GET /hermit/github.com/{path...}", s.redirectHandler)
		logger.InfoContext(ctx, "Hermit strategy initialized",
			slog.String("github_base_url", config.GitHubBaseURL),
			slog.Bool("internal_redirect", isInternalRedirect))
	} else {
		logger.InfoContext(ctx, "Hermit strategy initialized")
	}

	return s, nil
}

func (s *Hermit) String() string { return "hermit" }

func (s *Hermit) createDirectHandler(c cache.Cache) http.Handler {
	return handler.New(s.client, c).
		CacheKey(func(r *http.Request) string {
			return s.buildOriginalURL(r)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			return s.buildDirectRequest(r)
		})
}

func (s *Hermit) createRedirectHandler(isInternalRedirect bool, c cache.Cache) http.Handler {
	var cacheBackend cache.Cache
	if isInternalRedirect {
		cacheBackend = cache.NoOpCache()
	} else {
		cacheBackend = c
	}

	return handler.New(s.client, cacheBackend).
		CacheKey(func(r *http.Request) string {
			return s.buildGitHubURL(r)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			s.logger.DebugContext(r.Context(), "Redirect handler called for GitHub release")
			return s.buildRedirectRequest(r)
		})
}

func (s *Hermit) buildGitHubURL(r *http.Request) string {
	return buildURL("https", "github.com", r.PathValue("path"), r.URL.RawQuery)
}

func (s *Hermit) buildRedirectRequest(r *http.Request) (*http.Request, error) {
	path := ensureLeadingSlash(r.PathValue("path"))
	redirectURL := s.config.GitHubBaseURL + path
	if r.URL.RawQuery != "" {
		redirectURL += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, redirectURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create internal redirect request")
	}

	req.Header = r.Header.Clone()
	return req, nil
}

func (s *Hermit) buildDirectRequest(r *http.Request) (*http.Request, error) {
	originalURL := s.buildOriginalURL(r)

	s.logger.DebugContext(r.Context(), "Fetching Hermit package",
		slog.String("url", originalURL))

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, originalURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	return req, nil
}

func (s *Hermit) buildOriginalURL(r *http.Request) string {
	return buildURL("https", r.PathValue("host"), r.PathValue("path"), r.URL.RawQuery)
}

func buildURL(scheme, host, path, query string) string {
	u := &url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     ensureLeadingSlash(path),
		RawQuery: query,
	}
	return u.String()
}

func ensureLeadingSlash(path string) string {
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}
