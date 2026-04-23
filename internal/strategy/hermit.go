package strategy

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/jobscheduler"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func RegisterHermit(r *Registry) {
	Register(r, "hermit", "Caches Hermit package downloads.", func(ctx context.Context, config HermitConfig, c cache.Cache, mux Mux) (*Hermit, error) {
		return NewHermit(ctx, config, nil, c, mux)
	})
}

const defaultGitHubBaseURL = "http://127.0.0.1:8080/github.com"

type HermitConfig struct {
	GitHubBaseURL string        `hcl:"github-base-url" help:"Base URL for GitHub release redirects" default:"http://127.0.0.1:8080/github.com"`
	BootstrapTTL  time.Duration `hcl:"bootstrap-ttl,optional" help:"Cache TTL for mutable Hermit bootstrap files (binary, install script, install hash)" default:"1h"`
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

func NewHermit(ctx context.Context, config HermitConfig, _ jobscheduler.Scheduler, c cache.Cache, mux Mux) (*Hermit, error) {
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
		isInternalRedirect := config.GitHubBaseURL == defaultGitHubBaseURL
		s.redirectHandler = s.createRedirectHandler(isInternalRedirect, c)
		mux.Handle("GET /hermit/github.com/{org}/{repo}/releases/download/{path...}", s.redirectHandler)
		logger.InfoContext(ctx, "Hermit strategy initialized", "github_base_url", config.GitHubBaseURL,
			"internal_redirect", isInternalRedirect)
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
		}).
		TTL(func(r *http.Request) time.Duration {
			if isHermitBootstrapFile(r.PathValue("path")) {
				return s.config.BootstrapTTL
			}
			return 0
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

func (s *Hermit) githubReleasePath(r *http.Request) string {
	return r.PathValue("org") + "/" + r.PathValue("repo") + "/releases/download/" + r.PathValue("path")
}

func (s *Hermit) buildGitHubURL(r *http.Request) string {
	return buildURL("https", "github.com", s.githubReleasePath(r), r.URL.RawQuery)
}

func (s *Hermit) buildRedirectRequest(r *http.Request) (*http.Request, error) {
	path := ensureLeadingSlash(s.githubReleasePath(r))
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

	s.logger.DebugContext(r.Context(), "Fetching Hermit package", "url", originalURL)

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

// isHermitBootstrapFile returns true if the path refers to the hermit binary,
// install script, or install hash. These files are mutable (content changes on
// new releases without the URL changing) so they need a short cache TTL.
//
// The bootstrap files are:
//   - hermit-{os}-{arch}.gz: the hermit binary, downloaded by install.sh
//     (see cashapp/hermit files/install.sh.tmpl)
//   - install.sh: the installer script generated by geninstaller
//     (see cashapp/hermit cmd/geninstaller/main.go)
//   - install_hash: SHA-256 digest of install.sh, used by some distribution
//     channels to detect when the installer has changed
//
// The path includes the channel prefix, e.g. "stable/hermit-linux-amd64.gz".
// Note: public hermit (GitHub releases) goes through the redirect handler,
// not the direct handler, so this only affects non-GitHub distribution hosts.
func isHermitBootstrapFile(path string) bool {
	base := path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		base = path[i+1:]
	}
	// hermit-{os}-{arch}.gz binary pattern from install.sh.tmpl
	if strings.HasPrefix(base, "hermit-") && strings.HasSuffix(base, ".gz") {
		return true
	}
	return base == "install.sh" || base == "install_hash"
}

func ensureLeadingSlash(path string) string {
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}
