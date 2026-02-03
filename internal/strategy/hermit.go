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

func init() {
	Register("hermit", "Caches Hermit package downloads.", NewHermit)
}

type HermitConfig struct{}

// Hermit caches Hermit package downloads.
// Acts as a smart router: GitHub releases redirect to github-releases strategy,
// all other sources are handled directly.
type Hermit struct {
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	mux    Mux
}

var _ Strategy = (*Hermit)(nil)

func NewHermit(ctx context.Context, _ HermitConfig, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*Hermit, error) {
	logger := logging.FromContext(ctx)

	s := &Hermit{
		cache:  cache,
		client: http.DefaultClient,
		logger: logger,
		mux:    mux,
	}

	mux.Handle("GET /hermit/{host}/{path...}", http.HandlerFunc(s.handleRequest))

	logger.InfoContext(ctx, "Hermit strategy initialized")

	return s, nil
}

func (s *Hermit) String() string { return "hermit" }

func (s *Hermit) handleRequest(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	path := r.PathValue("path")

	if host == "github.com" && strings.Contains(path, "/releases/download/") {
		s.redirectToGitHubReleases(w, r, path)
		return
	}

	s.handleNonGitHub(w, r, host, path)
}

// redirectToGitHubReleases delegates to github-releases strategy using NoOpCache
// to avoid double caching (github-releases will cache the actual response).
func (s *Hermit) redirectToGitHubReleases(w http.ResponseWriter, r *http.Request, path string) {
	newPath := "/github.com/" + path

	s.logger.DebugContext(r.Context(), "Redirecting to github-releases strategy",
		slog.String("original_path", r.URL.Path),
		slog.String("redirect_path", newPath))

	h := handler.New(s.client, cache.NoOpCache()).
		Transform(func(r *http.Request) (*http.Request, error) {
			internalURL := &url.URL{
				Scheme:   "http",
				Host:     r.Host,
				Path:     newPath,
				RawQuery: r.URL.RawQuery,
			}

			if r.TLS != nil {
				internalURL.Scheme = "https"
			}

			req, err := http.NewRequestWithContext(r.Context(), r.Method, internalURL.String(), nil)
			if err != nil {
				return nil, errors.Wrap(err, "create internal redirect request")
			}

			req.Header = r.Header.Clone()

			return req, nil
		})

	h.ServeHTTP(w, r)
}

func (s *Hermit) handleNonGitHub(w http.ResponseWriter, r *http.Request, host, path string) {
	h := handler.New(s.client, s.cache).
		CacheKey(func(r *http.Request) string {
			return buildOriginalURL(host, path, r.URL.RawQuery)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			originalURL := buildOriginalURL(host, path, r.URL.RawQuery)

			s.logger.DebugContext(r.Context(), "Fetching Hermit package",
				slog.String("url", originalURL))

			return http.NewRequestWithContext(r.Context(), http.MethodGet, originalURL, nil)
		})

	h.ServeHTTP(w, r)
}

func buildOriginalURL(host, path, query string) string {
	u := &url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     "/" + path,
		RawQuery: query,
	}
	return u.String()
}
