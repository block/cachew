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

// HermitConfig for the Hermit strategy.
type HermitConfig struct {
	// Future configuration can be added here
}

// Hermit implements caching for Hermit package downloads.
// This strategy acts as a router that:
// 1. Detects GitHub release URLs and redirects to github-releases strategy
// 2. Handles all other URLs directly with simple HTTP GET.
type Hermit struct {
	cache  cache.Cache
	client *http.Client
	logger *slog.Logger
	mux    Mux
}

var _ Strategy = (*Hermit)(nil)

// NewHermit creates a new Hermit caching strategy.
func NewHermit(ctx context.Context, _ HermitConfig, _ jobscheduler.Scheduler, cache cache.Cache, mux Mux) (*Hermit, error) {
	logger := logging.FromContext(ctx)

	s := &Hermit{
		cache:  cache,
		client: http.DefaultClient,
		logger: logger,
		mux:    mux,
	}

	// Mount at /hermit/{host}/{path...}
	// Example: /hermit/go.dev/dl/go1.21.0.linux-amd64.tar.gz
	// Example: /hermit/github.com/squareup/repo/releases/download/v1.0/file.tar.gz
	mux.Handle("GET /hermit/{host}/{path...}", http.HandlerFunc(s.handleRequest))

	logger.InfoContext(ctx, "Hermit strategy initialized")

	return s, nil
}

func (s *Hermit) String() string { return "hermit" }

// handleRequest routes the request to the appropriate handler.
// GitHub releases are redirected to the github-releases strategy.
// Everything else is handled directly.
func (s *Hermit) handleRequest(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	path := r.PathValue("path")

	// Check if this is a GitHub release URL
	if host == "github.com" && strings.Contains(path, "/releases/download/") {
		// Redirect to github-releases strategy using handler pattern with no-cache
		s.redirectToGitHubReleases(w, r, path)
		return
	}

	// Not a GitHub release, handle directly with handler pattern
	s.handleNonGitHub(w, r, host, path)
}

// redirectToGitHubReleases redirects to the github-releases strategy without caching.
// Uses handler pattern with NoOpCache to avoid double caching.
// The github-releases strategy will handle caching.
func (s *Hermit) redirectToGitHubReleases(w http.ResponseWriter, r *http.Request, path string) {
	newPath := "/github.com/" + path

	s.logger.DebugContext(r.Context(), "Redirecting to github-releases strategy",
		slog.String("original_path", r.URL.Path),
		slog.String("redirect_path", newPath))

	// Use handler pattern with NoOpCache (no caching for the redirect)
	// The github-releases strategy will handle caching
	h := handler.New(s.client, cache.NoOpCache()).
		Transform(func(r *http.Request) (*http.Request, error) {
			// Create internal request to github-releases strategy
			// Build internal URL from the incoming request
			internalURL := &url.URL{
				Scheme:   "http",
				Host:     r.Host,
				Path:     newPath,
				RawQuery: r.URL.RawQuery,
			}

			// Use https if original request was TLS
			if r.TLS != nil {
				internalURL.Scheme = "https"
			}

			req, err := http.NewRequestWithContext(r.Context(), r.Method, internalURL.String(), nil)
			if err != nil {
				return nil, errors.Wrap(err, "create internal redirect request")
			}

			// Copy headers from original request
			req.Header = r.Header.Clone()

			return req, nil
		})

	h.ServeHTTP(w, r)
}

// handleNonGitHub handles non-GitHub release downloads using the handler pattern.
func (s *Hermit) handleNonGitHub(w http.ResponseWriter, r *http.Request, host, path string) {
	h := handler.New(s.client, s.cache).
		CacheKey(func(r *http.Request) string {
			// Cache key is the original URL with https:// scheme
			return buildOriginalURL(host, path, r.URL.RawQuery)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			// Build original URL with https:// and create HTTP GET request
			originalURL := buildOriginalURL(host, path, r.URL.RawQuery)

			s.logger.DebugContext(r.Context(), "Fetching Hermit package",
				slog.String("url", originalURL))

			return http.NewRequestWithContext(r.Context(), http.MethodGet, originalURL, nil)
		})

	h.ServeHTTP(w, r)
}

// buildOriginalURL reconstructs the original URL from the host and path.
// Example: host="go.dev", path="dl/go1.21.0.tar.gz" → https://go.dev/dl/go1.21.0.tar.gz
func buildOriginalURL(host, path, query string) string {
	// Use url.URL for proper URL construction (handles encoding, edge cases)
	u := &url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     "/" + path,
		RawQuery: query,
	}
	return u.String()
}
