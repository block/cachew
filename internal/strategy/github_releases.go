package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

func RegisterGitHubReleases(r *Registry, tokenManagerProvider githubapp.TokenManagerProvider) {
	Register(r, "github-releases", "Caches public and authenticated GitHub releases.", func(ctx context.Context, config GitHubReleasesConfig, cache cache.Cache, mux Mux) (*GitHubReleases, error) {
		return NewGitHubReleases(ctx, config, cache, mux, tokenManagerProvider)
	})
}

type GitHubReleasesConfig struct {
	Token       string   `hcl:"token,optional" help:"GitHub token for authentication."`
	PrivateOrgs []string `hcl:"private-orgs" help:"List of private GitHub organisations."`
}

// The GitHubReleases strategy fetches private (and public) release binaries from GitHub.
type GitHubReleases struct {
	config       GitHubReleasesConfig
	cache        cache.Cache
	client       *http.Client
	tokenManager *githubapp.TokenManager
}

// NewGitHubReleases creates a [Strategy] that fetches private (and public) release binaries from GitHub.
func NewGitHubReleases(ctx context.Context, config GitHubReleasesConfig, cache cache.Cache, mux Mux, tokenManagerProvider githubapp.TokenManagerProvider) (*GitHubReleases, error) {
	logger := logging.FromContext(ctx)

	// Get GitHub App token manager if configured
	tokenManager, err := tokenManagerProvider()
	if err != nil {
		return nil, errors.Wrap(err, "create token manager")
	}
	if tokenManager != nil {
		logger.InfoContext(ctx, "Using GitHub App authentication for github-releases strategy")
	} else if config.Token == "" {
		logger.WarnContext(ctx, "No authentication configured for github-releases strategy")
	}

	s := &GitHubReleases{
		config:       config,
		cache:        cache,
		client:       http.DefaultClient,
		tokenManager: tokenManager,
	}
	// eg. https://github.com/alecthomas/chroma/releases/download/v2.21.1/chroma-2.21.1-darwin-amd64.tar.gz
	h := handler.New(s.client, cache).
		CacheKey(func(r *http.Request) string {
			org := r.PathValue("org")
			repo := r.PathValue("repo")
			release := r.PathValue("release")
			file := r.PathValue("file")
			return fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/%s", org, repo, release, file)
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			org := r.PathValue("org")
			repo := r.PathValue("repo")
			release := r.PathValue("release")
			file := r.PathValue("file")
			return s.downloadRelease(r.Context(), org, repo, release, file)
		})
	mux.Handle("GET /github.com/{org}/{repo}/releases/download/{release}/{file}", h)
	return s, nil
}

var _ Strategy = (*GitHubReleases)(nil)

func (g *GitHubReleases) String() string { return "github-releases" }

// newGitHubRequest creates a new HTTP request with GitHub API headers and authentication.
func (g *GitHubReleases) newGitHubRequest(ctx context.Context, url, accept, org string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create request")
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("X-Github-Api-Version", "2022-11-28")

	// Try GitHub App authentication first, fall back to static token
	if g.tokenManager != nil && org != "" {
		token, err := g.tokenManager.GetTokenForOrg(ctx, org)
		if err != nil {
			logging.FromContext(ctx).WarnContext(ctx, "Failed to get GitHub App token, falling back to static token",
				slog.String("org", org),
				slog.String("error", err.Error()))
		} else if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}

	// Fall back to static token if GitHub App not used or failed
	if req.Header.Get("Authorization") == "" && g.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+g.config.Token)
	}

	return req, nil
}

// downloadRelease creates an HTTP request to download a GitHub release asset.
// For private orgs, it uses the GitHub API to find and download the asset.
// For public orgs, it constructs a direct download URL.
func (g *GitHubReleases) downloadRelease(ctx context.Context, org, repo, release, file string) (*http.Request, error) {
	isPrivate := slices.Contains(g.config.PrivateOrgs, org)

	logger := logging.FromContext(ctx).With(
		slog.String("org", org),
		slog.String("repo", repo),
		slog.String("release", release),
		slog.String("file", file))

	realURL := fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/%s", org, repo, release, file)
	if !isPrivate {
		// Public release - use direct download URL
		logger.DebugContext(ctx, "Using public download URL")
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, realURL, nil)
		if err != nil {
			return nil, httputil.Errorf(http.StatusInternalServerError, "create download request")
		}
		return req, nil
	}

	// Use GitHub API to get release info and find the asset
	logger.DebugContext(ctx, "Using GitHub API for private release")
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/tags/%s", org, repo, release)
	req, err := g.newGitHubRequest(ctx, apiURL, "application/vnd.github+json", org)
	if err != nil {
		return nil, httputil.Errorf(http.StatusInternalServerError, "create API request")
	}

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, httputil.Errorf(http.StatusBadGateway, "fetch release info failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httputil.Errorf(resp.StatusCode, "GitHub API returned %d", resp.StatusCode)
	}

	var releaseInfo struct {
		Assets []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&releaseInfo); err != nil {
		return nil, httputil.Errorf(http.StatusBadGateway, "decode release info failed: %w", err)
	}

	// Find the matching asset
	var assetURL string
	for _, asset := range releaseInfo.Assets {
		if asset.Name == file {
			assetURL = asset.URL
			break
		}
	}
	if assetURL == "" {
		logger.ErrorContext(ctx, "Asset not found in release", slog.Int("assets_count", len(releaseInfo.Assets)))
		return nil, httputil.Errorf(http.StatusNotFound, "asset %s not found in release %s", file, release)
	}

	logger.DebugContext(ctx, "Found asset in release", slog.String("asset_url", assetURL))

	// Create request for the asset download
	req, err = g.newGitHubRequest(ctx, assetURL, "application/octet-stream", org)
	if err != nil {
		return nil, httputil.Errorf(http.StatusInternalServerError, "create asset request failed: %w", err)
	}
	return req, nil
}
