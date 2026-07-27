package strategy

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/strategy/handler"
)

// RegisterAndroidSDK registers the Android SDK caching strategy.
func RegisterAndroidSDK(r *Registry) {
	Register(r, "android-sdk", "Caches Android SDK package downloads.", NewAndroidSDK)
}

// androidSDKArchiveTTL is the TTL used for immutable archive downloads. Archives
// use versioned filenames so a given URL's content never changes. The actual TTL
// is bounded by the cache backend's max-ttl setting.
const androidSDKArchiveTTL = 365 * 24 * time.Hour

// AndroidSDKConfig holds configuration for the Android SDK caching strategy.
//
// In HCL it looks something like this:
//
//	android-sdk {
//	  feed-ttl = "1h"
//	}
type AndroidSDKConfig struct {
	// FeedTTL controls how long mutable feed/manifest XML files are cached.
	// Archive downloads use a long TTL (1 year, bounded by the cache backend's
	// max-ttl). The Android SDK protocol uses XML for all mutable manifests and
	// .zip for all immutable archives.
	FeedTTL time.Duration `hcl:"feed-ttl,optional" help:"Cache TTL for mutable SDK feed XML files" default:"1h"`
}

// AndroidSDK caches Android SDK downloads. It routes all requests through
// /android-sdk/{host}/{path...}, reconstructing the original URL and caching
// the response. XML feeds get a short TTL; archive downloads get a long TTL.
type AndroidSDK struct {
	config AndroidSDKConfig
	cache  cache.Cache
	client *http.Client
}

var _ Strategy = (*AndroidSDK)(nil)

// NewAndroidSDK creates and registers the Android SDK strategy.
func NewAndroidSDK(ctx context.Context, config AndroidSDKConfig, c cache.Cache, mux Mux) (*AndroidSDK, error) {
	logger := logging.FromContext(ctx)

	s := &AndroidSDK{
		config: config,
		cache:  c,
		client: &http.Client{},
	}

	hdlr := handler.New(s.client, c).
		CacheKey(func(r *http.Request) string {
			return s.buildOriginalURL(r)
		}).
		TTL(func(r *http.Request) time.Duration {
			if strings.HasSuffix(r.URL.Path, ".xml") {
				return s.config.FeedTTL
			}
			return androidSDKArchiveTTL
		}).
		Transform(func(r *http.Request) (*http.Request, error) {
			originalURL := s.buildOriginalURL(r)
			return http.NewRequestWithContext(r.Context(), http.MethodGet, originalURL, nil)
		})

	mux.Handle("GET /android-sdk/{host}/{path...}", hdlr)
	logger.InfoContext(ctx, "Android SDK strategy initialized", "feed_ttl", config.FeedTTL)
	return s, nil
}

// String implements the Strategy interface.
func (s *AndroidSDK) String() string { return "android-sdk" }

func (s *AndroidSDK) buildOriginalURL(r *http.Request) string {
	host := r.PathValue("host")
	path := r.PathValue("path")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "https", Host: host, Path: path, RawQuery: r.URL.RawQuery}).String()
}
