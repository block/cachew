package strategy

import (
	"cmp"
	"context"
	_ "crypto/sha256" // Registers the digest algorithms that go-digest validates against.
	_ "crypto/sha512"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/alecthomas/errors"
	"github.com/opencontainers/go-digest"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/spool"
	"github.com/block/cachew/internal/strategy/handler"
)

// RegisterOCI registers the "oci" strategy.
func RegisterOCI(r *Registry, spools spool.Provider) {
	Register(r, "oci", "Pull-through cache for an OCI (Docker) registry.", func(ctx context.Context, config OCIConfig, c cache.Cache, mux Mux) (*OCI, error) {
		registry, err := spools()
		if err != nil {
			return nil, errors.Wrap(err, "create spool registry")
		}
		return NewOCI(ctx, config, c, mux, registry)
	})
}

// OCIConfig configures a pull-through cache for one OCI registry. Registries are
// separate blocks because each has its own credentials and client-facing hostnames.
type OCIConfig struct {
	Registry        string        `hcl:"registry,label" help:"Registry name as clients reference it, e.g. docker.io."`
	Upstream        string        `hcl:"upstream,optional" help:"API URL, for registries served from a different host than their name (defaults to https://<registry>)."`
	Hosts           []string      `hcl:"hosts,optional" help:"Hostnames serving this registry at /v2/, for clients that cannot use a path prefix (Docker, Podman)."`
	Username        string        `hcl:"username,optional" help:"Upstream username, shared by all clients."`
	Password        string        `hcl:"password,optional" help:"Upstream password or token, shared by all clients."`
	TagTTL          time.Duration `hcl:"tag-ttl,optional" help:"Maximum tag staleness; longer values mean fewer upstream checks." default:"5m"`
	BlobChunkSizeMB uint          `hcl:"blob-chunk-size-mb,optional" help:"Size of each parallel range request when filling a blob, in megabytes." default:"32"`
	BlobConcurrency uint          `hcl:"blob-concurrency,optional" help:"Number of parallel range requests when filling a blob." default:"4"`
	Rewrites        []OCIRewrite  `hcl:"rewrite,block" help:"Repository name rewrites, for names the upstream does not serve directly."`
}

// OCIRewrite maps a repository name as clients send it to the name the
// upstream serves, e.g. Docker Hub's implicit "library/" namespace.
type OCIRewrite struct {
	Pattern     string `hcl:"pattern,label" help:"Regular expression matched against the repository name."`
	Replacement string `hcl:"replacement" help:"Replacement, with $1-style references."`
}

// The OCI [Strategy] is a read-only pull-through cache for one OCI registry.
// Tags are revalidated with HEAD, which Docker Hub does not count against pull limits.
type OCI struct {
	registry  string
	upstream  *url.URL
	rewrites  []ociRewrite
	tagTTL    time.Duration
	client    *http.Client
	blobs     http.Handler
	manifests http.Handler
	logger    *slog.Logger
}

var _ Strategy = (*OCI)(nil)

const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

type ociRewrite struct {
	pattern     *regexp.Regexp
	replacement string
}

// NewOCI creates an OCI strategy and registers its routes on mux.
func NewOCI(ctx context.Context, config OCIConfig, c cache.Cache, mux Mux, spools *spool.Registry) (*OCI, error) {
	if u, err := url.Parse("//" + config.Registry); err != nil || u.Host != config.Registry || config.Registry == "" {
		return nil, errors.Errorf("invalid registry name %q", config.Registry)
	}
	rawUpstream := cmp.Or(config.Upstream, schemeHTTPS+"://"+config.Registry)
	upstream, err := url.Parse(rawUpstream)
	if err != nil || upstream.Host == "" || (upstream.Scheme != schemeHTTP && upstream.Scheme != schemeHTTPS) ||
		strings.Trim(upstream.Path, "/") != "" || upstream.User != nil || upstream.RawQuery != "" {
		return nil, errors.Errorf("invalid upstream %q: expected an HTTP(S) URL without a path", rawUpstream)
	}
	if config.TagTTL <= 0 {
		return nil, errors.Errorf("tag-ttl must be positive, got %s", config.TagTTL)
	}
	if config.BlobChunkSizeMB == 0 || config.BlobConcurrency == 0 {
		return nil, errors.New("blob-chunk-size-mb and blob-concurrency must be positive")
	}
	rewrites, err := compileOCIRewrites(config.Rewrites)
	if err != nil {
		return nil, err
	}
	transport, err := newOCITransport(upstream, config.Username, config.Password, httputil.UpstreamTransport())
	if err != nil {
		return nil, err
	}

	o := &OCI{
		registry: config.Registry,
		upstream: upstream,
		rewrites: rewrites,
		tagTTL:   config.TagTTL,
		client:   &http.Client{Transport: transport},
		logger:   logging.FromContext(ctx),
	}
	o.blobs = handler.New(o.client, c, spools).Metrics("oci", upstream.Hostname()).
		CacheKey(o.blobKey).
		Transform(o.transform).
		Verify(o.verifier).
		ParallelFetch(int64(config.BlobChunkSizeMB)<<20, int(config.BlobConcurrency))
	o.manifests = handler.New(o.client, c, spools).Metrics("oci", upstream.Hostname()).
		CacheKey(o.manifestKey).
		Transform(o.transform).
		Vary(o.manifestVary).
		Verify(o.verifier).
		Revalidate("Docker-Content-Digest", o.manifestFreshFor)

	// Patterns omit the method so that pushes reach ServeHTTP and get a 405
	// explaining the mirror is read-only.
	patterns := []string{"/oci/" + config.Registry + "/v2/{rest...}"}
	for _, host := range config.Hosts {
		patterns = append(patterns, host+"/v2/{rest...}")
	}
	for _, pattern := range patterns {
		mux.Handle(pattern, o)
	}
	o.logger.InfoContext(ctx, "Registered OCI registry", "registry", config.Registry, "upstream", upstream, "patterns", patterns)
	return o, nil
}

func compileOCIRewrites(configs []OCIRewrite) ([]ociRewrite, error) {
	rewrites := make([]ociRewrite, 0, len(configs))
	for _, config := range configs {
		pattern, err := regexp.Compile(config.Pattern)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid rewrite pattern %q", config.Pattern)
		}
		rewrites = append(rewrites, ociRewrite{pattern: pattern, replacement: config.Replacement})
	}
	return rewrites, nil
}

func (o *OCI) String() string { return "oci:" + o.registry }

// ServeHTTP answers the OCI Distribution API ping and routes pulls to the
// blob, manifest or pass-through path.
func (o *OCI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "registry mirror is read-only", http.StatusMethodNotAllowed)
		return
	}
	rest := r.PathValue("rest")
	if rest == "" {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.WriteHeader(http.StatusOK)
		return
	}
	req, err := o.parseRequest(rest)
	if err != nil {
		if responder, ok := errors.AsType[httputil.HTTPResponder](err); ok {
			responder.WriteHTTP(w, r)
		} else {
			httputil.ErrorResponse(w, r, http.StatusBadRequest, err.Error())
		}
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), ociRequestKey{}, req))
	switch req.kind {
	case ociBlob:
		o.blobs.ServeHTTP(w, r)
	case ociManifest:
		o.manifests.ServeHTTP(w, r)
	case ociTagList, ociReferrers:
		if err := o.passThrough(w, r); err != nil {
			o.logger.WarnContext(r.Context(), "Failed to stream upstream response", "url", r.URL, "error", err)
		}
	}
}

// parseRequest parses the path after /v2/ and validates it after rewriting,
// so rewrites can turn client names into names the upstream accepts.
func (o *OCI) parseRequest(path string) (ociRequest, error) {
	req, err := parseOCIPath(path)
	if err != nil {
		return ociRequest{}, err
	}
	req.repository = o.rewrite(req.repository)
	return req, req.validate()
}

func (o *OCI) rewrite(repository string) string {
	for _, rw := range o.rewrites {
		if rw.pattern.MatchString(repository) {
			return rw.pattern.ReplaceAllString(repository, rw.replacement)
		}
	}
	return repository
}

// passThrough forwards uncached endpoints, whose responses change too often
// or are too cheap to be worth caching.
func (o *OCI) passThrough(w http.ResponseWriter, r *http.Request) error {
	req, err := o.transform(r)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusInternalServerError, "failed to create upstream request", "error", err)
		return nil
	}
	req.Method = r.Method
	if accept := r.Header.Values("Accept"); len(accept) > 0 {
		req.Header["Accept"] = accept
	}
	resp, err := o.client.Do(req)
	if err != nil {
		httputil.ErrorResponse(w, r, http.StatusBadGateway, "failed to fetch from upstream", "error", err)
		return nil
	}
	defer resp.Body.Close()
	maps.Copy(w.Header(), httputil.FilterHeaders(resp.Header, httputil.HopByHopHeaders...))
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		return errors.Wrap(err, "stream upstream response")
	}
	return nil
}

// transform builds the upstream request from the parsed path, so client paths
// and query parameters such as containerd's "ns" never reach the upstream.
func (o *OCI) transform(r *http.Request) (*http.Request, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, o.upstreamURL(r), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create upstream request")
	}
	return req, nil
}

func (o *OCI) upstreamURL(r *http.Request) string {
	req := ociRequestFrom(r.Context())
	u := o.upstream.JoinPath("v2", req.repository, req.kind.segment(), req.reference)
	if req.kind == ociTagList || req.kind == ociReferrers {
		query := r.URL.Query()
		query.Del("ns")
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func (o *OCI) blobKey(r *http.Request) string {
	return o.registry + "/blobs/" + ociRequestFrom(r.Context()).reference
}

func (o *OCI) manifestKey(r *http.Request) string {
	req := ociRequestFrom(r.Context())
	return o.registry + "/" + req.repository + "/manifests/" + req.reference
}

// manifestVary keys tag manifests on Accept, which selects between an index and
// a single-platform manifest. Digest references name exact bytes.
func (o *OCI) manifestVary(r *http.Request) map[string]string {
	if ociRequestFrom(r.Context()).digest != "" {
		return nil
	}
	return map[string]string{"Accept": normaliseAccept(r.Header.Values("Accept"))}
}

// normaliseAccept stops equivalent Accept headers that differ only in order,
// spacing or repetition from caching separate copies of a manifest.
func normaliseAccept(values []string) string {
	var types []string
	for _, value := range values {
		for t := range strings.SplitSeq(value, ",") {
			if t = strings.TrimSpace(t); t != "" {
				types = append(types, t)
			}
		}
	}
	slices.Sort(types)
	return strings.Join(slices.Compact(types), ",")
}

func (o *OCI) manifestFreshFor(r *http.Request) time.Duration {
	if ociRequestFrom(r.Context()).digest != "" {
		return 0
	}
	return o.tagTTL
}

func (o *OCI) verifier(r *http.Request) handler.Verifier {
	d := ociRequestFrom(r.Context()).digest
	if d == "" {
		return nil
	}
	return digestVerifier{Verifier: d.Verifier(), digest: d}
}

type digestVerifier struct {
	digest.Verifier
	digest digest.Digest
}

func (v digestVerifier) Verify() error {
	if !v.Verified() {
		return errors.Errorf("content does not match digest %s", v.digest)
	}
	return nil
}

type ociKind int

const (
	ociManifest ociKind = iota
	ociBlob
	ociTagList
	ociReferrers
)

func (k ociKind) segment() string {
	switch k {
	case ociManifest:
		return "manifests"
	case ociBlob:
		return "blobs"
	case ociTagList:
		return "tags"
	case ociReferrers:
		return "referrers"
	}
	panic("unknown OCI request kind")
}

// ociRequest is a parsed OCI Distribution API path. It travels in the request
// context so fills that outlive the request see the same rewritten repository.
type ociRequest struct {
	repository string
	kind       ociKind
	reference  string
	digest     digest.Digest
}

type ociRequestKey struct{}

func ociRequestFrom(ctx context.Context) ociRequest {
	req, ok := ctx.Value(ociRequestKey{}).(ociRequest)
	if !ok {
		panic("OCI request missing from context")
	}
	return req
}

// parseOCIPath splits the path after /v2/ from the end, because repository
// names may themselves contain slashes.
func parseOCIPath(path string) (ociRequest, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return ociRequest{}, httputil.Errorf(http.StatusNotFound, "unknown OCI API path %q", path)
	}
	repository := strings.Join(parts[:len(parts)-2], "/")
	segment, reference := parts[len(parts)-2], parts[len(parts)-1]
	for _, kind := range []ociKind{ociManifest, ociBlob, ociReferrers, ociTagList} {
		if segment != kind.segment() || (kind == ociTagList && reference != "list") {
			continue
		}
		return ociRequest{repository: repository, kind: kind, reference: reference}, nil
	}
	return ociRequest{}, httputil.Errorf(http.StatusNotFound, "unknown OCI API path %q", path)
}

// From the OCI Distribution specification.
var (
	ociRepositoryPattern = regexp.MustCompile(`^[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*$`)
	ociTagPattern        = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`)
)

// validate rejects malformed names and references, and records the digest of
// digest references.
func (req *ociRequest) validate() error {
	if len(req.repository) > 255 || !ociRepositoryPattern.MatchString(req.repository) {
		return httputil.Errorf(http.StatusBadRequest, "invalid repository name %q", req.repository)
	}
	if req.kind == ociTagList {
		return nil
	}
	d, err := digest.Parse(req.reference)
	switch {
	case err == nil:
		req.digest = d
	case req.kind != ociManifest:
		return httputil.Errorf(http.StatusBadRequest, "invalid digest %q: %w", req.reference, err)
	case !ociTagPattern.MatchString(req.reference):
		return httputil.Errorf(http.StatusBadRequest, "invalid tag %q", req.reference)
	}
	return nil
}
