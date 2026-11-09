# Cachew

Cachew (pronounced "cashew") is a tiered, protocol-aware, caching HTTP proxy for software engineering infrastructure. It understands higher-level protocols (Git, Docker, Go modules, etc.) and makes smarter caching decisions than a naive HTTP proxy.

## Strategies

### Git

Caches Git repositories with two complementary techniques:

1. **Snapshots** — periodic `.tar.zst` archives that restore 4–5x faster than `git clone`.
2. **Pack caching** — passthrough caching of packs from `git-upload-pack` for incremental pulls.

Redirect Git traffic through cachew:

```ini
[url "https://cachew.example.com/git/github.com/"]
  insteadOf = https://github.com/
```

Restore a repository from a snapshot (with automatic delta bundle to reach HEAD):

```sh
cachew git restore https://github.com/org/repo ./repo
```

```hcl
git {
  snapshot-interval = "1h"
  repack-interval   = "1h"
}
```

### GitHub Releases

Caches public and private GitHub release assets. Private orgs use a token or GitHub App for authentication.

**URL pattern:** `/github-releases/{owner}/{repo}/{tag}/{asset}`

```hcl
github-releases {
  token        = "${GITHUB_TOKEN}"
  private-orgs = ["myorg"]
}
```

### Go Modules

Go module proxy (`GOPROXY`-compatible). Private modules are fetched via git clone.

**URL pattern:** `/gomod/...`

```sh
export GOPROXY=http://cachew.example.com/gomod,direct
```

```hcl
gomod {
  proxy         = "https://proxy.golang.org"
  private-paths = ["github.com/myorg/*"]
}
```

### Hermit

Caches [Hermit](https://cashapp.github.io/hermit/) package downloads. GitHub release URLs are automatically routed through the `github-releases` strategy.

**URL pattern:** `/hermit/{host}/{path...}`

```hcl
hermit {}
```

### Artifactory

Caches artifacts from JFrog Artifactory with host-based or path-based routing.

```hcl
artifactory "example.jfrog.io" {
  target = "https://example.jfrog.io"
}
```

### Host

Generic reverse-proxy caching for arbitrary HTTP hosts, with optional custom headers.

```hcl
host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer QQ=="
  }
}

host "https://w3.org" {}
```

Optional `fallback` blocks select trusted mirrors in declaration order. Cachew tries
the next mirror only after a 429, a 5xx response, or a transport error. It never
retries a 404 against a mirror: the primary is authoritative and mirrors can lag.
Cancellation stops retries. Failures after response streaming starts cannot retry.

```hcl
host "https://packages.example.com" {
  fallback "https://mirror.example.com/archive" {
    path-prefix = "/releases"
  }
}
```

This maps `/releases/pkg.tar.gz` to `https://mirror.example.com/archive/pkg.tar.gz`.
`path-prefix` defaults to `/` and matches complete path segments. Unmatched paths
do not use that fallback. Query strings are preserved.

Fallbacks receive the same request headers and are trusted like the primary for
shared cache content. Configure only mirrors that serve byte-identical content.
Successful responses use the primary URL's cache key, regardless of which upstream
served them. Non-200 and incomplete responses are not cached.

### HTTP Proxy

Caching proxy for clients that use absolute-form HTTP requests (e.g. Android `sdkmanager --proxy_host`).

```hcl
proxy {}
```

### OCI

Read-only pull-through cache for Docker/OCI image registries, with one block per registry. Pushes are rejected.

**URL patterns:** `/oci/{registry}/v2/...`, and `/v2/...` on each of `hosts`.

```hcl
oci "docker.io" {
  upstream = "https://registry-1.docker.io"
  hosts    = ["docker.cachew.example.com"]
  rewrite "^([^/]+)$" { replacement = "library/$1" }
}
```

- Blobs and digest-addressed manifests are checked against their digest and cached until evicted.
- Tag manifests are rechecked with a `HEAD` request once they are older than `tag-ttl` (default `5m`). If the registry is unavailable or rate limiting, the cached copy is served.
- `username` and `password` authenticate to the upstream for all clients. Clients' own credentials are never forwarded. Without them, access is anonymous.
- `blob-chunk-size-mb` (default `32`) and `blob-concurrency` (default `4`) control parallel range requests when filling large blobs.
- Tag lists and referrers are not cached.

containerd can use the path-prefixed form, e.g. in `/etc/containerd/certs.d/docker.io/hosts.toml`:

```toml
server = "https://registry-1.docker.io"

[host."https://cachew.example.com/oci/docker.io/v2"]
  capabilities = ["pull", "resolve"]
  override_path = true
```

Docker and Podman cannot use a path prefix, so they need a hostname from `hosts` that resolves to cachew. Docker's `registry-mirrors` setting only applies to Docker Hub. In `/etc/docker/daemon.json`:

```json
{ "registry-mirrors": ["https://docker.cachew.example.com"] }
```

## Cache Backends

Multiple backends can be configured simultaneously — they are automatically combined into a tiered cache. Cache blocks
are ordered from lowest/nearest to highest/authoritative. Reads check each tier in order and backfill lower tiers on a
hit. Writes go to all tiers in parallel. Replica invalidations evict only non-authoritative tiers; the final cache block
is authoritative. Tiered caches use the metadata backend to track authoritative ETags and invalidate stale lower-tier
copies before falling through to the authoritative tier.

### Memory

In-memory LRU cache.

```hcl
memory {
  limit-mb = 1024   # default
  max-ttl  = "1h"   # default
}
```

### Disk

On-disk LRU cache with TTL-based eviction.

```hcl
disk {
  limit-mb = 250000
  max-ttl  = "8h"
}
```

### S3

S3-compatible object storage (AWS S3, MinIO, etc.).

```hcl
s3 {
  bucket   = "my-cache-bucket"
  endpoint = "s3.amazonaws.com"
  region   = "us-east-1"
}
```

## Request Coalescing

Concurrent requests for the same uncached object share one upstream fetch, and every client streams the response while it downloads. The fetch continues and fills the cache even if every client disconnects.

Error responses are shared only when HTTP caching rules allow reuse, and for at most `error-window`.

```hcl
spool {
  dir          = "${CACHEW_STATE}/spools"  # Response files for backends that cannot be read while writing
  fill-timeout = "30m"                     # 0 disables the limit
  error-window = "5s"                      # 0 shares errors only with requests already waiting
}
```

## Upstream Rate Limits

After an upstream returns 429, cachewd stops sending upstream requests for that strategy until a cooldown ends. This includes its fallbacks and redirect targets. During the cooldown, misses get 429 with `Retry-After` and `Cache-Control: no-store` without contacting any upstream. Cache hits and other strategies are unaffected. Requests made outside the strategy handlers, such as Git, pause per origin (scheme, host and port). Nothing is retried automatically.

The cooldown uses the upstream's `Retry-After` (seconds or HTTP date) when it is valid, and `upstream-backoff` otherwise. Cooldowns are held in memory per process.

```hcl
upstream-backoff = "1m"  # Must be positive
```

## Authorization (OPA)

Cachew uses [Open Policy Agent](https://www.openpolicyagent.org/) for request authorization. The default policy allows all requests from localhost and restricts remote access to non-admin paths (`/api/*`, `/admin/*`).

Policies must be in `package cachew.authz` and define an `allow` rule. If `allow` is true the request proceeds; otherwise it is rejected with 403.

```hcl
opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if input.headers["authorization"]
  EOF
}
```

Or reference an external file with optional data:

```hcl
opa {
  policy-file = "./policy.rego"
  data-file   = "./opa-data.json"
}
```

**Input fields:** `input.method`, `input.path` (string array), `input.headers`, `input.remote_addr` (includes port — use `startswith` to match by IP).

### Testing policies

The `test` field holds a Rego test module that is run against the policy when `cachewd` starts. Any rule prefixed with `test_` is executed; if a test fails, `cachewd` exits.

```hcl
opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if input.method == "POST"
  EOF
  test = <<EOF
    package cachew.authz_test
    import data.cachew.authz

    test_post_allowed if authz.allow with input as {"method": "POST"}
    test_get_denied if not authz.allow with input as {"method": "GET"}
  EOF
}
```

## GitHub App Authentication

For private Git repositories and GitHub release assets, configure a GitHub App:

```hcl
github-app {
  app-id           = "12345"
  private-key-path = "./github-app.pem"
  installations    = { "myorg": "67890" }
}
```

Installations can also be discovered dynamically via the GitHub API.

## CLI

### Server (`cachewd`)

```sh
cachewd --config cachew.hcl
cachewd --schema  # print config schema
```

### Client (`cachew`)

```sh
# Object operations
cachew get <namespace> <key> [-o file]
cachew put <namespace> <key> [file] [--ttl 1h]
cachew stat <namespace> <key>
cachew delete <namespace> <key>
cachew namespaces

# Directory snapshots
cachew save <namespace> <directory> [paths...] (--key <key> | -H <glob>) [--ttl 1h] [--exclude pattern]
cachew restore <namespace> <directory> (--key <key> | -H <glob>)  # exit 0 hit, 2 miss, 1 error

# Git
cachew git restore <repo-url> <directory> [--no-bundle]
```

**Global flags:** `--url` (`CACHEW_URL`), `--authorization` (`CACHEW_AUTHORIZATION`), `--platform` (prefix keys with `os-arch`), `--daily`/`--hourly` (prefix keys with date).

## Observability

```hcl
log {
  level = "info"  # debug, info, warn, error
}

metrics {
  service-name = "cachew"
}
```

Admin endpoints: `/_liveness`, `/_readiness`, `PUT /admin/log/level`, `/admin/pprof/`.

GitHub App installations are checked at startup and every minute via `/rate_limit`.
The poller includes dynamically discovered installations and the configured fallback organization.

| Metric | Type | Labels |
| --- | --- | --- |
| `github.ratelimit_remaining` | Gauge | `app`, `installation_id`, `owner`, `resource` |
| `github.ratelimit_limit` | Gauge | `app`, `installation_id`, `owner`, `resource` |
| `github.ratelimit_used` | Gauge | `app`, `installation_id`, `owner`, `resource` |
| `github.api_calls` | Counter | `owner`, `operation` |

`app` is the configured App ID. `owner` identifies the installation actually used,
including fallback credentials. Installation discovery uses `owner=unknown` because
it authenticates as the App before resolving an installation. API calls for release
metadata/assets, installation discovery, token creation and quota polling are counted. Responses with less than
10% of their API quota remaining produce a warning. Static release tokens receive
API call counts and warnings, but are not polled for quota gauges.

Quota gauges describe the installation's shared API budget; do not sum them across
replicas. They do not track Git transport or LFS limits. Polling does not consume
the primary API budget, but can count toward GitHub's secondary limits.

### Artifact cache metrics

Generic artifact handlers (host, Artifactory, Hermit, HTTP proxy, OCI blobs and manifests, and GitHub release downloads) export:

| OpenTelemetry instrument | Type | Extra labels |
| --- | --- | --- |
| `cachew.origin.requests` | Counter | `status_code`, `error_type` |
| `cachew.origin.request_duration_seconds` | Histogram | `status_code`, `error_type` |
| `cachew.cache.fills` | Counter | `purpose`, `result` |
| `cachew.cache.tier_writes` | Counter | `backend`, `tier`, `purpose`, `result` |

All carry `strategy` and `registry`. Registry values come from configured host/Artifactory targets or fixed strategy labels; dynamic proxy/Hermit hosts use `other`. URLs, paths, cache keys, credentials and caller namespaces are not labels. Prometheus converts dots to underscores and appends `_total` to counters.

The HTTP server metrics of these handlers carry `cachew.cache.result` (`hit`, `miss`, `coalesced` or `error`). Hits also carry `cachew.cache.source`, the backend that served them (`disk`, `s3`, `memory`, `remote`, or `other`), after stale and conditional probes.

Origin metrics count HTTP exchanges that reach an origin through `http.DefaultTransport`, including redirect hops and transport failures. Requests rejected during an upstream 429 cooldown are not counted. `status_code=none` means no HTTP response; `error_type` is `none`, `transport`, `timeout`, or `canceled`. Duration ends at response headers or transport failure. Registry identifies the logical artifact provider, including its redirects, rather than each physical destination. These metrics do not count every low-level connection retry, Git transport, Go-module fetch, GitHub metadata lookup, OCI tag-list or referrer pass-through, or S3 operation. OCI registry ping and token exchanges are counted, because they go to the registry for the artifact. Internal Hermit forwarding to another Cachew route is excluded from origin/fill counts; the receiving handler measures its own work.

Fills record a terminal outcome, rather than treating an origin 200 as successful publication. `purpose` separates `origin`, `backfill` and ranged `heal`. Results distinguish successful completion from creation, write, commit, transfer or read errors, cancellation, and superseded heals. Tier writes report backend outcomes separately: a failed S3 write can coexist with a successful disk write. Early close or canceled backfill is not a successful fill. Client responses continue normally when cache writes fail.

Use `cachew.cache.result` and `cachew.cache.source` to compare local/shared hits and misses, origin requests by status to measure dependency traffic, and fills/tier writes to find failed publication. An origin error percentage must use origin exchanges as its denominator; client response percentages include cache hits. Idle periods have no percentage.

## Full Configuration Example

```hcl
state = "./state"
bind  = "0.0.0.0:8080"
url   = "http://cachew.example.com:8080/"

log {
  level = "info"
}

opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if startswith(input.remote_addr, "127.0.0.1:")
  EOF
}

metrics {}

github-app {
  app-id           = "12345"
  private-key-path = "./github-app.pem"
}

git-clone {}

git {
  snapshot-interval = "1h"
  repack-interval   = "1h"
}

github-releases {
  token        = "${GITHUB_TOKEN}"
  private-orgs = ["myorg"]
}

gomod {
  proxy         = "https://proxy.golang.org"
  private-paths = ["github.com/myorg/*"]
}

hermit {}

host "https://ghcr.io" {
  headers = {
    "Authorization": "Bearer ${GHCR_TOKEN}"
  }
}

disk {
  limit-mb = 250000
  max-ttl  = "8h"
}

proxy {}
```
