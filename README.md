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

### HTTP Proxy

Caching proxy for clients that use absolute-form HTTP requests (e.g. Android `sdkmanager --proxy_host`).

```hcl
proxy {}
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

## External Git Credentials

Background mirror operations can authenticate to an exact private HTTPS remote by invoking an external credential command:

```hcl
git-credential-command "ado-aks" {
  command = ["/usr/local/bin/aks-code-credential"]
  remotes = [
    "https://dev.azure.com/example/project/_git/repository",
  ]
  timeout        = "5s"
  refresh-before = "5m"
}
```

Cachew sends `{"version":1,"remote_url":"https://..."}` followed by a newline on stdin. `remote_url` is the canonical upstream Git remote URL. The command must return one JSON object on stdout:

```json
{"version":1,"authorization":"Bearer ...","expires_at":"2026-08-10T12:00:00Z"}
```

The command is executed directly without a shell. Remotes are canonicalized and matched exactly, credentials are cached only in memory, and a matched provider failure prevents Git from running. The command must write no credentials to stderr.

External providers written in Go can import `github.com/block/cachew/gitcredential`. `CommandMain` uses Kong to populate a provider-defined options struct and handles signals, stdin/stdout, protocol validation, and errors; the provider supplies only construction and credential logic:

```go
type Options struct {
    Audience string `help:"Token audience." required:""`
}

func main() {
    gitcredential.CommandMain(&Options{}, func(ctx context.Context, options *Options) (gitcredential.CommandHandler, error) {
        return gitcredential.CommandHandlerFunc(func(ctx context.Context, remoteURL string) (gitcredential.CommandResult, error) {
            token, expiresAt, err := obtainToken(ctx, options.Audience, remoteURL)
            return gitcredential.CommandResult{Authorization: "Bearer " + token, ExpiresAt: expiresAt}, err
        }), nil
    })
}
```

`ServeCommand`, `DecodeRequest`, and `EncodeResponse` are also available for executables that need custom CLI or process handling. Cachew's `Provider` interface remains separate and supports in-process providers; the GitHub App adapter uses that path without a subprocess.

Cachew images include an Azure Identity helper for Azure DevOps. The helper defaults to Workload Identity, requires an explicit access-token audience, and verifies that the repository URL supplied by Cachew uses `dev.azure.com`:

```hcl
git-credential-command "ado-aks" {
  command = [
    "/usr/local/bin/cachew-azure-git-credential",
    "--audience", "499b84ac-1321-427f-aa17-267ca6975798",
  ]
  remotes = ["https://dev.azure.com/example/project/_git/repository"]
}
```

For AKS Workload Identity, provide `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and `AZURE_FEDERATED_TOKEN_FILE`. The service principal or managed identity must also be added to the Azure DevOps organization with repository read permission. For development, append `--credential=default`; `--credential=managed-identity` selects the system-assigned managed identity directly. `--audience` is required; the example uses the Azure DevOps application ID, and the helper appends `/.default` when needed. This is the access-token audience, not the projected Kubernetes token audience (`api://AzureADTokenExchange`). Tokens are never logged.

A local test image includes a deterministic provider that validates the requested URL and returns a test-only bearer token:

```sh
just docker build-test
docker run --rm -p 8080:8080 \
  -e CACHEW_TEST_GIT_REMOTE=https://git.example.test/platform/source \
  -e CACHEW_TEST_GIT_TOKEN=replace-with-test-server-token \
  cachew:credential-test

git ls-remote http://localhost:8080/git/git.example.test/platform/source
```

Point `CACHEW_TEST_GIT_REMOTE` at an HTTPS repository served by the test system. The helper accepts only an alphanumeric, `.`, `_`, or `-` token and uses a fixed test expiration. Do not deploy this image or use production credentials with it.

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

git-credential-command "private-git" {
  command = ["/usr/local/bin/private-git-credential"]
  remotes = ["https://git.example.com/platform/source"]
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
