# Cachew (pronounced cashew) is a super-fast pass-through cache

Cachew is a server and tooling for incredibly efficient, protocol-aware caching. It is
designed to be used at scale, with minimal impact on upstream systems. By "protocol-aware", we mean that the proxy isn't
just a naive HTTP proxy, it is aware of the higher level protocol being proxied (Git, Docker, etc.) and can make more efficient decisions.

## Git

Git causes a number of problems for us, but the most obvious are:

1. Rate limiting by service providers.
2. `git clone` is very slow, even discounting network overhead

To solve this we apply two different strategies on the server:

1. Periodic full `.tar.zst` snapshots of the repository. These snapshots restore 4-5x faster than `git clone`.
2. Passthrough caching of the packs returned by `POST /repo.git/git-upload-pack` to support incremental pulls.

On the client we redirect git to the proxy:

```ini
[url "https://cachew.local/github/"]
  insteadOf = https://github.com/
```

As Git itself isn't aware of the snapshots, Git-specific code in the Cachew CLI can be used to reconstruct a repository.

## Authorization (OPA)

Cachew uses [Open Policy Agent](https://www.openpolicyagent.org/) (OPA) for request authorization. A default policy is
always active even without any configuration, allowing any request from 127.0.0.1 and `GET` and `HEAD` requests from
elsewhere.

To customise the policy, add an `opa` block to your configuration with either an inline policy or a path to a `.rego` file:

```hcl
# Inline policy
opa {
  policy = <<EOF
    package cachew.authz
    default allow := false
    allow if input.method == "GET"
    allow if input.method == "HEAD"
    allow if { input.method == "POST"; input.path[0] == "api" }
  EOF
}

# Or reference an external file
opa {
  policy-file = "./policy.rego"
}
```

Policies must be written under `package cachew.authz` and define a boolean `allow` rule. The input document available to policies contains:

| Field | Type | Description |
|---|---|---|
| `input.method` | string | HTTP method (GET, POST, etc.) |
| `input.path` | []string | URL path split by `/` (e.g. `["api", "v1", "object"]`) |
| `input.headers` | map[string]string | Request headers (lowercased keys) |
| `input.remote_addr` | string | Client address (ip:port) |

Since `remote_addr` includes the port, use `startswith` to match by IP:

```rego
allow if startswith(input.remote_addr, "127.0.0.1:")
```

Policies can reference external data that becomes available as `data.*` in Rego. Provide it inline via `data` or from a file via `data-file`:

```hcl
# Inline JSON data
opa {
  policy-file = "./policy.rego"
  data = <<EOF
    {"allowed_cidrs": ["10.0.0.0/8"], "jwks": {"keys": [...]}}
  EOF
}

# Or from a file
opa {
  policy-file = "./policy.rego"
  data-file = "./opa-data.json"
}
```

```json
{"allowed_cidrs": ["10.0.0.0/8"], "jwks": {"keys": [...]}}
```

```rego
package cachew.authz
default allow := false
allow if net.cidr_contains(data.allowed_cidrs[_], input.remote_addr)
```

If `data-file` is not set, `data.*` is empty but policies can still use `http.send` to fetch data at evaluation time.

## Docker

## Hermit

Caches Hermit package downloads from all sources (golang.org, npm, GitHub releases, etc.).

**URL pattern:** `/hermit/{host}/{path...}`

Example: `GET /hermit/golang.org/dl/go1.21.0.tar.gz`

GitHub releases are automatically redirected to the `github-releases` strategy.
