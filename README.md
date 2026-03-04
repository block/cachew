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
   Shallow snapshots are also supported via `?depth=N` (e.g., `/git/{host}/{repo}/snapshot.tar.zst?depth=100`),
   which produces much smaller snapshots for large repositories. Shallow snapshots are generated on-demand
   on first request and then refreshed periodically. Note: shallow snapshots do not include the working tree
   (`--no-checkout`), so clients must run `git checkout` after extracting.
2. Passthrough caching of the packs returned by `POST /repo.git/git-upload-pack` to support incremental pulls.

On the client we redirect git to the proxy:

```ini
[url "https://cachew.local/github/"]
  insteadOf = https://github.com/
```

As Git itself isn't aware of the snapshots, Git-specific code in the Cachew CLI can be used to reconstruct a repository.

## Docker

## Hermit

Caches Hermit package downloads from all sources (golang.org, npm, GitHub releases, etc.).

**URL pattern:** `/hermit/{host}/{path...}`

Example: `GET /hermit/golang.org/dl/go1.21.0.tar.gz`

GitHub releases are automatically redirected to the `github-releases` strategy.
