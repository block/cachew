# Git Caching Strategy Research

## Goals

1. Minimize impact on upstream Git servers
2. Make git clones as fast as possible
3. Efficiently handle incremental fetches

## Three-Layer Approach

### Layer 1: Snapshot Tarballs (Fastest Initial Clones)

**Observation**: `tar` is significantly faster than Git at populating a repository because:
- No pack negotiation overhead
- No delta resolution computation
- Single sequential read/write operation
- Can use fast compression (zstd)

**Approach**:
1. Cache server maintains full clones of upstream repositories
2. Generate daily tarballs of the full clone
3. Client downloads and extracts tarball, then runs `git fetch` to catch up

**Client-side workflow**:
```
# Instead of: git clone https://github.com/org/repo
cachew git clone https://github.com/org/repo
```

Under the hood:
1. Check if snapshot tarball exists for repo
2. Download and extract: curl ... | zstd -d | tar -xf -
3. Set remote URL to upstream (or through cache proxy)
4. git fetch to get any updates since snapshot
5. git checkout as normal

### Layer 2: Daily Bundles (Fallback for Non-Tarball Clients)

For clients that don't use the tarball option, daily bundles provide a simpler optimisation.

**Approach**:
- Generate one daily bundle containing all refs
- Cache server advertises bundle URI via protocol v2 `bundle-uri` capability
- Client cloning through cache proxy automatically fetches bundle first
- Git then negotiates remaining objects via normal protocol

### Layer 3: Git Protocol Proxy (Normal Fetches)

Proxy `git-upload-pack` requests, always serving from the local clone.

**Approach**:
- Cache server intercepts git protocol requests
- Always serves objects from local clone (never proxies to upstream)
- Local clone is kept fresh via periodic background fetches

**Cache Key Strategy**:

To cache packfile responses, normalize and hash the request:
```
cache_key = hash(repo_url, sorted(want_refs), sorted(have_refs))
```

**Normalization**:
- Sort want/have OIDs lexicographically
- Include repo identifier
- Optionally include filter spec (for partial clones)

**Example**:
```
wants: [abc123, def456, 789xyz]
haves: [111aaa, 222bbb]

normalized = "{host}/{path}:wants=789xyz,abc123,def456:haves=111aaa,222bbb"
cache_key = sha256(normalized)
```

**Benefits**:
- Zero load on upstream for git protocol operations
- Multiple clients with same repo state get cache hits
- CI builds cloning same commit hit cache
- Works transparently with standard git

**Considerations**:
- Local clone freshness depends on background fetch interval
- May need to handle shallow clones separately

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Cache Server                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌─────────────────────────────────┐ │
│  │   Full Clone    │    │      Daily Generators           │ │
│  │    Storage      │───▶│  - Tarball snapshots (.tar.zst) │ │
│  │                 │    │  - Bundle files (.bundle)       │ │
│  │  /repos/        │    └─────────────────────────────────┘ │
│  │   {host}/{path} │                   │                    │
│  │                 │                   ▼                    │
│  └────────┬────────┘    ┌─────────────────────────────────┐ │
│           │             │         Object Cache            │ │
│           │             │  - Snapshots                    │ │
│           │             │  - Bundles                      │ │
│           └────────────▶│  - Packfile responses           │ │
│                         └─────────────────────────────────┘ │
│                                        │                    │
│                                        ▼                    │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                    HTTP Endpoints                        ││
│  │                                                          ││
│  │  GET  /git/{host}/{path}/snapshot.tar.zst               ││
│  │  GET  /git/{host}/{path}/bundle.bundle                  ││
│  │  POST /git/{host}/{path}/git-upload-pack                ││
│  │                                                          ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### Client Options

**Option A: Wrapper Script** (`cachew-git`) - Recommended
- Intercepts `clone` command
- Downloads snapshot tarball, extracts, fetches updates
- Falls back to bundle-uri or cached git protocol

**Option B: Git Config Redirect**
- Configure `url.<base>.insteadOf` to redirect through cache
- Works with standard git commands
- Only benefits from protocol caching and bundles (no tarball support)

### Data Flow: Initial Clone (Tarball Client)

```
Client                     Cache Server                 Upstream
  │                             │                          │
  │ GET /snapshot.tar.zst       │                          │
  │────────────────────────────▶│                          │
  │◀────────────────────────────│ (serve from cache)       │
  │ tar -xf                     │                          │
  │                             │                          │
  │ git fetch (via cache)       │                          │
  │────────────────────────────▶│                          │
  │                             │ (cache lookup by         │
  │                             │  hashed refs)            │
  │◀────────────────────────────│                          │
```

### Data Flow: Normal Git Clone (Protocol Proxy)

```
Client                     Cache Server                 Upstream
  │                             │                          │
  │ git-upload-pack             │                          │
  │ wants=[...] haves=[...]     │                          │
  │────────────────────────────▶│                          │
  │                             │ hash(wants, haves)       │
  │                             │ cache lookup             │
  │                             │                          │
  │                             │ MISS: serve from local   │
  │                             │ clone, cache response    │
  │◀────────────────────────────│                          │
  │                             │                          │
  │                             │ HIT: serve from cache    │
  │◀────────────────────────────│                          │
```

## Implementation Plan

### Phase 1: Clone Management
1. Storage for full clones on cache server
2. Background job to `git fetch` from upstream periodically
3. Track last-fetched time per repository

### Phase 2: Snapshot Tarballs
1. Daily tarball generation from full clones
2. HTTP endpoint to serve snapshots
3. Client wrapper script (`cachew-git clone`)

### Phase 3: Git Protocol Proxy
1. Implement `git-upload-pack` endpoint
2. Parse wants/haves from request
3. Normalize and hash for cache key
4. Serve from local clone, cache packfile responses

### Phase 4: Bundle Support
1. Daily bundle generation from full clones
2. HTTP endpoint to serve bundle file
3. Advertise bundle-uri in protocol v2 capability during git-upload-pack

## Key Decisions

### Git Version Requirement
- Git 2.38+ for bundle-uri support
- Client wrapper works with any Git version

### Compression
- Tarballs: zstd (fast decompression, good ratio)
- Bundles: Git's native pack compression

### Cache Keys
- Snapshots: `git/{host}/{path}/snapshot-{date}.tar.zst`
- Bundles: `git/{host}/{path}/bundle-{date}.bundle`
- Packfiles: `git/{host}/{path}/pack-{hash(wants,haves)}.pack`

### Freshness
- Bare clone fetch: every 5-15 minutes (configurable)
- Snapshots: generated daily
- Bundles: generated daily
- Packfiles: long TTL (immutable for given inputs)

### Storage
- Full clones: local filesystem (fast access needed)
- Everything else: cache backend (tiered)

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Stale snapshots | Always `git fetch` after snapshot extract |
| Large repositories | Consider blobless partial clone support later |
| Upstream auth | Pass through credentials or use deployment keys |
| Storage growth | Retention policies, single clone per repo |
| Packfile cache misses | Most CI builds have identical state = high hit rate |

## References

- [Git Bundle-URI Documentation](https://git-scm.com/docs/bundle-uri)
- [Git Protocol v2](https://git-scm.com/docs/protocol-v2)
- [Git Pack Protocol](https://git-scm.com/docs/pack-protocol)
