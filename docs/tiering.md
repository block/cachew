# How cache tiering works

How multiple cache backends are composed into a single tiered cache with
fallback, backfill, and invalidation semantics. For how a request reaches the
cache in the first place, see [architecture.md](architecture.md). Source of
truth: `internal/cache/tiered.go` and `internal/cache/api.go`.

Cache backends are object stores with per-object TTLs and metadata. Three are
configurable: `memory`, `disk`, and `s3`. Configuring more than one `cache`
block composes them into a `Tiered` cache automatically
(`cache.MaybeNewTiered`); with a single block the cache is wrapped so that
`Invalidate` is a no-op, since the only tier is authoritative.

**Order matters.** Cache blocks are ordered nearest-first. The **final tier is
authoritative** — typically shared storage like S3 — and everything before it
is treated as a local copy that can be re-fetched.

```
cache memory { }          # tier 0: nearest, fastest
cache disk   { }          # tier 1
cache s3     { ... }      # tier 2: authoritative
```

```
        read: probe in order, first hit wins
        ────────────────────────────────────▶
┌────────┐      ┌──────┐      ┌────────────────┐
│ memory │      │ disk │      │ s3 (authorit.) │
└────────┘      └──────┘      └────────────────┘
        ◀────────────────────────────────────
        backfill tier 0 on a deeper hit

        write: all tiers in parallel
```

## Reads

`Open`/`Stat` probe tiers in order and return the first definitive answer.
When a deeper tier hits, the returned reader transparently **backfills tier
0** as the caller reads (`backfillReadCloser`), so the next read is served
locally. Backfill is asynchronous and safe: only a stream consumed to EOF
commits the tier-0 entry; a partial read or mid-stream error discards it.

Conditional requests complicate "definitive". A tier holding a *different
version* than the request's validators name (failed `If-Match`, `If-Range`
miss) is not a definitive miss — deeper tiers are consulted for the named
version. Only when no tier holds it does the first tier's outcome stand. A
tier that errored while being probed takes precedence, so outages are not
misreported as missing versions.

Ranged reads return partial bodies, which must never be backfilled as whole
objects. Instead a bounded background **healer** re-fetches the full object
from the serving tier and refreshes tier 0 out of band, so a divergent tier 0
still converges even when clients only ever issue ranged requests — as the
parallel snapshot downloader does (see [architecture.md](architecture.md)).

## Writes

`Create` writes to **all tiers in parallel** through a single writer; the
first error aborts every in-flight write. Entries only become readable once
completely written and closed — a cancelled context discards the object in
every tier.

## ETags and stale-tier invalidation

When a write **replaces** an existing object in the authoritative tier with
different content, the tiered cache records the new authoritative ETag in the
metadata store (the `metadata` block; see [metadatadb-s3.md](metadatadb-s3.md)).
On reads, a tier whose ETag doesn't match the recorded authoritative ETag is
**invalidated and skipped**, falling through to the next tier. This is how
replicas still holding the previous version converge onto the shared tier
after a rewrite. Keys that have never been rewritten have no recorded ETag and
are served from whichever tier hits first.

## Delete vs Invalidate

- `Delete` removes the object from **every** tier.
- `Invalidate` evicts stale copies from the **non-authoritative** tiers only;
  the final tier is left intact by construction. This is what replicas use to
  drop local copies without destroying shared state.

## Instance-to-instance tiering

Cachew is designed to run as a local instance (workstation/CI) backed by a
shared remote instance. The pieces:

- The remote instance serves its cache via the **api-v1 strategy** (see
  [architecture.md](architecture.md)).
- `client/` is a standalone Go client for that API, and
  `internal/cache/remote.go` adapts it to the `Cache` interface — a remote
  Cachew instance as a cache tier.
