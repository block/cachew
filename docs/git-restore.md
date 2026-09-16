# Git restore: snapshots, bundles, and parallel downloads

`cachew git restore <repo-url> <dir>` recreates a working tree from the
server's cached artifacts instead of running `git clone`. The flow
(`cmd/cachew/git.go`):

1. **Snapshot download** — fetch `/git/{repo}/snapshot.tar.zst`, a periodic
   `.tar.zst` archive of the working tree, and pipe it straight into
   extraction so decompression overlaps the transfer. When the target
   directory doesn't exist yet, extraction goes into a staging directory
   renamed into place on success, so a failed download never leaves a
   half-written checkout. Restoring over an existing checkout extracts
   directly into it (a rename can't replace a non-empty directory), so a
   failure there can leave partially extracted files behind.
2. **Delta bundle** — the snapshot response advertises a bundle URL
   (`X-Cachew-Bundle-Url`) covering commits from the snapshot's commit to the
   mirror's current HEAD. The client fetches and applies it to catch up
   without talking to the upstream. A bundle failure is a warning, not an
   error — the restore continues with the snapshot state.
3. **Freshen** (only with `--ref`/`--commit`) — ask the server to ensure the
   required refs/commits exist on its mirror, then `git pull --ff-only` from
   origin so the working tree catches up. Skipped entirely when the
   snapshot+bundle already contain everything requested.

## Parallel snapshot download

The snapshot is downloaded as many concurrent byte-range requests
(`client.ParallelGet`, driven by `--download-concurrency` and
`--download-chunk-size-mb`):

- **Discovery**: the first chunk is requested with a `Range` header; the
  response reveals the object's total size and ETag, plus the snapshot
  metadata headers (commit, bundle URL). The git strategy advertises these on
  both full (200) and ranged (206) responses, so the client learns everything
  it needs from this one response (`internal/strategy/git/snapshot.go`).
- **Pinning**: every subsequent chunk carries `If-Match` with the discovery
  ETag, so an object rewritten mid-download (e.g. by the periodic snapshotter)
  is rejected rather than spliced together from two revisions.
- **Streaming**: chunks complete out of order and are reassembled into a
  sequential stream with bounded buffering, feeding `tar` extraction as bytes
  arrive.

## Fallbacks and failure modes

Single-stream fallback happens at discovery time only: a server that ignores
the range, an object with no ETag to pin to, an object that fits in the first
chunk, or `--download-concurrency=1` all degrade to one full read.

After discovery, a mid-download rewrite is **fatal**: pinned chunks fail their
`If-Match` precondition and the download errors rather than delivering a
corrupt archive. The client does not retry single-stream and does not fall
back to `git clone` — retrying the restore (or cloning) is the caller's
decision. Bundle failures, by contrast, are non-fatal as described above.
