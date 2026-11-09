This codebase is a tiered, caching, protocol-aware, pass-through HTTP proxy. The cache is designed for software
engineering machines, in CI, interactive in the cloud, or locally.

The cache server is tiered, in that it runs locally on workstations, but backs onto remote instances of the cache
server. If the local instance doesn't have an object, it will automatically fall back to the remote instance.

Logically, it's broken up into two main components - cache backends, and caching strategies. Cache backends are object
stores with some extra metadata and per-object TTLs. Strategies are protocol-aware caching mechanisms. For example, the
github-releases strategy knows how to retrieve and cache private and public GitHub releases artefacts. Another strategy
example is for Git, where the proxy is aware of the Git protocol and uses it to reduce the impact on upstream Git
servers.

The way the two components interact is that the strategies have access to a Cache implementation, which they use to
cache and restore data. Strategies are not limited to using the Cache though; for example the Git strategy could 
keep a local bare checkout of upstream Git repositories and serve packs from the repo directly.

The codebase uses Hermit to manage toolchains. It is written in Go, and uses Just for running common tasks.

ONLY add comments for relatively large blocks of code, 20+ lines or more, and ONLY if it is not obvious what the code is
doing. ALWAYS add Go-style documentation comments for public variables/types/functions. If you do add comments, the
comments MUST explain WHY something is happening, not WHAT is happening. NEVER add comments to top-level tests. BE
SUCCINCT WHEN COMMENTING.

Functions should return errors, not log them internally. Logging belongs at the call site so callers retain control over
how failures are reported and handled.

When logging with slog, NEVER use slog.String(), slog.Int and friends, it's too verbose. Instead, use `key, value` pairs. Don't use a new line for each pair, concatenate them and wrap at 120 columns.

<!-- BEGIN tsk -->
# Linters

This repository enforces its conventions with `tsk` linters, configured in
`.tsk/config.toml`. Use the `tsk` skill for how to enable, configure, and write
them.

- Run `tsk` to lint, and fix its findings before finishing a change. Keep an
  intentional finding with a `//nolint:<linter> // <reason>` comment.
- When the user asks you to fix a kind of issue across the codebase, check
  `tsk list` for a linter that reports it. If one exists and is not enabled,
  suggest enabling it so the issue stays fixed.
- When the user asks you to apply an architectural constraint or convention,
  enforce it with a linter: enable or configure an existing one, or write one
  in `.tsk/`. Write a configurable linter for the kind of rule, and set the
  specific rule in its config.
- When the user corrects you, check whether the correction is a lasting rule a
  linter could enforce. Any review comment or question that leads you to
  change code is a correction. "Why is this a separate type?" counts as much
  as "don't do this".
- Offer a linter if these plausibly hold. They are a quick filter, not a gate,
  and the user decides whether the linter is worth it:
  - The rule applies across the codebase, not only to the current change.
  - A linter can detect violations from Go syntax and types with few false
    positives.
  - No linter in `tsk list` already enforces it.
- Make the offer in the response that applies the correction, not later in the
  session. End that response with one sentence that names the rule and says
  how a linter would detect it.
- Offer once per rule, and do not repeat it if the user declines.
<!-- END tsk -->
