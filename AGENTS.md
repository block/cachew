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

Only add comments for relatively large blocks of code, 20+ lines or more, and ONLY if it is not obvious what the code is
doing. ALWAYS add Go-style documentation comments for public variables/types/functions. If you do add comments, the
comments should explain WHY something is happening, not WHAT is happening.
