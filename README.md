# Cachew (pronounced cashew) is a super-fast pass-through cache

Cachew is a server and tooling for incredibly efficient, protocol-aware caching. It is
designed to be used at scale, with minimal impact on upstream systems. By "protocol-aware", we mean that the proxy isn't
just a naive HTTP proxy, it is aware of the higher level protocol being proxied (Git, Docker, etc.) and can make more efficient decisions.

## Quick Start

### Local Development

**Run natively (fastest for development):**
```bash
just run                    # Build and run on localhost:8080
```

**Run in Docker:**
```bash
just docker-run             # Build and run in container
just docker-run debug       # Run with debug logging
```

### Using the Cache

The `cachew` CLI client interacts with the `cachewd` server:

```bash
# Upload to cache
cachew put my-key myfile.txt --ttl 24h

# Download from cache  
cachew get my-key -o myfile.txt

# Check if cached
cachew stat my-key

# Snapshot a directory
cachew snapshot deps-cache ./node_modules --ttl 7d

# Restore a directory
cachew restore deps-cache ./node_modules
```

### Building and Testing

```bash
just build              # Build for current platform
just build-linux        # Build for Linux (for Docker)
just build-all          # Build all platforms
just test               # Run tests
just lint               # Lint code
just fmt                # Format code
```

## Docker Images

### Build Images

```bash
just docker-build        # Single-arch local image (fast)
just docker-build-multi  # Multi-arch image for ECR
```

### Push to ECR

**Push from local machine:**
```bash
just image-push          # Push with commit SHA tag
just image-push latest   # Push :latest and commit SHA tags
```
## Configuration

The default configuration is in `cachew.hcl`. Key settings:

- **Disk cache**: `./state/cache` (250GB limit, 8h TTL)
- **Git mirrors**: `./state/git-mirrors` 
- **Bind address**: `0.0.0.0:8080` (in Docker), `127.0.0.1:8080` (native)