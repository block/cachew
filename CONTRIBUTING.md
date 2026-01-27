# Contributing to Cachew

Thank you for your interest in contributing to Cachew! This guide will help you get started with local development, building, and testing.

## Local Development

**Run natively (fastest for development):**
```bash
just run                    # Build and run on localhost:8080
```

**Run in Docker:**
```bash
just docker-run             # Build and run in container
just docker-run debug       # Run with debug logging
```

## Building and Testing

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

## Using the Cache

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
